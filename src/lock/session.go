package lock

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"time"

	"github.com/HqOapp/go-metric-client/metrics"

	otext "github.com/opentracing/opentracing-go/ext"
)

// Tasker can do the work associated with the tasks passed to it.
// It should return any completed tasks so they can by flaged as "finished"
type Tasker func(ctx context.Context, tasks []Task) ([]Task, error)

// Runner will loop and run tasks assigned to it
type Runner struct {
	stop         chan bool
	stopGroup    *sync.WaitGroup
	sessionMutex sync.RWMutex
	sessionID    int64
	dbFinder     DBFinder
	client       metrics.Client
	scanTask     ScanTask
	loopTick     time.Duration
	logger       Logger
	tasker       Tasker
	name         string
}

// NewRunner will create a new Runner to handle a type of task
// dbFinder can get an instance of the Database interface on demand
// scanTask can read from a sql.row into a Task
// tasker can complete Tasks
// looptick defines how often to check for tasks to complete
// client is a go-metrics-client that will also start spans for us
// logger is optional and will log errors if provided
func NewRunner(dbFinder DBFinder, scanTask ScanTask, tasker Tasker, loopTick time.Duration, logger Logger, name string, client metrics.Client) *Runner {
	if client == nil {
		return nil
	}
	if logger == nil {
		logger = &noopLogger{}
	}
	var sg sync.WaitGroup
	return &Runner{
		dbFinder:  dbFinder,
		client:    client,
		scanTask:  scanTask,
		loopTick:  loopTick,
		logger:    logger,
		tasker:    tasker,
		name:      name,
		stopGroup: &sg,
	}
}

// Run will start looping and processing tasks
// dont call this more than once.
func (r *Runner) Run() error {
	db, err := r.dbFinder()
	if err != nil {
		return err
	}

	ctx := context.Background()

	r.sessionMutex.Lock()
	r.sessionID, err = r.startSession(ctx, db)
	r.sessionMutex.Unlock()
	if err != nil {
		return err
	}

	r.stop = make(chan bool)
	go func() {
		// sleep up to 10 seconds to break up services that start at the same time
		time.Sleep(time.Duration(rand.Int63n(10)) * time.Second)

		// setup a ticker to get and do work
		tick := time.Tick(r.loopTick)
		for {
			select {
			case <-r.stop: // if Stop() was called, exit
				err := r.endSession(context.Background())
				if err != nil {
					r.logger.Printf("Error ending session: %v", err)
				}
				return
			default:
				// noop
			}
			select {
			case <-tick:
				// use wait group to block while doing work.
				r.stopGroup.Add(1)
				err := r.doWork(context.Background())
				if err != nil {
					r.logger.Printf("Error doing work: %v", err)
				}
				r.stopGroup.Done()
			}
		}
	}()
	go func() {
		// setup a ticker bump the session every 30 seconds
		// This will keep the session active even when working on tasks for a long time.
		// When the service shuts down bump will stop being called, sessions will eventually expire,
		// and other services will pick up new work.
		tick := time.Tick(time.Second * 30)
		for {
			select {
			case <-tick:
				r.sessionMutex.RLock()
				err := db.BumpSession(context.Background(), r.sessionID)
				r.sessionMutex.RUnlock()
				if err != nil {
					r.logger.Printf("Error bumping session: %v", err)
				}
			}
		}
	}()
	return nil
}

func (r *Runner) startSession(ctx context.Context, db Database) (sessionID int64, err error) {
	span, spanCtx := r.client.StartSpanWithContext(ctx, "runner start session")
	defer func() {
		if err != nil {
			otext.Error.Set(span, true)
			span.SetTag("inner-error", err)
		}
		span.Finish()
	}()

	sessionID, err = db.StartSession(spanCtx)
	span.SetTag("session_id", sessionID)
	return sessionID, err
}

func (r *Runner) endSession(ctx context.Context) (err error) {
	span, spanCtx := r.client.StartSpanWithContext(ctx, "runner end session")
	defer func() {
		if err != nil {
			otext.Error.Set(span, true)
			span.SetTag("inner-error", err)
		}
		span.Finish()
	}()

	db, err := r.dbFinder()
	if err != nil {
		return err
	}

	r.sessionMutex.Lock()
	err = db.EndSession(spanCtx, r.sessionID)
	r.sessionMutex.Unlock()
	if err != nil {
		return fmt.Errorf("Error ending session: %v", err)
	}
	return
}

func (r *Runner) doWork(ctx context.Context) (err error) {
	span, spanCtx := r.client.StartSpanWithContext(ctx, "doing work")
	start := time.Now()
	name := r.name
	sessionID := strconv.FormatInt(r.sessionID, 10)
	params := make(map[string]string)
	r.client.BackgroundRate(sessionID, name, params, 1)
	defer func() {
		if err != nil {
			otext.Error.Set(span, true)
			span.SetTag("inner-error", err)
		}
		span.Finish()
	}()

	// get work and process
	db, err := r.dbFinder()
	if err != nil {
		r.handleError(start, sessionID, name, "Failed to find DB", err.Error(), params)
		return fmt.Errorf("Error finding DB: %v", err)
	}
	r.sessionMutex.RLock()
	tasks, dbErr := db.GetWork(spanCtx, r.sessionID, r.scanTask)
	r.sessionMutex.RUnlock()
	if dbErr != nil {
		switch dbErr.Code() {
		case SQLErrorSessionNotFound:
			r.logger.Printf("Session expired. Getting new one")
			r.sessionMutex.Lock()
			r.sessionID, err = db.StartSession(spanCtx)
			r.sessionMutex.Unlock()
			if err != nil {
				r.handleError(start, sessionID, name, "Failed to start session", err.Error()+" with dbError: "+dbErr.Error(), params)
				return fmt.Errorf("Error starting new session: %v", dbErr)
			}
		default:
			r.handleError(start, sessionID, name, "Failed getting work from db", err.Error()+" with dbError: "+dbErr.Error(), params)
			return fmt.Errorf("Error getting work from db: %v", dbErr)
		}

	}

	completedTasks, err := r.tasker(spanCtx, tasks)
	if err != nil {
		r.handleError(start, sessionID, name, "Error running tasks", err.Error(), params)
		return fmt.Errorf("Error running tasks: %v", err)
	}

	taskIDs := make([]string, len(completedTasks))
	for i, t := range completedTasks {
		taskIDs[i] = t.GetID()
	}

	dbErr = db.FinishTasks(spanCtx, taskIDs)
	if dbErr != nil {
		r.handleError(start, sessionID, name, "Error finishing tasks", dbErr.Error(), params)
		return fmt.Errorf("Error finishing tasks: %v", dbErr)
	}
	end := time.Since(start)
	r.client.BackgroundDuration(sessionID, name, params, end)
	return nil
}

// Does common error stuff
func (r *Runner) handleError(start time.Time, sessionID, name, code, message string, params map[string]string) {
	end := time.Since(start)
	r.client.BackgroundDuration(sessionID, name, params, end)
	r.client.BackgroundError(sessionID, name, params, code, message, 1)
}

// Stop stops the runner from looping
// Stop returns a WaitGroup which you can wait on to ensure all work is finished
func (r *Runner) Stop() *sync.WaitGroup {
	close(r.stop)
	return r.stopGroup
}
