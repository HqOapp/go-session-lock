package lock

import (
	"context"
	"fmt"
	"math/rand"
	"time"
)

// Tasker can do the work associated with the tasks passed to it.
// It should return any completed tasks so they can by flaged as "finished"
type Tasker func(ctx context.Context, tasks []Task) ([]Task, error)

// Runner will loop and run tasks assigned to it
type Runner struct {
	stop      chan bool
	sessionID int64
	dbFinder  DBFinder
	tracer    Tracer
	scanTask  ScanTask
	loopTick  time.Duration
	logger    Logger
	tasker    Tasker
}

// NewRunner will create a new Runner to handle a type of task
// dbFinder can get an instance of the Database interface on demand
// scanTask can read from a sql.row into a Task
// tasker can complete Tasks
// looptick defines how often to check for tasks to complete
// tracer is optional and will log tracing metrics in an open tracing friendly way if provided
// logger is optional and will log errors if provided
func NewRunner(dbFinder DBFinder, scanTask ScanTask, tasker Tasker, loopTick time.Duration, tracer Tracer, logger Logger) *Runner {
	if tracer == nil {
		tracer = newNoopTracer()
	}
	if logger == nil {
		logger = &noopLogger{}
	}
	return &Runner{
		dbFinder: dbFinder,
		tracer:   tracer,
		scanTask: scanTask,
		loopTick: loopTick,
		logger:   logger,
		tasker:   tasker,
	}
}

// Run will start looping and processing tasks
func (r *Runner) Run() error {
	db, err := r.dbFinder()
	if err != nil {
		return err
	}

	ctx := context.Background()

	r.sessionID, err = r.startSession(ctx, db)
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
			case <-tick:
				err = r.doWork(ctx)
				if err != nil {
					r.logger.Printf("Error doing work: %v", err)
				}

			case <-r.stop: // if Stop() was called, exit
				err = r.endSession(ctx)
				if err != nil {
					r.logger.Printf("Error ending session: %v", err)
				}
				return
			}
		}
	}()
	return nil
}

func (r *Runner) startSession(ctx context.Context, db Database) (sessionID int64, err error) {
	span, spanCtx := r.tracer.StartSpanWithContext(ctx, "runner start session")
	defer func() {
		if err != nil {
			span.SetError(err)
		}
		span.Finish()
	}()

	sessionID, err = db.StartSession(spanCtx)
	return sessionID, err
}

func (r *Runner) endSession(ctx context.Context) (err error) {
	span, spanCtx := r.tracer.StartSpanWithContext(ctx, "runner end session")
	defer func() {
		if err != nil {
			span.SetError(err)
		}
		span.Finish()
	}()

	db, err := r.dbFinder()
	if err != nil {
		return err
	}

	err = db.EndSession(spanCtx, r.sessionID)
	if err != nil {
		return fmt.Errorf("Error ending session: %v", err)
	}
	return
}

func (r *Runner) doWork(ctx context.Context) (err error) {
	span, spanCtx := r.tracer.StartSpanWithContext(ctx, "doing work")
	defer func() {
		if err != nil {
			span.SetError(err)
		}
		span.Finish()
	}()

	// get work and process
	db, err := r.dbFinder()
	if err != nil {
		return fmt.Errorf("Error finding DB: %v", err)
	}
	tasks, dbErr := db.GetWork(spanCtx, r.sessionID, r.scanTask)
	if dbErr != nil {
		switch dbErr.Code() {
		case SQLErrorSessionNotFound:
			r.logger.Printf("Session expired. Getting new one")
			r.sessionID, err = db.StartSession(spanCtx)
			if err != nil {
				return fmt.Errorf("Error starting new session: %v", dbErr)
			}
		default:
			return fmt.Errorf("Error getting work from db: %v", dbErr)
		}

	}

	completedTasks, err := r.tasker(spanCtx, tasks)
	if err != nil {
		return fmt.Errorf("Error running tasks: %v", err)
	}

	taskIDs := make([]int64, len(completedTasks))
	for i, t := range completedTasks {
		taskIDs[i] = t.GetID()
	}

	dbErr = db.FinishTasks(spanCtx, taskIDs)
	if dbErr != nil {
		return fmt.Errorf("Error finishing tasks: %v", dbErr)
	}

	return nil
}

// Stop stops the runner from looping
func (r *Runner) Stop() {
	close(r.stop)
}