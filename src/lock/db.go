package lock

import (
	"context"

	"github.com/healthimation/go-glitch/glitch"
)

//go:generate mockgen -destination=./mocks/db-mock.go -package=lockmock github.com/promoboxx/go-session-lock/src/lock Database,Task,Scanner

// SQL errors
const (
	SQLErrorSessionNotFound = "SL001"
)

// Database can make the PG calls necessary to use a session locked runner
type Database interface {
	StartSession(ctx context.Context) (int64, glitch.DataError)
	EndSession(ctx context.Context, sessionID int64) glitch.DataError
	BumpSession(ctx context.Context, sessionID int64) glitch.DataError
	GetWork(ctx context.Context, sessionID int64, scanTask ScanTask) ([]Task, glitch.DataError)
	FinishTasks(ctx context.Context, taskIDs []string) glitch.DataError
}

// Task is an interface that can GetID - This is meant to be implemented as a struct that holds all task info that
// The Tasker needs to do the work associated with the task.
type Task interface {
	GetID() string
}

// DBFinder will return an Database implementation
// This will be called every loop in case the DB moves
type DBFinder func() (Database, error)

// Scanner is an interface for the database/sql Scan function.  sql.Rows and sql.Row implement this
type Scanner interface {
	Scan(dest ...interface{}) error
}

// ScanTask can scan the data from Get work and store it in a struct.  That struct should be returned and will be added to the GetWork array.
type ScanTask func(row Scanner) (Task, glitch.DataError)
