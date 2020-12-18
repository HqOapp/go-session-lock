# go-session-lock

This repository provides postgres migration templates and a lock package that, when used together, will run tasks on an interval within a distributed system.
The lock package relies on postgres to ensure that tasks are run successfully once and that the tasks are load balanced across all instances of the services running the lock
package and connecting to the same postgres instance.

The lock package supports optional logging and tracing in an [open tracing](https://opentracing.io/) friendly way.

The postgres schema and function template assume you are using a migration tool that supports running some files exactly once and others every run.  
Specifically the files are formated for use with [away-team/migrate](https://github.com/away-team/migrate).

## Glossary

* Task - A job that needs to be accomplished once.
* Tasker - A function that can accomplish an array of tasks, returning the list of accomplished tasks
* Session - A session to load balance tasks across.  Generally each instance of a service would have 1 Runner which would have 1 session.  If a Runner dies the session
will expire and the tasks will be redistributed to remaining sessions.
* Runner - The main processor that will tick on the interval provided, keep the session alive, request work from the DB for this session, do the work using the Tasker and flag
successfully completed work as complete.


## Implementation

1. Copy the files in ./migration to your migration directory and rename/modify their numbers as necessary.
2. Follow the TODOs in the migration files
    * Modify the sessions.up.sql file with an `ALTER TABLE` command to add a `session_id BIGINT` column to the table that stores your task information.
    * Modify the tasks.alwaysup.sql to fill in each of the plpgsql functions following the commented TODOs.  Each function has a basic example commented out for reference.
3. Implement the `lock.Task` interface on a struct that contains all the necessary task information.
4. Implement a `lock.ScanTask` function that can scan the results of the `get_work` plpgsql function into the `lock.Task` implemented in step 3.
5. Implement a `lock.Tasker` function that get complete a set of given tasks and return the tasks that were completed.
6. Implement a `lock.Database` that can call the plpgsql functions previously defined.
7. Implement a `lock.DBFinder` that can get the current `lock.Database` instance.  This allows for DBs to move between intervals if necessary in your environment.
8. Instantiate a `lock.Runner`.
9. Call `Run()` on the Runner to start the ticker loop.
10. Call `Stop()` on the Runner to stop the ticker loop.  It is a good idea to call `Stop()` during graceful service shutdowns.
