package lock

// Logger allows you to control how logging happens
type Logger interface {
	Printf(format string, a ...interface{})
}

type noopLogger struct{}

func (nl *noopLogger) Printf(format string, a ...interface{}) {}
