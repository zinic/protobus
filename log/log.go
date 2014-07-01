package log

import (
	"io"
	"os"
	"fmt"
	"strings"
)

const (
	DEFAULT_OUTPUT_FORMAT = "[%s] %s\n"
)

// Package variables and their init
var (
	DEBUG LogLevel
	INFO LogLevel
	WARNING LogLevel
	ERROR LogLevel

	rootLogger Logger
)

func init() {
	DEBUG = LogLevel {
		Name: "DEBUG",
		Weight: 0,
	}
	INFO = LogLevel {
		Name: "INFO",
		Weight: 1,
	}
	WARNING = LogLevel {
		Name: "WARNING",
		Weight: 2,
	}
	ERROR = LogLevel {
		Name: "ERROR",
		Weight: 3,
	}

	rootLogLevel := INFO
	if envLogLevel := os.Getenv("LOG"); envLogLevel != "" {
		switch strings.ToUpper(envLogLevel) {
		case DEBUG.Name:
			rootLogLevel = DEBUG
		case INFO.Name:
			rootLogLevel = INFO
		case WARNING.Name:
			rootLogLevel = WARNING
		case ERROR.Name:
			rootLogLevel = ERROR

		default:
			fmt.Printf("Unknown log level: %s.\n", envLogLevel)
		}
	}

	rootLogger = IoWriterLogger(rootLogLevel, os.Stdout)
	rootLogger.Infof("Root logger verbosity set to: %s.", rootLogLevel.Name)
}


// Logging levels
type LogLevel struct {
	Name string
	Weight int
}


// Logger interface
type Logger interface {
	Level() (level LogLevel)
	SetLevel(newLevel LogLevel)

	Log(level LogLevel, log string)
	Debug(log string)
	Info(log string)
	Warning(log string)
	Error(log string)

	Logf(level LogLevel, format string, args... interface{})
	Debugf(format string, args... interface{})
	Infof(format string, args... interface{})
	Warningf(format string, args... interface{})
	Errorf(format string, args... interface{})
}


// Default logger implementation
type LogHandler func(logger Logger, outputLevel LogLevel, log string)

type DefaultLogger struct {
	level LogLevel
	handler LogHandler
}

func (dl *DefaultLogger) Level() (level LogLevel) {
	return dl.level
}

func (dl *DefaultLogger) SetLevel(newLevel LogLevel) {
	dl.level = newLevel
}

func (dl *DefaultLogger) Log(level LogLevel, log string) {
	dl.handler(dl, level, fmt.Sprintf(DEFAULT_OUTPUT_FORMAT, level.Name, log))
}

func (dl *DefaultLogger) Debug(log string) {
	dl.Log(DEBUG, log)
}

func (dl *DefaultLogger) Info(log string) {
	dl.Log(INFO, log)
}

func (dl *DefaultLogger) Warning(log string) {
	dl.Log(WARNING, log)
}

func (dl *DefaultLogger) Error(log string) {
	dl.Log(ERROR, log)
}

func (dl *DefaultLogger) Logf(level LogLevel, format string, args... interface{}) {
	dl.Log(level, fmt.Sprintf(format, args...))
}

func (dl *DefaultLogger) Debugf(format string, args... interface{}) {
	dl.Logf(DEBUG, format, args...)
}

func (dl *DefaultLogger) Infof(format string, args... interface{}) {
	dl.Logf(INFO, format, args...)
}

func (dl *DefaultLogger) Warningf(format string, args... interface{}) {
	dl.Logf(WARNING, format, args...)
}

func (dl *DefaultLogger) Errorf(format string, args... interface{}) {
	dl.Logf(ERROR, format, args...)
}


// Logger for io.Writer
func IoWriterLogger(level LogLevel, out io.Writer) (logger Logger) {
	return &DefaultLogger {
		level: level,
		handler: func(logger Logger, outputLevel LogLevel, log string) {
			if outputLevel.Weight >= logger.Level().Weight {
				fmt.Fprint(out, log)
			}
		},
	}
}

// Logger that delegates to another logger and copies its log level
func DelegateLogger(delegate Logger) (logger Logger) {
	return &DefaultLogger {
		level: delegate.Level(),
		handler: func(logger Logger, outputLevel LogLevel, log string) {
			if outputLevel.Weight >= logger.Level().Weight {
				logger.Log(outputLevel, log)
			}
		},
	}
}


// Root-level logger
func RootLogger() (log Logger) {
	return rootLogger
}

func NewLogger(name string) (log Logger) {
	return DelegateLogger(rootLogger)
}


// Nice root-level methods
func Debug(log string) {
	rootLogger.Debugf(log)
}

func Info(log string) {
	rootLogger.Infof(log)
}

func Warning(log string) {
	rootLogger.Warningf(log)
}

func Error(log string) {
	rootLogger.Errorf(log)
}

func Debugf(format string, args... interface{}) {
	rootLogger.Debugf(format, args...)
}

func Infof(format string, args... interface{}) {
	rootLogger.Infof(format, args...)
}

func Warningf(format string, args... interface{}) {
	rootLogger.Warningf(format, args...)
}

func Errorf(format string, args... interface{}) {
	rootLogger.Errorf(format, args...)
}
