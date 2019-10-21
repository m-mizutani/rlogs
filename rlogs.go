package rlogs

import (
	"time"

	"github.com/pkg/errors"
)

// LogQueue is message queue between Reader and main procedure.
// It includes both of LogRecord and Error but should be set either one.
type LogQueue struct {
	Log   *LogRecord
	Error error
}

// LogRecord has not only log message (original log) but also parsed meta data.
type LogRecord struct {
	// Tag indicates log type (log schema)
	Tag string
	// Timestamp comes from log data
	Timestamp time.Time
	// Raw is raw log data
	Raw []byte
	// Value is parsed log data
	Values interface{}
	// Sequence number in log object
	Seq int
	// Log source location
	Src LogSource
}

// MessageQueue is a queue bring raw log message and sequence between Loader and Parser
type MessageQueue struct {
	Error error
	Raw   []byte
	Seq   int
	Src   LogSource
}

// Parser converts raw log message to LogRecord(s)
type Parser interface {
	Parse(msg *MessageQueue) ([]*LogRecord, error)
}

// Loader downloads object from cloud object storage and create MessageQueue(s)
type Loader interface {
	Load(src LogSource) chan *MessageQueue
}

// Pipeline is a pair of Parser and Loader.
type Pipeline struct {
	Ldr       Loader
	Psr       Parser
	QueueSize int
}

// Run of Pipeline downloads object and parse it.
func (x *Pipeline) Run(src LogSource, ch chan *LogQueue) {
	defer close(ch)

	msgch := x.Ldr.Load(src)
	for msg := range msgch {
		if msg.Error != nil {
			ch <- &LogQueue{Error: errors.Wrap(msg.Error, "Fail to load log message")}
			return
		}

		logs, err := x.Psr.Parse(msg)
		if err != nil {
			ch <- &LogQueue{Error: errors.Wrap(msg.Error, "Fail to parse log message")}
			return
		}

		for i := range logs {
			ch <- &LogQueue{Log: logs[i]}
		}
	}
}

// String function just converts string to string pointer
func String(s string) *string { return &s }
