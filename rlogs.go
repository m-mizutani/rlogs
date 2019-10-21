package rlogs

import "time"

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
