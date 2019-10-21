package rlogs

import (
	"fmt"
)

// Reader is interface of log load and parse
type Reader interface {
	Read(src LogSource) chan *LogQueue
}

// BasicReader provides basic structured Reader with naive implementation
type BasicReader struct {
	LogEntries []*LogEntry
	QueueSize  int
}

// LogEntry is a pair of Loader and Paresr
type LogEntry struct {
	Src  LogSource
	Pipe Pipeline
}

func (x *BasicReader) Read(src LogSource) chan *LogQueue {
	queueSize := 128
	if x.QueueSize > 0 {
		queueSize = x.QueueSize
	}
	ch := make(chan *LogQueue, queueSize)

	var entry *LogEntry
	for _, e := range x.LogEntries {
		if e.Src.Contains(src) {
			entry = e
			break
		}
	}

	if entry == nil {
		ch <- &LogQueue{Error: fmt.Errorf("No matched LogEntry for %v", src)}
		return ch
	}

	go entry.Pipe.Run(src, ch)

	return ch
}
