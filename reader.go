package rlogs

import (
	"fmt"
)

// Reader provides basic structured Reader with naive implementation
type Reader struct {
	LogEntries []*LogEntry
	QueueSize  int
}

// NewReader is constructor of Reader
func NewReader(entries []*LogEntry) *Reader {
	return &Reader{
		LogEntries: entries,
	}
}

// LogEntry is a pair of Loader and Paresr
type LogEntry struct {
	Src  LogSource
	Pipe Pipeline
}

func (x *Reader) Read(src LogSource) chan *LogQueue {
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
