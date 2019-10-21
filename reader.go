package rlogs

import (
	"fmt"

	"github.com/pkg/errors"
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
	Src LogSource
	Ldr Loader
	Psr Parser
}

func (x *BasicReader) Read(src LogSource) chan *LogQueue {
	queueSize := 128
	if x.QueueSize > 0 {
		queueSize = x.QueueSize
	}

	ch := make(chan *LogQueue, queueSize)
	go func() {
		defer close(ch)

		var entry *LogEntry
		for _, e := range x.LogEntries {
			if e.Src.Contains(src) {
				entry = e
				break
			}
		}

		if entry == nil {
			ch <- &LogQueue{Error: fmt.Errorf("No matched LogEntry for %v", src)}
			return
		}

		msgch := entry.Ldr.Load(src)
		for msg := range msgch {
			if msg.Error != nil {
				ch <- &LogQueue{Error: errors.Wrap(msg.Error, "Fail to load log message")}
				return
			}

			logs, err := entry.Psr.Parse(msg)
			if err != nil {
				ch <- &LogQueue{Error: errors.Wrap(msg.Error, "Fail to parse log message")}
				return
			}

			for i := range logs {
				ch <- &LogQueue{Log: logs[i]}
			}
		}
	}()

	return ch
}
