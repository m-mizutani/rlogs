package rlogs

// Reader is interface of log load and parse
type Reader interface {
	Read(src LogSource) chan *LogQueue
}

// BaseReader provides basic structured Reader with naive implementation
type BaseReader struct {
	Pipelines []*Pipeline
}

// Pipeline is a pair of Loader and Paresr
type Pipeline struct {
	Src LogSource
	Ldr Loader
	Psr Parser
}
