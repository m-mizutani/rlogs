package pipeline

import (
	"github.com/m-mizutani/rlogs"
)

type nullLoader struct{}

func (x *nullLoader) Load(src rlogs.LogSource) chan *rlogs.MessageQueue {
	return nil
}

// NewIgnore provides set of Parser and Loader for VPC FlowLogs
func NewIgnore() rlogs.Pipeline {
	return rlogs.Pipeline{
		Psr: nil,
		Ldr: &nullLoader{},
	}
}
