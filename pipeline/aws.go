package pipeline

import (
	"github.com/m-mizutani/rlogs"
	"github.com/m-mizutani/rlogs/parser"
)

// NewVpcFlowLogs provides set of Parser and Loader for VPC FlowLogs
func NewVpcFlowLogs() rlogs.Pipeline {
	return rlogs.Pipeline{
		Psr: &parser.VpcFlowLogs{},
		Ldr: &rlogs.S3LineLoader{},
	}
}

// NewCloudTrail provides set of Parser and Loader for CloudTrail logs
func NewCloudTrail() rlogs.Pipeline {
	return rlogs.Pipeline{
		Psr: &parser.CloudTrail{},
		Ldr: &rlogs.S3FileLoader{},
	}
}
