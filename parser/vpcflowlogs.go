package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/m-mizutani/rlogs"
)

// VpcFlowLog is traffic record generated AWS VPC FlowLogs.
// Type of fields that may have number is defined as string intentionally
// because there is non number values such as "-" in SrcPort, DstPort and so on.
type VpcFlowLog struct {
	Version     string
	AccountID   string
	InterfaceID string
	SrcAddr     string
	DstAddr     string
	SrcPort     string
	DstPort     string
	Protocol    string
	Packets     string
	Bytes       string
	Start       string
	End         string
	Action      string
	LogStatus   string

	// New for FlowLogs v3
	InstanceID string
	PktSrcAddr string
	PktDstAddr string
	SubnetID   string
	TCPFlags   string
	Type       string
	VpcID      string
}

// VpcFlowLogs is parser of VPC FlowLogs in AWS S3. The parser supports only S3 object
// deliveried by VPC FlowLogs function directly.
type VpcFlowLogs struct {
	index []int
}

var vpcFlowLogsIndex = map[string]int{
	// v2
	"version":      0,
	"account-id":   1,
	"interface-id": 2,
	"srcaddr":      3,
	"dstaddr":      4,
	"srcport":      5,
	"dstport":      6,
	"protocol":     7,
	"packets":      8,
	"bytes":        9,
	"start":        10,
	"end":          11,
	"action":       12,
	"log-status":   13,

	// v3
	"instance-id": 14,
	"pkt-srcaddr": 15,
	"pkt-dstaddr": 16,
	"subnet-id":   17,
	"tcp-flags":   18,
	"type":        19,
	"vpc-id":      20,

	// v4
	"region":           21,
	"az-id":            22,
	"sublocation-type": 23,
	"sublocation-id":   24,
}

// Parse of VpcFlowLogs parses flow log with ignoring header.
func (x *VpcFlowLogs) Parse(msg *rlogs.MessageQueue) ([]*rlogs.LogRecord, error) {
	raw := string(msg.Raw)
	row := strings.Split(raw, " ")

	if msg.Seq == 0 { // header
		x.index = []int{}
		for i, r := range row {
			idx, ok := vpcFlowLogsIndex[r]
			if !ok {
				return nil, fmt.Errorf("Invalid header item: %s at %d column", r, i)
			}

			x.index = append(x.index, idx)
		}

		return nil, nil // Skip header
	}

	if len(row) != len(x.index) {
		return nil, fmt.Errorf("Invalid row length (expected %d, but %d)", len(x.index), len(row))
	}

	buf := make([]string, len(vpcFlowLogsIndex))
	for i := range row {
		buf[x.index[i]] = row[i]
	}

	log := VpcFlowLog{
		Version:     buf[0],
		AccountID:   buf[1],
		InterfaceID: buf[2],
		SrcAddr:     buf[3],
		DstAddr:     buf[4],
		SrcPort:     buf[5],
		DstPort:     buf[6],
		Protocol:    buf[7],
		Packets:     buf[8],
		Bytes:       buf[9],
		Start:       buf[10],
		End:         buf[11],
		Action:      buf[12],
		LogStatus:   buf[13],

		InstanceID: buf[14],
		PktSrcAddr: buf[15],
		PktDstAddr: buf[16],
		SubnetID:   buf[17],
		TCPFlags:   buf[18],
		Type:       buf[19],
		VpcID:      buf[20],
	}

	var ts time.Time
	if log.Start != "" {
		if n10, err := strconv.Atoi(log.Start); err == nil {
			ts = time.Unix(int64(n10), 0)
		}
	}

	return []*rlogs.LogRecord{
		{
			Tag:       "aws.vpcflowlogs",
			Timestamp: ts,
			Raw:       msg.Raw,
			Values:    &log,
			Seq:       msg.Seq,
			Src:       msg.Src,
		},
	}, nil
}
