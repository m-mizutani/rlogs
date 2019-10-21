package parser

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/m-mizutani/rlogs"
)

// VpcFlowLog is traffic record generated AWS VPC FlowLogs.
// Type of fields taht may have number is defined as string intentionally
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
}

// VpcFlowLogs is parser of VPC FlowLogs in AWS S3. The parser supports only S3 object
// deliveried by VPC FlowLogs function directly.
type VpcFlowLogs struct{}

// Parse of VpcFlowLogs parses flow log with ignoring header.
func (x *VpcFlowLog) Parse(msg *rlogs.MessageQueue) ([]*rlogs.LogRecord, error) {
	raw := string(msg.Raw)
	row := strings.Split(raw, " ")
	if len(row) != 14 {
		return nil, fmt.Errorf("Invalid row length (expected 14, but %d)", len(row))
	}

	if row[0] == "version" {
		return nil, nil // Skip header
	}

	if row[0] != "2" {
		return nil, fmt.Errorf("Unsupported VPC Flow Logs version: %s", row[0])
	}

	log := VpcFlowLog{
		AccountID:   row[1],
		InterfaceID: row[2],
		SrcAddr:     row[3],
		DstAddr:     row[4],
		SrcPort:     row[5],
		DstPort:     row[6],
		Protocol:    row[7],
		Packets:     row[8],
		Bytes:       row[9],
		Start:       row[10],
		End:         row[11],
		Action:      row[12],
		LogStatus:   row[13],
	}

	var ts time.Time
	if n10, err := strconv.Atoi(row[10]); err == nil {
		ts = time.Unix(int64(n10), 0)
	}

	return []*rlogs.LogRecord{
		{
			Tag:       "aws.vpcflowlogs",
			Timestamp: ts,
			Raw:       msg.Raw,
			Values:    log,
			Seq:       msg.Seq,
			Src:       msg.Src,
		},
	}, nil
}
