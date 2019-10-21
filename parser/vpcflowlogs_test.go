package parser_test

import (
	"testing"

	"github.com/m-mizutani/rlogs"
	"github.com/m-mizutani/rlogs/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestVpcFlowLogsParser(t *testing.T) {
	lines := []string{
		`version account-id interface-id srcaddr dstaddr srcport dstport protocol packets bytes start end action log-status`,
		`2 1234567890 eni-0bdfe84b34abcdedf 10.10.102.238 10.10.163.10 43210 80 6 2 341 1554076587 1554076828 ACCEPT OK`,
	}

	src := &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"}
	psr := parser.VpcFlowLogs{}

	logs, err := psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(lines[0]),
		Src: src,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, len(logs))

	logs, err = psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(lines[1]),
		Src: src,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, len(logs))
	log := logs[0].Values.(*parser.VpcFlowLog)
	assert.Equal(t, "2", log.Version)
	assert.Equal(t, "1234567890", log.AccountID)
	assert.Equal(t, "eni-0bdfe84b34abcdedf", log.InterfaceID)
	assert.Equal(t, "10.10.102.238", log.SrcAddr)
	assert.Equal(t, "10.10.163.10", log.DstAddr)
	assert.Equal(t, "43210", log.SrcPort)
	assert.Equal(t, "80", log.DstPort)
	assert.Equal(t, "6", log.Protocol)
	assert.Equal(t, "2", log.Packets)
	assert.Equal(t, "341", log.Bytes)
	assert.Equal(t, "1554076587", log.Start)
	assert.Equal(t, "1554076828", log.End)
	assert.Equal(t, "ACCEPT", log.Action)
	assert.Equal(t, "OK", log.LogStatus)
}

func TestCloudTrailParserErrorCase(t *testing.T) {
	notVersion2 := `3 1234567890 eni-0bdfe84b34abcdedf 10.10.102.238 10.10.163.10 43210 80 6 2 341 1554076587 1554076828 ACCEPT OK`
	tooShort := `2 1234567890 eni-0bdfe84b34abcdedf 10.10.102.238 10.10.163.10 43210 80 6 2 341 1554076587 1554076828 ACCEPT`
	tooLong := `2 1234567890 eni-0bdfe84b34abcdedf 10.10.102.238 10.10.163.10 43210 80 6 2 341 1554076587 1554076828 ACCEPT OK ?`

	src := &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"}
	psr := parser.VpcFlowLogs{}

	var err error
	_, err = psr.Parse(&rlogs.MessageQueue{Raw: []byte(notVersion2), Src: src})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Unsupported VPC Flow Logs version")

	_, err = psr.Parse(&rlogs.MessageQueue{Raw: []byte(tooShort), Src: src})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid row length")

	_, err = psr.Parse(&rlogs.MessageQueue{Raw: []byte(tooLong), Src: src})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid row length")
}
