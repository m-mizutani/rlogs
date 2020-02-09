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
		Seq: 0,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, len(logs))

	logs, err = psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(lines[1]),
		Src: src,
		Seq: 1,
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

func TestVpcFlowLogsParserForV3(t *testing.T) {
	lines := []string{
		`version account-id interface-id srcaddr dstaddr srcport dstport protocol packets bytes start end action log-status instance-id pkt-dstaddr pkt-srcaddr subnet-id tcp-flags type vpc-id`,
		`3 1234567890 eni-06bec2a3c4f1474f6 172.30.0.100 52.196.35.56 51282 443 6 13 5891 1581206401 1581206403 ACCEPT OK i-05c7d5c9925dc669d 52.196.35.56 172.30.0.100 subnet-02d24420af123455 3 IPv4 vpc-038e2f511f79682c4`,
		`3 1234567890 eni-06bec2a3c4f1474f6 52.196.35.56 172.30.0.100 443 51282 6 10 4698 1581206401 1581206403 ACCEPT OK i-07d5c9925dc669d 172.30.0.100 52.196.35.56 subnet-02d24420af123455 19 IPv4 vpc-038e2f511f79682c4`,
	}

	src := &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"}
	psr := parser.VpcFlowLogs{}

	logs, err := psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(lines[0]),
		Src: src,
		Seq: 0,
	})
	require.NoError(t, err)
	assert.Equal(t, 0, len(logs))

	logs, err = psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(lines[1]),
		Src: src,
		Seq: 1,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, len(logs))
	log := logs[0].Values.(*parser.VpcFlowLog)
	assert.Equal(t, "3", log.Version)
	assert.Equal(t, "1234567890", log.AccountID)
	assert.Equal(t, "eni-06bec2a3c4f1474f6", log.InterfaceID)
	assert.Equal(t, "i-05c7d5c9925dc669d", log.InstanceID)
	assert.Equal(t, "52.196.35.56", log.PktDstAddr)
	assert.Equal(t, "172.30.0.100", log.PktSrcAddr)
	assert.Equal(t, "subnet-02d24420af123455", log.SubnetID)
	assert.Equal(t, "3", log.TCPFlags)
	assert.Equal(t, "IPv4", log.Type)
	assert.Equal(t, "vpc-038e2f511f79682c4", log.VpcID)

}

func TestCloudTrailParserErrorCase(t *testing.T) {
	hdr := `version account-id interface-id srcaddr dstaddr srcport dstport protocol packets bytes start end action log-status`

	tooShort := `2 1234567890 eni-0bdfe84b34abcdedf 10.10.102.238 10.10.163.10 43210 80 6 2 341 1554076587 1554076828 ACCEPT`
	tooLong := `2 1234567890 eni-0bdfe84b34abcdedf 10.10.102.238 10.10.163.10 43210 80 6 2 341 1554076587 1554076828 ACCEPT OK ?`

	src := &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"}
	psr := parser.VpcFlowLogs{}

	var err error
	_, err = psr.Parse(&rlogs.MessageQueue{Raw: []byte(hdr), Src: src, Seq: 0})
	require.NoError(t, err)

	_, err = psr.Parse(&rlogs.MessageQueue{Raw: []byte(tooShort), Src: src, Seq: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid row length")

	_, err = psr.Parse(&rlogs.MessageQueue{Raw: []byte(tooLong), Src: src, Seq: 1})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "Invalid row length")
}
