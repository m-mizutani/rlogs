package parser_test

import (
	"testing"

	"github.com/m-mizutani/rlogs"
	"github.com/m-mizutani/rlogs/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCloudTrailParser(t *testing.T) {
	// Sample original: https://docs.aws.amazon.com/awscloudtrail/latest/userguide/cloudtrail-log-file-examples.html
	// NOTE: add one more cloudtrail record to .Records
	msg := `{"Records":[{"eventVersion":"1.0","userIdentity":{"type":"IAMUser","principalId":"EX_PRINCIPAL_ID","arn":"arn:aws:iam::123456789012:user/Alice","accessKeyId":"EXAMPLE_KEY_ID","accountId":"123456789012","userName":"Alice"},"eventTime":"2014-03-06T21:22:54Z","eventSource":"ec2.amazonaws.com","eventName":"StartInstances","awsRegion":"us-east-2","sourceIPAddress":"205.251.233.176","userAgent":"ec2-api-tools 1.6.12.2","requestParameters":{"instancesSet":{"items":[{"instanceId":"i-ebeaf9e2"}]}},"responseElements":{"instancesSet":{"items":[{"instanceId":"i-ebeaf9e2","currentState":{"code":0,"name":"pending"},"previousState":{"code":80,"name":"stopped"}}]}}},{"eventVersion":"1.0","userIdentity":{"type":"IAMUser","principalId":"EX_PRINCIPAL_ID","arn":"arn:aws:iam::123456789012:user/Alice","accessKeyId":"EXAMPLE_KEY_ID","accountId":"123456789012","userName":"Alice"},"eventTime":"2014-03-06T21:32:54Z","eventSource":"ec2.amazonaws.com","eventName":"StartInstances","awsRegion":"us-east-2","sourceIPAddress":"205.251.233.176","userAgent":"ec2-api-tools 1.6.12.2","requestParameters":{"instancesSet":{"items":[{"instanceId":"i-ebeaf9e2"}]}},"responseElements":{"instancesSet":{"items":[{"instanceId":"i-ebeaf9e2","currentState":{"code":0,"name":"pending"},"previousState":{"code":80,"name":"stopped"}}]}}}]}`
	src := &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"}
	psr := parser.CloudTrail{}

	logs, err := psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(msg),
		Src: src,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, len(logs))

	rec := logs[0].Values.(parser.CloudTrailRecord)
	assert.Equal(t, "2014-03-06T21:22:54", logs[0].Timestamp.Format("2006-01-02T15:04:05"))
	assert.Equal(t, "aws.cloudtrail", logs[0].Tag)

	assert.Equal(t, "1.0", rec["eventVersion"])
	assert.Equal(t, "EX_PRINCIPAL_ID", rec["userIdentity"].(map[string]interface{})["principalId"])
	assert.Equal(t, "Alice", rec["userIdentity"].(map[string]interface{})["userName"])
	_, ok := rec["requestParameters"].(map[string]interface{})["instancesSet"]
	assert.True(t, ok)
	_, ok = rec["responseElements"].(map[string]interface{})["instancesSet"]
	assert.True(t, ok)
}
