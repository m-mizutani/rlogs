package rlogs_test

import (
	"fmt"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/m-mizutani/rlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyS3ClientForBasicReader struct{}

func (x *dummyS3ClientForBasicReader) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if *input.Bucket != "some-bucket" {
		return nil, fmt.Errorf("invalid bucket")
	}

	switch *input.Key {
	case "magic/history.json":
		lines := []string{
			`{"ts":"1902-10-10T10:00:00","name":"?","number":1}`,
			`{"ts":"1929-10-10T10:00:00","name":"Parallel Worlds","number":2}`,
			`{"ts":"1954-10-10T10:00:00","name":"Heaven's Feel","number":3}`,
			`{"ts":"1983-10-10T10:00:00","name":"?","number":4}`,
			`{"ts":"1991-10-10T10:00:00","name":"Blue","number":5}`,
		}
		return &s3.GetObjectOutput{
			Body: toReadCloser(strings.Join(lines, "\n")),
		}, nil

	default:
		return nil, fmt.Errorf("Key not found")
	}
}

func TestBasicReader(t *testing.T) {
	dummy := dummyS3ClientForBasicReader{}
	rlogs.InjectNewS3Client(&dummy)
	defer rlogs.FixNewS3Client()

	reader := rlogs.BasicReader{
		LogEntries: []*rlogs.LogEntry{
			{
				Psr: &rlogs.JSONParser{
					Tag:             "ts",
					TimestampField:  rlogs.String("ts"),
					TimestampFormat: rlogs.String("2006-01-02T15:04:05"),
				},
				Ldr: &rlogs.S3LineLoader{},
				Src: &rlogs.AwsS3LogSource{
					Region: "some-region",
					Bucket: "some-bucket",
					Key:    "magic/",
				},
			},
		},
	}

	ch := reader.Read(&rlogs.AwsS3LogSource{
		Region: "some-region",
		Bucket: "some-bucket",
		Key:    "magic/history.json",
	})
	var logs []*rlogs.LogRecord
	for log := range ch {
		require.NoError(t, log.Error)
		logs = append(logs, log.Log)
	}

	assert.Equal(t, 5, len(logs))
	v4, ok := logs[4].Values.(map[string]interface{})
	assert.True(t, ok)
	n4, ok := v4["name"].(string)
	assert.True(t, ok)
	assert.Equal(t, "Blue", n4)
}
