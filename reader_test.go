package rlogs_test

import (
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/m-mizutani/rlogs"
	"github.com/m-mizutani/rlogs/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type dummyS3ClientForReader struct{}

func (x *dummyS3ClientForReader) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if *input.Bucket != "your-bucket" {
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

	case "http/log.json":
		lines := []string{
			`{"ts":"2019-10-10T10:00:00","src":"10.1.2.3","port":34567,"path":"/hello"}`,
			`{"ts":"2019-10-10T10:00:02","src":"10.2.3.4","port":45678,"path":"/world"}`,
		}
		return &s3.GetObjectOutput{
			Body: toReadCloser(strings.Join(lines, "\n")),
		}, nil

	default:
		return nil, fmt.Errorf("Key not found")
	}
}

func makeTestPipeline() rlogs.Pipeline {
	return rlogs.Pipeline{
		Psr: &parser.JSON{
			Tag:             "ts",
			TimestampField:  rlogs.String("ts"),
			TimestampFormat: rlogs.String("2006-01-02T15:04:05"),
		},
		Ldr: &rlogs.S3LineLoader{},
	}
}

func TestReader(t *testing.T) {
	dummy := dummyS3ClientForReader{}
	rlogs.InjectNewS3Client(&dummy)
	defer rlogs.FixNewS3Client()

	reader := rlogs.NewReader([]*rlogs.LogEntry{
		{
			Pipe: makeTestPipeline(),
			Src: &rlogs.AwsS3LogSource{
				Region: "some-region",
				Bucket: "your-bucket",
				Key:    "magic/",
			},
		},
	})

	ch := reader.Read(&rlogs.AwsS3LogSource{
		Region: "some-region",
		Bucket: "your-bucket",
		Key:    "magic/history.json",
	})
	var logs []*rlogs.LogRecord
	for q := range ch {
		require.NoError(t, q.Error)
		logs = append(logs, q.Log)
	}

	assert.Equal(t, 5, len(logs))
	v4, ok := logs[4].Values.(map[string]interface{})
	assert.True(t, ok)
	n4, ok := v4["name"].(string)
	assert.True(t, ok)
	assert.Equal(t, "Blue", n4)
}

func TestReaderNotFound(t *testing.T) {
	dummy := dummyS3ClientForReader{}
	rlogs.InjectNewS3Client(&dummy)
	defer rlogs.FixNewS3Client()

	reader := rlogs.Reader{
		LogEntries: []*rlogs.LogEntry{
			{
				Pipe: makeTestPipeline(),
				Src: &rlogs.AwsS3LogSource{
					Region: "ap-northeast-1",
					Bucket: "your-bucket",
					Key:    "http/",
				},
			},
		},
	}

	ch := reader.Read(&rlogs.AwsS3LogSource{
		Region: "some-region",
		Bucket: "your-bucket",
		Key:    "key-is-not-found",
	})
	q := <-ch
	assert.Error(t, q.Error)
}

func ExampleReader() {
	// To avoid accessing actual S3.
	dummy := dummyS3ClientForReader{}
	rlogs.InjectNewS3Client(&dummy)
	defer rlogs.FixNewS3Client()

	// Example is below
	pipeline := rlogs.Pipeline{
		Psr: &parser.JSON{
			Tag:             "ts",
			TimestampField:  rlogs.String("ts"),
			TimestampFormat: rlogs.String("2006-01-02T15:04:05"),
		},
		Ldr: &rlogs.S3LineLoader{},
	}

	reader := rlogs.NewReader([]*rlogs.LogEntry{
		{
			Pipe: pipeline,
			Src: &rlogs.AwsS3LogSource{
				Region: "ap-northeast-1",
				Bucket: "your-bucket",
				Key:    "http/",
			},
		},
	})

	// s3://your-bucket/http/log.json is following:
	// {"ts":"2019-10-10T10:00:00","src":"10.1.2.3","port":34567,"path":"/hello"}
	// {"ts":"2019-10-10T10:00:02","src":"10.2.3.4","port":45678,"path":"/world"}

	ch := reader.Read(&rlogs.AwsS3LogSource{
		Region: "ap-northeast-1",
		Bucket: "your-bucket",
		Key:    "http/log.json",
	})

	for q := range ch {
		if q.Error != nil {
			log.Fatal(q.Error)
		}
		values := q.Log.Values.(map[string]interface{})
		fmt.Printf("[log] tag=%s time=%s src=%v\n", q.Log.Tag, q.Log.Timestamp, values["src"])
	}
	// Output:
	// [log] tag=ts time=2019-10-10 10:00:00 +0000 UTC src=10.1.2.3
	// [log] tag=ts time=2019-10-10 10:00:02 +0000 UTC src=10.2.3.4
}
