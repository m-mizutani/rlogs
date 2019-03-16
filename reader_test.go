package rlogs_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/m-mizutani/rlogs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testLog struct {
	month string
	day   string
	tm    string
	host  string
	proc  string
	msg   string
}

func toTestLog(line string) (*testLog, error) {
	arr := strings.Split(line, " ")
	if len(arr) < 6 {
		return nil, errors.New("message is too short")
	}
	log := testLog{
		month: arr[0],
		day:   arr[1],
		tm:    arr[2],
		host:  arr[3],
		proc:  arr[4],
		msg:   strings.Join(arr[5:len(arr)], " "),
	}

	return &log, nil
}

type testLineParser struct{}

func (x *testLineParser) Parse(msg []byte) ([]rlogs.LogRecord, error) {
	log, err := toTestLog(string(msg))
	if err != nil {
		return nil, err
	}

	return []rlogs.LogRecord{{
		Tag:       "test.log",
		Timestamp: time.Now().UTC(),
		Entity:    &log,
	}}, nil
}

type testFileParser struct{}

func (x *testFileParser) Parse(msg []byte) ([]rlogs.LogRecord, error) {
	body := string(msg)

	var logs []rlogs.LogRecord
	for _, line := range strings.Split(body, "\n") {
		if len(line) == 0 {
			continue
		}

		log, err := toTestLog(line)
		if err != nil {
			return nil, err
		}

		logs = append(logs, rlogs.LogRecord{
			Tag:       "test.log",
			Timestamp: time.Now().UTC(),
			Entity:    &log,
		})
	}

	return logs, nil
}

func TestBasicS3LineReader(t *testing.T) {
	reader := rlogs.NewReader()
	reader.AddHandler("s3logs-test", "", &rlogs.S3Lines{}, &testLineParser{})

	count := 0
	for q := range reader.Load("ap-northeast-1", "s3logs-test", "test1.log") {
		count++
		require.NoError(t, q.Error)
	}
	assert.Equal(t, 10, count)
}

func TestBasicS3FileReader(t *testing.T) {
	reader := rlogs.NewReader()
	reader.AddHandler("s3logs-test", "", &rlogs.S3File{}, &testFileParser{})

	count := 0
	for q := range reader.Load("ap-northeast-1", "s3logs-test", "test1.log") {
		count++
		require.NoError(t, q.Error)
	}
	assert.Equal(t, 10, count)
}

func TestBasicS3GzipReader(t *testing.T) {
	reader := rlogs.NewReader()
	reader.AddHandler("s3logs-test", "", &rlogs.S3GzipLines{}, &testLineParser{})

	count := 0
	for q := range reader.Load("ap-northeast-1", "s3logs-test", "test2.log.gz") {
		count++
		require.NoError(t, q.Error)
	}
	assert.Equal(t, 10, count)
}
