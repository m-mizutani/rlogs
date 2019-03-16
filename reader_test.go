package rlogs_test

import (
	"fmt"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
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
	reader.DefineHandler("s3logs-test", "", &rlogs.S3Lines{}, &testLineParser{})

	count := 0
	for q := range reader.Load("ap-northeast-1", "s3logs-test", "test1.log") {
		count++
		require.NoError(t, q.Error)
	}
	assert.Equal(t, 10, count)
}

func TestBasicS3FileReader(t *testing.T) {
	reader := rlogs.NewReader()
	reader.DefineHandler("s3logs-test", "", &rlogs.S3File{}, &testFileParser{})

	count := 0
	for q := range reader.Load("ap-northeast-1", "s3logs-test", "test1.log") {
		count++
		require.NoError(t, q.Error)
	}
	assert.Equal(t, 10, count)
}

func TestBasicS3GzipReader(t *testing.T) {
	reader := rlogs.NewReader()
	reader.DefineHandler("s3logs-test", "", &rlogs.S3GzipLines{}, &testLineParser{})

	count := 0
	for q := range reader.Load("ap-northeast-1", "s3logs-test", "test2.log.gz") {
		count++
		require.NoError(t, q.Error)
	}
	assert.Equal(t, 10, count)
}

type myParser struct {
	regex *regexp.Regexp
}
type myLog struct {
	ipAddr   string
	userName string
	port     string
}

func newMyParser() *myParser {
	return &myParser{
		regex: regexp.MustCompile(`Invalid user (\S+) from (\S+) port (\d+)`),
	}
}

func (x *myParser) Parse(msg []byte) ([]rlogs.LogRecord, error) {
	line := string(msg)

	resp := x.regex.FindStringSubmatch(line)
	if len(resp) == 0 {
		return nil, nil
	}

	log := myLog{
		userName: resp[1],
		ipAddr:   resp[2],
		port:     resp[3],
	}

	return []rlogs.LogRecord{{
		Tag:       "my.log",
		Timestamp: time.Now().UTC(),
		Entity:    &log,
	}}, nil
}

func Example() {

	reader := rlogs.NewReader()
	reader.DefineHandler("s3logs-test", "", &rlogs.S3GzipLines{}, &myParser{})

	for q := range reader.Load("ap-northeast-1", "s3logs-test", "test3.log") {
		if log, ok := q.Record.Entity.(*myLog); ok {
			if log.userName == "root" {
				fmt.Println("found SSH root access challenge")
			}
		}
	}
}
