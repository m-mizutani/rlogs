package s3logs_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m-mizutani/s3logs"
)

type testParser struct{}
type testLog struct {
	month string
	day   string
	tm    string
	host  string
	proc  string
	msg   string
}

func (x *testParser) Parse(msg []byte) ([]s3logs.LogRecord, error) {
	s := string(msg)
	arr := strings.Split(s, " ")
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

	return []s3logs.LogRecord{{
		Tag:       "test.log",
		Timestamp: time.Now().UTC(),
		Entity:    &log,
	}}, nil
}

func TestBasicS3Reader(t *testing.T) {
	reader := s3logs.NewReader()
	reader.AddHandler("s3logs-test", "", &s3logs.S3Lines{}, &testParser{})

	count := 0
	for q := range reader.Load("ap-northeast-1", "s3logs-test", "test1.log") {
		count++
		require.NoError(t, q.Error)
	}
	assert.Equal(t, 10, count)
}
