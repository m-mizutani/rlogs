package parser_test

import (
	"testing"

	"github.com/m-mizutani/rlogs"
	"github.com/m-mizutani/rlogs/parser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestJSONParserTimestamp(t *testing.T) {
	psr := parser.JSON{
		TimestampField:  rlogs.String("ts"),
		TimestampFormat: rlogs.String("2006-01-02T15:04:05"),
	}
	logs, err := psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(`{"color":"blue","ts":"2019-10-19T04:44:44"}`),
		Src: &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, len(logs))
	src, ok := logs[0].Src.(*rlogs.AwsS3LogSource)
	require.True(t, ok)
	assert.Equal(t, "test-r", src.Region)
	assert.Equal(t, "test-b", src.Bucket)
	assert.Equal(t, "test-k", src.Key)
	assert.Equal(t, 2019, logs[0].Timestamp.Year())
	assert.Equal(t, 4, logs[0].Timestamp.Hour())
}

func TestJSONParserUnixtime(t *testing.T) {
	psr := parser.JSON{
		UnixtimeField:   rlogs.String("unix"), // prioritized
		TimestampField:  rlogs.String("ts"),
		TimestampFormat: rlogs.String("2006-01-02T15:04:05"),
	}
	logs, err := psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(`{"color":"blue","ts":"2019-10-19T04:44:44","unix":1571630400}`),
		Src: &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, len(logs))
	assert.Equal(t, 21, logs[0].Timestamp.Day())
	assert.Equal(t, 4, logs[0].Timestamp.Hour())
}

func TestJSONParserUnixtimeMilliSeconds(t *testing.T) {
	psr := parser.JSON{
		UnixtimeMilliField: rlogs.String("unix"),
	}
	logs, err := psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(`{"color":"blue","ts":"2019-10-19T04:44:44","unix":1571630400123}`),
		Src: &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, len(logs))
	assert.Equal(t, 21, logs[0].Timestamp.Day())
	assert.Equal(t, 4, logs[0].Timestamp.Hour())
}

func TestJSONParserUnixtimeString(t *testing.T) {
	psr := parser.JSON{
		UnixtimeStringField: rlogs.String("unix"),
	}
	logs, err := psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(`{"color":"blue","ts":"2019-10-19T04:44:44","unix":"1571630400"}`),
		Src: &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"},
	})

	require.NoError(t, err)
	assert.Equal(t, 1, len(logs))
	assert.Equal(t, 21, logs[0].Timestamp.Day())
	assert.Equal(t, 4, logs[0].Timestamp.Hour())
}

func TestJSONParserNoTimestampField(t *testing.T) {
	psr := parser.JSON{}
	_, err := psr.Parse(&rlogs.MessageQueue{
		Raw: []byte(`{"color":"blue","ts":"2019-10-19T04:44:44"}`),
		Src: &rlogs.AwsS3LogSource{Region: "test-r", Bucket: "test-b", Key: "test-k"},
	})

	assert.Error(t, err)
	assert.Contains(t, err.Error(), "No timestamp field arguments")
}
