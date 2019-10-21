package rlogs_test

import (
	"testing"

	"github.com/m-mizutani/rlogs"
	"github.com/stretchr/testify/assert"
)

type testDummySource struct{}

func (x *testDummySource) Contains(src rlogs.LogSource) bool { return true }

func TestAwsS3LogSource(t *testing.T) {
	src := rlogs.AwsS3LogSource{
		Region: "ap-northeast-1",
		Bucket: "test-bucket",
		Key:    "logs/dir",
	}

	assert.True(t, src.Contains(&rlogs.AwsS3LogSource{
		Region: "ap-northeast-1",
		Bucket: "test-bucket",
		Key:    "logs/dir/k1.json",
	}))

	assert.False(t, src.Contains(&rlogs.AwsS3LogSource{
		Region: "us-east-1", // other region
		Bucket: "test-bucket",
		Key:    "logs/dir/k1.json",
	}))
	assert.False(t, src.Contains(&rlogs.AwsS3LogSource{
		Region: "ap-northeast-1",
		Bucket: "not-own-bucket", // other bucket
		Key:    "logs/dir/k1.json",
	}))
	assert.False(t, src.Contains(&rlogs.AwsS3LogSource{
		Region: "ap-northeast-1",
		Bucket: "not-own-bucket",
		Key:    "logs/k1.json", // other path
	}))

	// Not AwsS3LogSource
	assert.False(t, src.Contains(&testDummySource{}))
}
