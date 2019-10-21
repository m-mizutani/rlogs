package rlogs_test

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"testing"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/m-mizutani/rlogs"
	"github.com/stretchr/testify/assert"
)

type dummyS3ClientForS3Loader struct{}

func toReadCloser(msg string) io.ReadCloser {
	return ioutil.NopCloser(bytes.NewReader([]byte(msg)))
}

func (x *dummyS3ClientForS3Loader) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	if *input.Bucket != "my-own-bucket" {
		return nil, fmt.Errorf("invalid bucket")
	}

	switch *input.Key {
	case "my/log/data.json":
		return &s3.GetObjectOutput{
			Body: toReadCloser("blue\norange\nred\n"),
		}, nil

	default:
		return nil, fmt.Errorf("Key not found")
	}
}

func TestS3LineLoaderBasic(t *testing.T) {
	dummy := dummyS3ClientForS3Loader{}
	rlogs.InjectNewS3Client(&dummy)
	defer rlogs.FixNewS3Client()

	ldr := rlogs.S3LineLoader{}

	var messages []*rlogs.MessageQueue
	for msg := range ldr.Load(&rlogs.AwsS3LogSource{
		Region: "ap-northeast-1",
		Bucket: "my-own-bucket",
		Key:    "my/log/data.json",
	}) {
		messages = append(messages, msg)
	}

	assert.Equal(t, 3, len(messages))
	assert.NoError(t, messages[0].Error)
	assert.NoError(t, messages[1].Error)
	assert.NoError(t, messages[2].Error)
	assert.Equal(t, "blue", string(messages[0].Raw))
	assert.Equal(t, "orange", string(messages[1].Raw))
	assert.Equal(t, "red", string(messages[2].Raw))
}

func TestS3FileLoaderBasic(t *testing.T) {
	dummy := dummyS3ClientForS3Loader{}
	rlogs.InjectNewS3Client(&dummy)
	defer rlogs.FixNewS3Client()

	ldr := rlogs.S3FileLoader{}

	var messages []*rlogs.MessageQueue
	for msg := range ldr.Load(&rlogs.AwsS3LogSource{
		Region: "ap-northeast-1",
		Bucket: "my-own-bucket",
		Key:    "my/log/data.json",
	}) {
		messages = append(messages, msg)
	}

	assert.Equal(t, 1, len(messages))
	assert.NoError(t, messages[0].Error)
	assert.Equal(t, "blue\norange\nred\n", string(messages[0].Raw))
}
