package rlogs

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

// Loader downloads object from cloud object storage and create MessageQueue(s)
type Loader interface {
	Load(src LogSource) chan *MessageQueue
}

const (
	defaultS3LineLoaderScanBufferSize  = 1 * 1024 * 1024   // 1 MB
	defaultS3LineLoaderScanBufferLimit = 128 * 1024 * 1024 // 128 MB
)

// S3LineLoader is for line delimitered log file on AWS S3
type S3LineLoader struct {
	ScanBufferSize  int
	ScanBufferLimit int
}

// Load of S3LineLoader reads a log object line by line
func (x *S3LineLoader) Load(src LogSource) chan *MessageQueue {
	chMsg := make(chan *MessageQueue)

	go func() {
		defer close(chMsg)

		s3src, ok := src.(*AwsS3LogSource)
		if !ok {
			chMsg <- &MessageQueue{Error: fmt.Errorf("S3LineLoader accepts only AwsS3LogSource: %v", src)}
		}

		s3client := newS3Client(s3src.Region)
		resp, err := s3client.GetObject(&s3.GetObjectInput{
			Bucket: aws.String(s3src.Bucket),
			Key:    aws.String(s3src.Key),
		})

		if err != nil {
			chMsg <- &MessageQueue{Error: errors.Wrap(err, "Fail to get object")}
			return
		}

		var r io.ReadCloser
		if resp.ContentType == nil {
			r = resp.Body
		} else if *resp.ContentType == "application/x-gzip" ||
			(*resp.ContentType == "binary/octet-stream" &&
				strings.HasSuffix(s3src.Key, ".gz")) {
			gr, err := gzip.NewReader(resp.Body)
			if err != nil {
				chMsg <- &MessageQueue{Error: errors.Wrap(err, "Fail to create a new gzip reader")}
				return
			}
			r = gr
		} else {
			r = resp.Body
		}
		defer r.Close()

		scanner := bufio.NewScanner(r)

		var bufSize int = defaultS3LineLoaderScanBufferSize
		if x.ScanBufferSize > 0 {
			bufSize = x.ScanBufferSize
		}
		var bufLimit int = defaultS3LineLoaderScanBufferLimit
		if x.ScanBufferLimit > 0 {
			bufLimit = x.ScanBufferLimit
		}
		scanner.Buffer(make([]byte, bufSize), bufLimit)

		seq := 0
		for scanner.Scan() {
			line := scanner.Bytes()
			data := make([]byte, len(line))
			copy(data, line)

			chMsg <- &MessageQueue{
				Raw: data,
				Seq: seq,
				Src: src,
			}

			seq++
		}
	}()

	return chMsg
}
