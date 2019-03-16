package s3logs

import (
	"bufio"

	"compress/gzip"
	"io"
	"io/ioutil"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

var BufferInitSize = 1024 * 1024
var BufferMaxSize = 128 * 1024 * 1024

func getS3Reader(region, s3bucket, s3key string) (io.Reader, error) {
	ssn := session.Must(session.NewSession(&aws.Config{
		Region: aws.String(region),
	}))
	srv := s3.New(ssn)
	resp, err := srv.GetObject(&s3.GetObjectInput{
		Bucket: aws.String(s3bucket),
		Key:    aws.String(s3key),
	})

	if err != nil {
		return nil, errors.Wrap(err, "Fail to get object")
	}

	if resp.ContentType != nil && *resp.ContentType == "application/x-gzip" {
		gr, err := gzip.NewReader(resp.Body)
		if err != nil {
			return nil, errors.Wrap(err, "Fail to decompress gzip")
		}
		return gr, nil
	}

	return resp.Body, nil
}

// S3Lines is a loader to fetch S3 object and pass data line by line to parser.
type S3Lines struct{}

func (x *S3Lines) Load(region, s3bucket, s3key string) chan *msgQueue {
	chMsg := make(chan *msgQueue)

	go func() {
		defer close(chMsg)

		r, err := getS3Reader(region, s3bucket, s3key)
		if err != nil {
			chMsg <- &msgQueue{err: err}
			return
		}

		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, BufferInitSize), BufferMaxSize)

		for scanner.Scan() {
			chMsg <- &msgQueue{message: []byte(scanner.Text())}
		}
	}()

	return chMsg
}

// S3File is a loader to fetch S3 object and pass all data directly to parser.
type S3File struct{}

func (x *S3File) Load(region, s3bucket, s3key string) chan *msgQueue {
	chMsg := make(chan *msgQueue)

	go func() {
		defer close(chMsg)

		r, err := getS3Reader(region, s3bucket, s3key)
		if err != nil {
			chMsg <- &msgQueue{err: err}
			return
		}

		data, err := ioutil.ReadAll(r)
		if err != nil {
			chMsg <- &msgQueue{err: errors.Wrap(err, "Fail to read data")}
			return
		}

		chMsg <- &msgQueue{message: data}
	}()

	return chMsg
}

// S3GzipLines is a loader to fetch gzipped S3 object and pass all data directly to parser.
type S3GzipLines struct{}

func (x *S3GzipLines) Load(region, s3bucket, s3key string) chan *msgQueue {
	chMsg := make(chan *msgQueue)

	go func() {
		defer close(chMsg)

		r, err := getS3Reader(region, s3bucket, s3key)
		if err != nil {
			chMsg <- &msgQueue{err: err}
			return
		}

		if _, ok := r.(*gzip.Reader); !ok {
			if r, err = gzip.NewReader(r); err != nil {
				chMsg <- &msgQueue{err: errors.Wrap(err, "Fail to create gzip reader")}
				return
			}
		}

		scanner := bufio.NewScanner(r)
		scanner.Buffer(make([]byte, BufferInitSize), BufferMaxSize)

		for scanner.Scan() {
			chMsg <- &msgQueue{message: []byte(scanner.Text())}
		}
	}()

	return chMsg
}

// S3File is a loader to fetch S3 object and pass all data directly to parser.
type S3GzipFile struct{}

func (x *S3GzipFile) Load(region, s3bucket, s3key string) chan *msgQueue {
	chMsg := make(chan *msgQueue)

	go func() {
		defer close(chMsg)

		r, err := getS3Reader(region, s3bucket, s3key)
		if err != nil {
			chMsg <- &msgQueue{err: err}
			return
		}

		if _, ok := r.(*gzip.Reader); !ok {
			if r, err = gzip.NewReader(r); err != nil {
				chMsg <- &msgQueue{err: errors.Wrap(err, "Fail to create gzip reader")}
				return
			}
		}

		data, err := ioutil.ReadAll(r)
		if err != nil {
			chMsg <- &msgQueue{err: errors.Wrap(err, "Fail to read data")}
			return
		}

		chMsg <- &msgQueue{message: data}
	}()

	return chMsg
}
