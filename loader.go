package s3logs

import (
	"bufio"

	"compress/gzip"
	"io"
	"io/ioutil"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/pkg/errors"
)

type S3Lines struct{}

func getS3Response(region, s3bucket, s3key string) (*s3.GetObjectOutput, error) {
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

	return resp, nil
}

func (x *S3Lines) Load(region, s3bucket, s3key string) chan *msgQueue {
	chMsg := make(chan *msgQueue)

	go func() {
		defer close(chMsg)

		resp, err := getS3Response(region, s3bucket, s3key)
		if err != nil {
			chMsg <- &msgQueue{err: err}
			return
		}

		var r io.Reader
		if resp.ContentType != nil && *resp.ContentType == "application/x-gzip" {
			gr, err := gzip.NewReader(resp.Body)
			if err != nil {
				chMsg <- &msgQueue{err: errors.Wrap(err, "Fail to decompress gzip")}
				return
			}
			r = gr
		} else {
			r = resp.Body
		}

		scanner := bufio.NewScanner(r)
		var mega int = 1024 * 1024
		scanner.Buffer(make([]byte, mega), 256*mega)

		for scanner.Scan() {
			chMsg <- &msgQueue{message: []byte(scanner.Text())}
		}
	}()

	return chMsg
}

type S3File struct{}

func (x *S3File) Load(region, s3bucket, s3key string) chan *msgQueue {
	chMsg := make(chan *msgQueue)

	go func() {
		defer close(chMsg)

		resp, err := getS3Response(region, s3bucket, s3key)
		if err != nil {
			chMsg <- &msgQueue{err: err}
			return
		}

		var r io.Reader
		if strings.HasSuffix(s3key, ".gz") {
			gr, err := gzip.NewReader(resp.Body)
			if err != nil {
				chMsg <- &msgQueue{err: errors.Wrap(err, "Fail to decompress gzip")}
				return
			}
			r = gr
		} else {
			r = resp.Body
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
