package rlogs

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

type s3Client interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

var newS3Client = newAwsS3Client

func newAwsS3Client(region string) s3Client {
	ssn := session.New(&aws.Config{Region: aws.String(region)})
	return s3.New(ssn)
}
