package rlogs

import "github.com/aws/aws-sdk-go/service/s3"

// InjectNewS3Client replaces mock s3Client for testing. Use the function in only test case.
func InjectNewS3Client(c s3Client) {
	newS3Client = func(s string) s3Client { return c }
}

// FixNewS3Client fixes s3Client constructor with original one. Use the function in only test case.
func FixNewS3Client() { newS3Client = newAwsS3Client }

// TestS3ClientBase is base s3 client interface structure. The structure do nothing.
type TestS3ClientBase struct{}

// GetObject is dummy function. It should be overwritten if required in test.
func (x *TestS3ClientBase) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return nil, nil
}
