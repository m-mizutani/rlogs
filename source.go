package rlogs

import "strings"

// LogSource indicates location of log object data. Only AWS S3 is supported for now.
type LogSource interface {
	Contains(src LogSource) bool
}

// AwsS3LogSource indicates location of AWS S3 object
type AwsS3LogSource struct {
	Region string // required
	Bucket string // required
	Key    string // required
}

// Contains checks if src is included in own AwsS3LogSource
func (x *AwsS3LogSource) Contains(src LogSource) bool {
	s3, ok := src.(*AwsS3LogSource)
	if !ok {
		return false
	}

	return (s3.Region == x.Region && s3.Bucket == x.Bucket &&
		strings.HasPrefix(s3.Key, x.Key))
}
