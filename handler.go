package rlogs

import "strings"

type Handler interface {
	Match(s3bucet, s3key string) bool
	Loader() LogLoader
	Parser() LogParser
}

type S3PrefixHandler struct {
	S3Bucket string
	S3Prefix string
	S3Loader LogLoader
	S3Parser LogParser
}

func (x *S3PrefixHandler) Loader() LogLoader { return x.S3Loader }
func (x *S3PrefixHandler) Parser() LogParser { return x.S3Parser }

func (x S3PrefixHandler) Match(s3bucket, s3key string) bool {
	return (x.S3Bucket == s3bucket && strings.HasPrefix(s3key, x.S3Prefix))
}
