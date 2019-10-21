package rlogs

// LogSource indicates location of log object data. Only AWS S3 is supported for now.
type LogSource interface {
	Contains(src LogSource)
}

// AwsS3LogSource indicates location of AWS S3 object
type AwsS3LogSource struct {
	Region string
	Bucket string
	Key    string
}
