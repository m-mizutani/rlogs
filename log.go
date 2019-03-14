package s3logs

import "time"

type LogRecord struct {
	Tag       string
	Timestamp time.Time
	Entity    interface{}
	Encodable interface{}
	Raw       []byte
}
