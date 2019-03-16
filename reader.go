package rlogs

import "errors"

type LogParser interface {
	Parse(msg []byte) ([]LogRecord, error)
}

type LogLoader interface {
	Load(region, s3bucket, s3key string) chan *msgQueue
}

type Reader struct {
	Handlers []Handler
}

func NewReader() *Reader {
	return &Reader{}
}

func (x *Reader) AddHandler(s3bucket, s3prefix string, loader LogLoader, parser LogParser) {
	h := Handler{
		S3Bucket: s3bucket,
		S3Prefix: s3prefix,
		Parser:   parser,
		Loader:   loader,
	}
	x.Handlers = append(x.Handlers, h)
}

func (x *Reader) Load(region, s3bucket, s3key string) chan *LogQueue {
	chLog := make(chan *LogQueue)

	go func() {
		defer close(chLog)

		for _, handler := range x.Handlers {
			if handler.match(s3bucket, s3key) {
				handler.bind(chLog, region, s3bucket, s3key)
				return
			}
		}

		chLog <- &LogQueue{Error: errors.New("No mathced entry")}
	}()

	return chLog
}
