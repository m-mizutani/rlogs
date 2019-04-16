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

func (x *Reader) DefineHandler(s3bucket, s3prefix string, loader LogLoader, parser LogParser) {
	h := S3PrefixHandler{
		S3Bucket: s3bucket,
		S3Prefix: s3prefix,
		S3Parser: parser,
		S3Loader: loader,
	}
	x.Handlers = append(x.Handlers, &h)
}

func bind(chLog chan *LogQueue, handler Handler, region, s3bucket, s3key string) {
	chMsg := handler.Loader().Load(region, s3bucket, s3key)

	for q := range chMsg {
		if q == nil { // closed
			return
		}

		if q.err != nil {
			chLog <- &LogQueue{Error: q.err}
			return
		}

		logs, err := handler.Parser().Parse(q.message)
		if err != nil {
			chLog <- &LogQueue{Error: err}
			return
		}

		rawMsg := []byte(q.message)
		for idx := range logs {
			logs[idx].Raw = rawMsg
			if logs[idx].Encodable == nil {
				logs[idx].Encodable = logs[idx].Entity
			}

			chLog <- &LogQueue{Record: &logs[idx]}
		}
	}
}

func (x *Reader) Load(region, s3bucket, s3key string) chan *LogQueue {
	chLog := make(chan *LogQueue)

	go func() {
		defer close(chLog)

		for _, handler := range x.Handlers {
			if handler.Match(s3bucket, s3key) {
				bind(chLog, handler, region, s3bucket, s3key)
				return
			}
		}

		chLog <- &LogQueue{Error: errors.New("No mathced entry")}
	}()

	return chLog
}

// Read loads and provides log data for one shot.
func Read(region, s3bucket, s3key string, loader LogLoader, parser LogParser) chan *LogQueue {
	chLog := make(chan *LogQueue)

	go func() {
		defer close(chLog)

		handler := S3PrefixHandler{
			S3Bucket: s3bucket,
			S3Prefix: s3key,
			S3Parser: parser,
			S3Loader: loader,
		}

		bind(chLog, &handler, region, s3bucket, s3key)
		return
	}()

	return chLog
}
