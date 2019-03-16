package rlogs

import "strings"

type Handler struct {
	S3Bucket string
	S3Prefix string
	Loader   LogLoader
	Parser   LogParser
}

func (x Handler) match(s3bucket, s3key string) bool {
	return (x.S3Bucket == s3bucket && strings.HasPrefix(s3key, x.S3Prefix))
}

func (x *Handler) bind(chLog chan *LogQueue, region, s3bucket, s3key string) {
	chMsg := x.Loader.Load(region, s3bucket, s3key)

	for q := range chMsg {
		if q == nil { // closed
			return
		}

		if q.err != nil {
			chLog <- &LogQueue{Error: q.err}
			return
		}

		logs, err := x.Parser.Parse(q.message)
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
