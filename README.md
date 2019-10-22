# rlogs: A framework to load remote log files in Go

[![Travis-CI](https://travis-ci.org/m-mizutani/rlogs.svg)](https://travis-ci.org/m-mizutani/rlogs) [![Report card](https://goreportcard.com/badge/github.com/m-mizutani/rlogs)](https://goreportcard.com/report/github.com/m-mizutani/rlogs)

`rlogs` is a framework to download and parse a log file on remote object storage (currently only AWS S3 is supported). It's good architecture to send log files to high availablity and scalable object storage such as AWS S3. In general, object storage does not care schema of log and the logs can be put easily. However a user and system to leverage stored logs in object storage need to parse the logs before leveraging. Then the schema of the logs should be managed and this framework support the task.

## Getting Started

```go
func main() {
	pipeline := rlogs.Pipeline{
		Psr: &parser.JSON{
			Tag:             "ts",
			TimestampField:  rlogs.String("ts"),
			TimestampFormat: rlogs.String("2006-01-02T15:04:05"),
		},
		Ldr: &rlogs.S3LineLoader{},
	}

	reader := rlogs.NewReader([]*rlogs.LogEntry{
		{
			Pipe: pipeline,
			Src: &rlogs.AwsS3LogSource{
				Region: "ap-northeast-1",
				Bucket: "your-bucket",
				Key:    "http/",
			},
		},
	})

	// s3://your-bucket/http/log.json is following:
	// {"ts":"2019-10-10T10:00:00","src":"10.1.2.3","port":34567,"path":"/hello"}
	// {"ts":"2019-10-10T10:00:02","src":"10.2.3.4","port":45678,"path":"/world"}

	ch := reader.Read(&rlogs.AwsS3LogSource{
		Region: "some-region",
		Bucket: "your-bucket",
		Key:    "http/log.json",
	})

	for q := range ch {
		if q.Error != nil {
			log.Fatal(q.Error)
		}
		values := q.Log.Values.(map[string]interface{})
		fmt.Printf("[log] tag=%s time=%s src=%v\n", q.Log.Tag, q.Log.Timestamp, values["src"])
	}
	// Output:
	// [log] tag=ts time=2019-10-10 10:00:00 +0000 UTC src=10.1.2.3
	// [log] tag=ts time=2019-10-10 10:00:02 +0000 UTC src=10.2.3.4
}
```

## Usage

### Reader

`BasicReader` is provided for now. This reader has slice of `rlogs.LogEntry` that has `Parser`, `Loader` and `LogSource`. When calling `Read(*LogSource)` function, the reader checks given `LogSource` with `LogSource` one by one. If an entry hits, download S3 object and parse it with `Loader` and `Parser` in the entry.

### Loader

- `S3LineLoader`: Download AWS S3 object and split the file line by line
- `S3FileLoader`: Download AWS S3 object and pass whole data of the object to Parser directly

### Parser

Following parser is available in this pacakge.

- `JSON`: Generic JSON parser. A field name and time foramt are required as arguments.
- `VpcFlowLogs`: Parse VPC flog log S3 object taht is put by VPCFlowLogs directly. The parser requires `S3LineLoader`
- `CloudTrail`:  Parse CloudTrail S3 object log taht is put by CloudTrail directly. The parser requires `S3FileLoader`

## License

- Author: Masayoshi Mizutani < mizutani@sfc.wide.ad.jp >
- License: The 3-Clause BSD License
