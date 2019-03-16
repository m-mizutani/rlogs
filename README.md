# rlogs: A framework to load remote log files in Go

`rlogs` is a framework to download and parse a log file on remote object storage (currently only AWS S3 is supported). It's good architecture to send log files to high availablity and scalable object storage such as AWS S3. In general, object storage does not care schema of log and the logs can be put easily. However a user and system to leverage stored logs in object storage need to parse the logs before leveraging. Then the schema of the logs should be managed and this framework support the task.

## Usage

The following example shows to parse OpenSSH logs in S3 and check user name of SSH access. Parser module (`yourpkg`) and main module (`main`) are splitted to re-use parser module.

```go
package yourpkg

import (
    "regexp"
    "github.com/m-mizutani/rlogs"
)

type myParser struct {
	regex *regexp.Regexp
}
type MyLog struct {
	IPAddr   string
	UserName string
	Port     string
}

func newMyParser() *myParser {
	return &myParser{
		regex: regexp.MustCompile(`Invalid user (\S+) from (\S+) port (\d+)`),
	}
}

func (x *myParser) Parse(msg []byte) ([]rlogs.LogRecord, error) {
	line := string(msg)

	resp := x.regex.FindStringSubmatch(line)
	if len(resp) == 0 {
		return nil, nil
	}

	log := MyLog{
		UserName: resp[1],
		IPAddr:   resp[2],
		Port:     resp[3],
	}

	return []rlogs.LogRecord{{
		Tag:       "my.log",
		Timestamp: time.Now().UTC(),
		Entity:    &log,
	}}, nil
}

func NewReader() *rlogs.Reader {
	reader := rlogs.NewReader()
	reader.DefineHandler("your-bucket", "", &rlogs.S3GzipLines{}, &myParser{})
}
```

```go
package main

import (
    "github.com/someone/yourpkg"
)

func main() {
    reader := yourpkg.NewReader()

	for q := range reader.Load("ap-northeast-1", "your-bucket", "test.log") {
		if log, ok := q.Record.Entity.(*MyLog); ok {
			if log.UserName == "root" {
				fmt.Println("found SSH root access challenge")
			}
		}
	}
}
```

## License

- Author: Masayoshi Mizutani < mizutani@sfc.wide.ad.jp >
- License: The 3-Clause BSD License
