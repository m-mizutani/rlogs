# rlogs: A framework to load remote log files in Go

[![Travis-CI](https://travis-ci.org/m-mizutani/rlogs.svg)](https://travis-ci.org/m-mizutani/rlogs) [![Report card](https://goreportcard.com/badge/github.com/m-mizutani/rlogs)](https://goreportcard.com/report/github.com/m-mizutani/rlogs)

`rlogs` is a framework to download and parse a log file on remote object storage (currently only AWS S3 is supported). It's good architecture to send log files to high availablity and scalable object storage such as AWS S3. In general, object storage does not care schema of log and the logs can be put easily. However a user and system to leverage stored logs in object storage need to parse the logs before leveraging. Then the schema of the logs should be managed and this framework support the task.

## License

- Author: Masayoshi Mizutani < mizutani@sfc.wide.ad.jp >
- License: The 3-Clause BSD License
