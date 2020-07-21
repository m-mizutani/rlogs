package parser

import (
	"encoding/json"
	"time"

	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
)

type cloudTrailEventWrapper struct {
	Records []interface{} `json:"Records"`
}

type CloudTrailRecord map[string]interface{}

// CloudTrail is parser of AWS CloudTrail logs.
type CloudTrail struct{}

// Parse converts CloudTrail logs that are put to S3 from CloudTrail directly.
func (x *CloudTrail) Parse(msg *rlogs.MessageQueue) ([]*rlogs.LogRecord, error) {
	var logs []*rlogs.LogRecord

	var event cloudTrailEventWrapper
	if err := json.Unmarshal(msg.Raw, &event); err != nil {
		return nil, errors.Wrap(err, "Fail to parse CloudTrail logs")
	}

	for idx, v := range event.Records {
		logmsg, err := json.Marshal(v)
		if err != nil {
			return nil, errors.Wrapf(err, "Fail to marshal CloudTrail log [%d]", idx)
		}

		var record CloudTrailRecord
		if err := json.Unmarshal(logmsg, &record); err != nil {
			return nil, errors.Wrapf(err, "Fail to re-unmarshal CloudTrail log: %s", string(logmsg))
		}

		// 2018-12-18T00:07:21Z
		ts, err := time.Parse("2006-01-02T15:04:05Z", record["eventTime"].(string))
		if err != nil {
			return nil, errors.Wrapf(err, "Fail to parse timestamp of CloudTrail: %v", record["eventTime"].(string))
		}

		log := rlogs.LogRecord{
			Seq:       idx,
			Values:    record,
			Raw:       logmsg,
			Timestamp: ts,
			Tag:       "aws.cloudtrail",
		}

		logs = append(logs, &log)
	}

	return logs, nil
}
