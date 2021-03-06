package parser

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/m-mizutani/rlogs"
	"github.com/pkg/errors"
)

// JSON is basic json log parser.
type JSON struct {
	Tag                 string
	UnixtimeField       *string
	UnixtimeStringField *string
	UnixtimeMilliField  *string
	TimestampField      *string
	TimestampFormat     *string
}

// Parse of JSON parses a json formatted log message.
// The parser assumes that json of log message has string map structure.
// Then, array based json format (e.g. AWS CloudTrail) can not be accepted.
func (x *JSON) Parse(msg *rlogs.MessageQueue) ([]*rlogs.LogRecord, error) {
	var value map[string]interface{}
	if err := json.Unmarshal(msg.Raw, &value); err != nil {
		return nil, errors.Wrapf(err, "Fail to unmarshal log message: %v", msg)
	}

	var t time.Time
	switch {
	case x.UnixtimeField != nil:
		if ts, ok := value[*x.UnixtimeField].(float64); ok {
			t = time.Unix(int64(ts), 0).UTC()
		} else {
			return nil, fmt.Errorf("No unixtime field (%s): %v", *x.UnixtimeField, value)
		}

	case x.UnixtimeMilliField != nil:
		if ts, ok := value[*x.UnixtimeMilliField].(float64); ok {
			t = time.Unix(int64(ts)/1000, (int64(ts)%1000)*1000).UTC()
		} else {
			return nil, fmt.Errorf("No unixtime milliseconds field (%s): %v", *x.UnixtimeMilliField, value)
		}

	case x.UnixtimeStringField != nil:
		if str, ok := value[*x.UnixtimeStringField].(string); ok {
			ts, err := strconv.ParseInt(str, 10, 64)
			if err != nil {
				return nil, errors.Wrapf(err, "Fail to parse UnixTimeString: %v", str)
			}
			t = time.Unix(int64(ts), 0).UTC()
		} else {
			return nil, fmt.Errorf("No unixtime milliseconds field (%s): %v", *x.UnixtimeStringField, value)
		}

	case x.TimestampField != nil:
		if x.TimestampFormat == nil {
			return nil, fmt.Errorf("TimestampFormat is required, but not set")
		}

		if ts, ok := value[*x.TimestampField].(string); ok {
			if p, err := time.Parse(*x.TimestampFormat, ts); err == nil {
				t = p.UTC()
			} else {
				return nil, errors.Wrapf(err, "Fail to parse timestamp field by format '%s': %v", *x.TimestampFormat, value)
			}
		}

	default:
		return nil, fmt.Errorf("No timestamp field arguments. One of UnixtimeField, UnixtimeMilliField and TimestampField is required")
	}

	return []*rlogs.LogRecord{
		{
			Tag:       x.Tag,
			Timestamp: t,
			Raw:       msg.Raw,
			Values:    value,
			Seq:       msg.Seq,
			Src:       msg.Src,
		},
	}, nil
}
