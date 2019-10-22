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

// CloudTrailRecord is main log record of CloudTrail.
type CloudTrailRecord struct {
	APIVersion         string                 `json:"apiVersion"`
	AwsRegion          string                 `json:"awsRegion"`
	ErrorCode          string                 `json:"errorCode"`
	ErrorMessage       string                 `json:"errorMessage"`
	EventID            string                 `json:"eventID"`
	EventName          string                 `json:"eventName"`
	EventSource        string                 `json:"eventSource"`
	EventTime          string                 `json:"eventTime"`
	EventType          string                 `json:"eventType"`
	EventVersion       string                 `json:"eventVersion"`
	ReadOnly           bool                   `json:"readOnly"`
	RecipientAccountID string                 `json:"recipientAccountId"`
	RequestID          string                 `json:"requestID"`
	RequestParameters  map[string]interface{} `json:"requestParameters"`
	Resources          []CloudTrailResource   `json:"resources"`
	ResponseElements   map[string]interface{} `json:"responseElements"`
	SharedEventID      string                 `json:"sharedEventID"`
	SourceIPAddress    string                 `json:"sourceIPAddress"`
	UserAgent          string                 `json:"userAgent"`
	UserIdentity       CloudTrailUserIdentity `json:"userIdentity"`
	VpcEndpointID      string                 `json:"vpcEndpointId"`
}

// CloudTrailUserIdentity indicates identity of event subject.
type CloudTrailUserIdentity struct {
	AccessKeyID    string                   `json:"accessKeyId"`
	AccountID      string                   `json:"accountId"`
	Arn            string                   `json:"arn"`
	InvokedBy      string                   `json:"invokedBy"`
	PrincipalID    string                   `json:"principalId"`
	SessionContext CloudTrailSessionContext `json:"sessionContext"`
	Type           string                   `json:"type"`
	UserName       string                   `json:"userName"`
}

// CloudTrailSessionContext indicates AWS session information
type CloudTrailSessionContext struct {
	Attributes struct {
		CreationDate     string `json:"creationDate"`
		MfaAuthenticated string `json:"mfaAuthenticated"`
	} `json:"attributes"`
	SessionIssuer struct {
		AccountID   string `json:"accountId"`
		Arn         string `json:"arn"`
		PrincipalID string `json:"principalId"`
		Type        string `json:"type"`
		UserName    string `json:"userName"`
	} `json:"sessionIssuer"`
}

// CloudTrailResource indicates target resource(s)
type CloudTrailResource struct {
	Arn       string `json:"ARN"`
	AccountID string `json:"accountId"`
	Type      string `json:"type"`
}

// CloudTrail is parser of AWS CloudTrail logs.
type CloudTrail struct{}

// NewCloudTrailPipeline provides set of Parser and Loader for CloudTrail logs
func NewCloudTrailPipeline() rlogs.Pipeline {
	return rlogs.Pipeline{
		Psr: &CloudTrail{},
		Ldr: &rlogs.S3FileLoader{},
	}
}

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
		ts, err := time.Parse("2006-01-02T15:04:05Z", record.EventTime)
		if err != nil {
			return nil, errors.Wrapf(err, "Fail to parse timestamp of CloudTrail: %v", record.EventTime)
		}

		log := rlogs.LogRecord{
			Seq:       idx,
			Values:    &record,
			Raw:       logmsg,
			Timestamp: ts,
			Tag:       "aws.cloudtrail",
		}

		logs = append(logs, &log)
	}

	return logs, nil
}
