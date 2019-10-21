package rlogs

// Parser converts raw log message to LogRecord(s)
type Parser interface {
	Parse(msg *MessageQueue) ([]*LogRecord, error)
}
