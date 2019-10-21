package rlogs

// Loader downloads object from cloud object storage and create MessageQueue(s)
type Loader interface {
	Load(src LogSource) chan *MessageQueue
}
