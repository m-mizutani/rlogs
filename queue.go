package rlogs

type msgQueue struct {
	message []byte
	err     error
}

type LogQueue struct {
	Record *LogRecord
	Error  error
}
