package pgmq

import (
	"encoding/json"
	"time"
)

// Message represents a message read from a PGMQ queue.
type Message struct {
	MsgID      int64
	ReadCount  int64
	EnqueuedAt time.Time
	LastReadAt *time.Time // Available in PGMQ v1.10.0+
	VT         time.Time
	Message    json.RawMessage
	Headers    json.RawMessage
}

// QueueInfo represents metadata about a PGMQ queue, returned by ListQueues.
type QueueInfo struct {
	QueueName     string
	IsPartitioned bool
	IsUnlogged    bool
	CreatedAt     time.Time
}

// Metrics represents statistics for a single PGMQ queue.
type Metrics struct {
	QueueName          string
	QueueLength        int64
	NewestMsgAgeSec    *int64
	OldestMsgAgeSec    *int64
	TotalMessages      int64
	ScrapeTime         time.Time
	QueueVisibleLength int64
}

// sendOpts holds optional parameters for Send and SendBatch operations.
type sendOpts struct {
	delay          *int
	delayTimestamp *time.Time
	headers        json.RawMessage
	batchHeaders   []json.RawMessage
}

// SendOption configures optional parameters for Send and SendBatch.
type SendOption func(*sendOpts)

// WithDelay sets the message delay in seconds.
func WithDelay(seconds int) SendOption {
	return func(o *sendOpts) {
		o.delay = &seconds
	}
}

// WithDelayTimestamp sets the message delay as an absolute timestamp.
// The message will become visible at the specified time.
func WithDelayTimestamp(t time.Time) SendOption {
	return func(o *sendOpts) {
		o.delayTimestamp = &t
	}
}

// WithHeaders sets JSONB headers (metadata) on the message.
// Used with Send for a single message.
func WithHeaders(h json.RawMessage) SendOption {
	return func(o *sendOpts) {
		o.headers = h
	}
}

// WithBatchHeaders sets per-message JSONB headers for a batch send.
// The length of the headers slice should match the number of messages.
// Used with SendBatch.
func WithBatchHeaders(headers []json.RawMessage) SendOption {
	return func(o *sendOpts) {
		o.batchHeaders = headers
	}
}

// readOpts holds optional parameters for Read operations.
type readOpts struct {
	conditional json.RawMessage
}

// ReadOption configures optional parameters for Read and ReadBatch.
type ReadOption func(*readOpts)

// WithConditional sets an experimental JSONB filter for conditional reads.
func WithConditional(filter json.RawMessage) ReadOption {
	return func(o *readOpts) {
		o.conditional = filter
	}
}

// pollOpts holds optional parameters for ReadWithPoll.
type pollOpts struct {
	maxPollSeconds int
	pollIntervalMs int
	conditional    json.RawMessage
}

// PollOption configures optional parameters for ReadWithPoll.
type PollOption func(*pollOpts)

// WithMaxPollSeconds sets the maximum number of seconds to poll. Default is 5.
func WithMaxPollSeconds(seconds int) PollOption {
	return func(o *pollOpts) {
		o.maxPollSeconds = seconds
	}
}

// WithPollIntervalMs sets the polling interval in milliseconds. Default is 100.
func WithPollIntervalMs(ms int) PollOption {
	return func(o *pollOpts) {
		o.pollIntervalMs = ms
	}
}

// WithPollConditional sets an experimental JSONB filter for conditional poll reads.
func WithPollConditional(filter json.RawMessage) PollOption {
	return func(o *pollOpts) {
		o.conditional = filter
	}
}
