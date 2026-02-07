package pgmq

import (
	"context"

	"github.com/jackc/pgx/v5"
)

// Read reads a single message from the queue. The message becomes invisible
// for the duration of the visibility timeout (vt) in seconds. If vt is 0,
// the default of 30 seconds is used.
//
// Returns ErrNoRows if no messages are available.
func (c *Client) Read(ctx context.Context, queue string, vt int64, opts ...ReadOption) (*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	o := &readOpts{}
	for _, opt := range opts {
		opt(o)
	}

	query := "SELECT * FROM pgmq.read($1, $2, $3)"
	args := []any{queue, vt, 1}

	if len(o.conditional) > 0 {
		query = "SELECT * FROM pgmq.read($1, $2, $3, $4::jsonb)"
		args = append(args, o.conditional)
	}

	rows, err := c.db.Query(ctx, query, args...)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, ErrNoRows
	}

	return scanMessage(rows)
}

// ReadBatch reads up to numMsgs messages from the queue. Each returned
// message becomes invisible for the duration of the visibility timeout (vt)
// in seconds. If vt is 0, the default of 30 seconds is used.
//
// Returns an empty slice if no messages are available.
func (c *Client) ReadBatch(ctx context.Context, queue string, vt int64, numMsgs int64, opts ...ReadOption) ([]*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	o := &readOpts{}
	for _, opt := range opts {
		opt(o)
	}

	query := "SELECT * FROM pgmq.read($1, $2, $3)"
	args := []any{queue, vt, numMsgs}

	if len(o.conditional) > 0 {
		query = "SELECT * FROM pgmq.read($1, $2, $3, $4::jsonb)"
		args = append(args, o.conditional)
	}

	rows, err := c.db.Query(ctx, query, args...)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	return collectMessages(rows)
}

// ReadWithPoll reads up to numMsgs messages from the queue using long-polling.
// It will poll for messages for up to maxPollSeconds (default 5) with a polling
// interval of pollIntervalMs (default 100ms).
//
// Returns an empty slice if no messages become available within the polling window.
func (c *Client) ReadWithPoll(ctx context.Context, queue string, vt int64, numMsgs int64, opts ...PollOption) ([]*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	o := &pollOpts{
		maxPollSeconds: 5,
		pollIntervalMs: 100,
	}
	for _, opt := range opts {
		opt(o)
	}

	query := "SELECT * FROM pgmq.read_with_poll($1, $2, $3, $4, $5)"
	args := []any{queue, vt, numMsgs, o.maxPollSeconds, o.pollIntervalMs}

	if len(o.conditional) > 0 {
		query = "SELECT * FROM pgmq.read_with_poll($1, $2, $3, $4, $5, $6::jsonb)"
		args = append(args, o.conditional)
	}

	rows, err := c.db.Query(ctx, query, args...)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	return collectMessages(rows)
}

// Pop reads and immediately deletes a single message from the queue.
// Unlike Read, the visibility timeout does not apply because the message
// is deleted immediately.
//
// Returns ErrNoRows if the queue is empty.
func (c *Client) Pop(ctx context.Context, queue string) (*Message, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.pop($1)", queue)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, ErrNoRows
	}

	return scanMessage(rows)
}

// PopBatch reads and immediately deletes up to qty messages from the queue.
// Returns an empty slice if the queue is empty.
func (c *Client) PopBatch(ctx context.Context, queue string, qty int64) ([]*Message, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.pop($1, $2)", queue, qty)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	return collectMessages(rows)
}

// ReadGrouped reads up to qty messages from the queue using FIFO grouped
// ordering (similar to AWS SQS message groups). Messages are grouped by
// the "x-pgmq-group" header value.
func (c *Client) ReadGrouped(ctx context.Context, queue string, vt int64, qty int64) ([]*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.read_grouped($1, $2, $3)", queue, vt, qty)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	return collectMessages(rows)
}

// ReadGroupedWithPoll reads up to qty messages using FIFO grouped ordering
// with long-polling support.
func (c *Client) ReadGroupedWithPoll(ctx context.Context, queue string, vt int64, qty int64, opts ...PollOption) ([]*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	o := &pollOpts{
		maxPollSeconds: 5,
		pollIntervalMs: 100,
	}
	for _, opt := range opts {
		opt(o)
	}

	rows, err := c.db.Query(ctx,
		"SELECT * FROM pgmq.read_grouped_with_poll($1, $2, $3, $4, $5)",
		queue, vt, qty, o.maxPollSeconds, o.pollIntervalMs,
	)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	return collectMessages(rows)
}

// ReadGroupedRoundRobin reads up to qty messages using round-robin grouped ordering.
func (c *Client) ReadGroupedRoundRobin(ctx context.Context, queue string, vt int64, qty int64) ([]*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.read_grouped_rr($1, $2, $3)", queue, vt, qty)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	return collectMessages(rows)
}

// ReadGroupedRoundRobinWithPoll reads up to qty messages using round-robin
// grouped ordering with long-polling support.
func (c *Client) ReadGroupedRoundRobinWithPoll(ctx context.Context, queue string, vt int64, qty int64, opts ...PollOption) ([]*Message, error) {
	if vt == 0 {
		vt = vtDefault
	}

	o := &pollOpts{
		maxPollSeconds: 5,
		pollIntervalMs: 100,
	}
	for _, opt := range opts {
		opt(o)
	}

	rows, err := c.db.Query(ctx,
		"SELECT * FROM pgmq.read_grouped_rr_with_poll($1, $2, $3, $4, $5)",
		queue, vt, qty, o.maxPollSeconds, o.pollIntervalMs,
	)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	return collectMessages(rows)
}

// collectMessages collects all messages from pgx.Rows and returns them.
func collectMessages(rows pgx.Rows) ([]*Message, error) {
	var msgs []*Message
	for rows.Next() {
		msg, err := scanMessage(rows)
		if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return msgs, nil
}
