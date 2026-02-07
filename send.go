package pgmq

import (
	"context"
	"encoding/json"
	"fmt"
)

// Send sends a single message to the specified queue. Returns the message ID.
//
// Options can be provided to set a delay or headers:
//
//	client.Send(ctx, "my_queue", msg)
//	client.Send(ctx, "my_queue", msg, WithDelay(10))
//	client.Send(ctx, "my_queue", msg, WithHeaders(h))
//	client.Send(ctx, "my_queue", msg, WithHeaders(h), WithDelay(10))
//	client.Send(ctx, "my_queue", msg, WithDelayTimestamp(t))
//
// Note: WithDelayTimestamp cannot be combined with WithHeaders (PGMQ limitation).
func (c *Client) Send(ctx context.Context, queue string, msg json.RawMessage, opts ...SendOption) (int64, error) {
	o := &sendOpts{}
	for _, opt := range opts {
		opt(o)
	}

	if err := validateSendOpts(o); err != nil {
		return 0, err
	}

	query, args := buildSendQuery(queue, msg, o)

	var msgID int64
	err := c.db.QueryRow(ctx, query, args...).Scan(&msgID)
	if err != nil {
		return 0, wrapPostgresError(err)
	}

	return msgID, nil
}

// SendBatch sends multiple messages to the specified queue. Returns the message IDs.
//
// Options can be provided to set a delay or per-message headers:
//
//	client.SendBatch(ctx, "my_queue", msgs)
//	client.SendBatch(ctx, "my_queue", msgs, WithDelay(10))
//	client.SendBatch(ctx, "my_queue", msgs, WithBatchHeaders(headers))
//	client.SendBatch(ctx, "my_queue", msgs, WithBatchHeaders(headers), WithDelay(10))
//	client.SendBatch(ctx, "my_queue", msgs, WithDelayTimestamp(t))
//
// Note: WithDelayTimestamp cannot be combined with WithBatchHeaders (PGMQ limitation).
func (c *Client) SendBatch(ctx context.Context, queue string, msgs []json.RawMessage, opts ...SendOption) ([]int64, error) {
	o := &sendOpts{}
	for _, opt := range opts {
		opt(o)
	}

	if err := validateSendOpts(o); err != nil {
		return nil, err
	}

	query, args := buildSendBatchQuery(queue, msgs, o)

	rows, err := c.db.Query(ctx, query, args...)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var msgIDs []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, wrapPostgresError(err)
		}
		msgIDs = append(msgIDs, id)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return msgIDs, nil
}

// validateSendOpts ensures that conflicting options are not set.
func validateSendOpts(o *sendOpts) error {
	if o.delay != nil && o.delayTimestamp != nil {
		return fmt.Errorf("%w: cannot use both WithDelay and WithDelayTimestamp", ErrInvalidOption)
	}

	// PGMQ does not have an overload for headers + timestamp delay.
	if len(o.headers) > 0 && o.delayTimestamp != nil {
		return fmt.Errorf("%w: cannot use WithHeaders with WithDelayTimestamp (unsupported by PGMQ)", ErrInvalidOption)
	}

	if len(o.batchHeaders) > 0 && o.delayTimestamp != nil {
		return fmt.Errorf("%w: cannot use WithBatchHeaders with WithDelayTimestamp (unsupported by PGMQ)", ErrInvalidOption)
	}

	return nil
}

// buildSendQuery builds the SQL and args for a single Send call.
func buildSendQuery(queue string, msg json.RawMessage, o *sendOpts) (string, []any) {
	switch {
	case len(o.headers) > 0 && o.delay != nil:
		return "SELECT * FROM pgmq.send($1, $2, $3::jsonb, $4::int)", []any{queue, msg, o.headers, *o.delay}
	case len(o.headers) > 0:
		return "SELECT * FROM pgmq.send($1, $2, $3::jsonb)", []any{queue, msg, o.headers}
	case o.delayTimestamp != nil:
		return "SELECT * FROM pgmq.send($1, $2, $3::timestamptz)", []any{queue, msg, *o.delayTimestamp}
	case o.delay != nil:
		return "SELECT * FROM pgmq.send($1, $2, $3::int)", []any{queue, msg, *o.delay}
	default:
		return "SELECT * FROM pgmq.send($1, $2, $3::int)", []any{queue, msg, 0}
	}
}

// buildSendBatchQuery builds the SQL and args for a SendBatch call.
func buildSendBatchQuery(queue string, msgs []json.RawMessage, o *sendOpts) (string, []any) {
	switch {
	case len(o.batchHeaders) > 0 && o.delay != nil:
		return "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::jsonb[], $4::int)", []any{queue, msgs, o.batchHeaders, *o.delay}
	case len(o.batchHeaders) > 0:
		return "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::jsonb[])", []any{queue, msgs, o.batchHeaders}
	case o.delayTimestamp != nil:
		return "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::timestamptz)", []any{queue, msgs, *o.delayTimestamp}
	case o.delay != nil:
		return "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::int)", []any{queue, msgs, *o.delay}
	default:
		return "SELECT * FROM pgmq.send_batch($1, $2::jsonb[], $3::int)", []any{queue, msgs, 0}
	}
}
