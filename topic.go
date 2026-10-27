package pgmq

import (
	"context"
	"encoding/json"
	"fmt"
)

// BindTopic binds a wildcard topic pattern to a queue.
func (c *Client) BindTopic(ctx context.Context, pattern string, queue string) error {
	_, err := c.db.Exec(ctx, "SELECT pgmq.bind_topic($1, $2)", pattern, queue)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// UnbindTopic removes a topic binding and reports whether a binding was removed.
func (c *Client) UnbindTopic(ctx context.Context, pattern string, queue string) (bool, error) {
	var removed bool
	err := c.db.QueryRow(ctx, "SELECT pgmq.unbind_topic($1, $2)", pattern, queue).Scan(&removed)
	if err != nil {
		return false, wrapPostgresError(err)
	}

	return removed, nil
}

// ListTopicBindings returns all topic bindings.
func (c *Client) ListTopicBindings(ctx context.Context) ([]TopicBinding, error) {
	return c.listTopicBindings(ctx, "SELECT * FROM pgmq.list_topic_bindings()")
}

// ListTopicBindingsForQueue returns topic bindings for one queue.
func (c *Client) ListTopicBindingsForQueue(ctx context.Context, queue string) ([]TopicBinding, error) {
	return c.listTopicBindings(ctx, "SELECT * FROM pgmq.list_topic_bindings($1)", queue)
}

func (c *Client) listTopicBindings(ctx context.Context, query string, args ...any) ([]TopicBinding, error) {
	rows, err := c.db.Query(ctx, query, args...)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var bindings []TopicBinding
	for rows.Next() {
		var binding TopicBinding
		if err := rows.Scan(&binding.Pattern, &binding.QueueName, &binding.BoundAt, &binding.CompiledRegex); err != nil {
			return nil, wrapPostgresError(err)
		}
		bindings = append(bindings, binding)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return bindings, nil
}

// TestRouting returns the topic bindings that match a routing key.
func (c *Client) TestRouting(ctx context.Context, routingKey string) ([]TopicRoute, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.test_routing($1)", routingKey)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var routes []TopicRoute
	for rows.Next() {
		var route TopicRoute
		if err := rows.Scan(&route.Pattern, &route.QueueName, &route.CompiledRegex); err != nil {
			return nil, wrapPostgresError(err)
		}
		routes = append(routes, route)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return routes, nil
}

// SendTopic sends a message to every queue whose binding matches routingKey.
// WithHeaders and WithDelay are supported. PGMQ does not provide a timestamp
// overload for send_topic, so WithDelayTimestamp is rejected.
func (c *Client) SendTopic(ctx context.Context, routingKey string, msg json.RawMessage, opts ...SendOption) (int64, error) {
	o := &sendOpts{}
	for _, opt := range opts {
		opt(o)
	}
	if err := validateTopicSendOpts(o); err != nil {
		return 0, err
	}

	query, args := buildSendTopicQuery(routingKey, msg, o)
	var count int64
	if err := c.db.QueryRow(ctx, query, args...).Scan(&count); err != nil {
		return 0, wrapPostgresError(err)
	}

	return count, nil
}

// SendBatchTopic sends messages to every queue whose binding matches routingKey.
func (c *Client) SendBatchTopic(ctx context.Context, routingKey string, msgs []json.RawMessage, opts ...SendOption) ([]TopicMessage, error) {
	o := &sendOpts{}
	for _, opt := range opts {
		opt(o)
	}
	if err := validateSendOpts(o); err != nil {
		return nil, err
	}
	if len(msgs) == 0 {
		return nil, fmt.Errorf("%w: msgs must contain at least one message", ErrInvalidOption)
	}
	if len(o.batchHeaders) > 0 && len(o.batchHeaders) != len(msgs) {
		return nil, fmt.Errorf("%w: batch headers length must match msgs length", ErrInvalidOption)
	}

	query, args := buildSendBatchTopicQuery(routingKey, msgs, o)
	rows, err := c.db.Query(ctx, query, args...)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var sent []TopicMessage
	for rows.Next() {
		var message TopicMessage
		if err := rows.Scan(&message.QueueName, &message.MsgID); err != nil {
			return nil, wrapPostgresError(err)
		}
		sent = append(sent, message)
	}
	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return sent, nil
}

func validateTopicSendOpts(o *sendOpts) error {
	if err := validateSendOpts(o); err != nil {
		return err
	}
	if o.delayTimestamp != nil {
		return fmt.Errorf("%w: WithDelayTimestamp is not supported by pgmq.send_topic", ErrInvalidOption)
	}
	return nil
}

func buildSendTopicQuery(routingKey string, msg json.RawMessage, o *sendOpts) (string, []any) {
	switch {
	case len(o.headers) > 0 && o.delay != nil:
		return "SELECT pgmq.send_topic($1, $2, $3::jsonb, $4::int)", []any{routingKey, msg, o.headers, *o.delay}
	case len(o.headers) > 0:
		return "SELECT pgmq.send_topic($1, $2, $3::jsonb, $4::int)", []any{routingKey, msg, o.headers, 0}
	case o.delay != nil:
		return "SELECT pgmq.send_topic($1, $2, $3::int)", []any{routingKey, msg, *o.delay}
	default:
		return "SELECT pgmq.send_topic($1, $2)", []any{routingKey, msg}
	}
}

func buildSendBatchTopicQuery(routingKey string, msgs []json.RawMessage, o *sendOpts) (string, []any) {
	switch {
	case len(o.batchHeaders) > 0 && o.delay != nil:
		return "SELECT * FROM pgmq.send_batch_topic($1, $2::jsonb[], $3::jsonb[], $4::int)", []any{routingKey, msgs, o.batchHeaders, *o.delay}
	case len(o.batchHeaders) > 0 && o.delayTimestamp != nil:
		return "SELECT * FROM pgmq.send_batch_topic($1, $2::jsonb[], $3::jsonb[], $4::timestamptz)", []any{routingKey, msgs, o.batchHeaders, *o.delayTimestamp}
	case len(o.batchHeaders) > 0:
		return "SELECT * FROM pgmq.send_batch_topic($1, $2::jsonb[], $3::jsonb[])", []any{routingKey, msgs, o.batchHeaders}
	case o.delay != nil:
		return "SELECT * FROM pgmq.send_batch_topic($1, $2::jsonb[], $3::int)", []any{routingKey, msgs, *o.delay}
	case o.delayTimestamp != nil:
		return "SELECT * FROM pgmq.send_batch_topic($1, $2::jsonb[], $3::timestamptz)", []any{routingKey, msgs, *o.delayTimestamp}
	default:
		return "SELECT * FROM pgmq.send_batch_topic($1, $2::jsonb[])", []any{routingKey, msgs}
	}
}
