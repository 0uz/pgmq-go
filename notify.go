package pgmq

import "context"

// EnableNotifyInsert enables PostgreSQL LISTEN/NOTIFY on message inserts
// for the specified queue. When enabled, a NOTIFY event is fired when new
// messages are inserted, allowing consumers to react immediately instead of
// polling.
//
// The throttleIntervalMs parameter controls the minimum interval between
// notifications in milliseconds. Use 0 to disable throttling.
func (c *Client) EnableNotifyInsert(ctx context.Context, queue string, throttleIntervalMs int) error {
	if throttleIntervalMs < 0 {
		return ErrInvalidOption
	}

	_, err := c.db.Exec(ctx, "SELECT pgmq.enable_notify_insert($1, $2)", queue, throttleIntervalMs)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// DisableNotifyInsert disables LISTEN/NOTIFY on message inserts for the
// specified queue.
func (c *Client) DisableNotifyInsert(ctx context.Context, queue string) error {
	_, err := c.db.Exec(ctx, "SELECT pgmq.disable_notify_insert($1)", queue)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// UpdateNotifyInsert changes the throttle interval for an enabled queue.
func (c *Client) UpdateNotifyInsert(ctx context.Context, queue string, throttleIntervalMs int) error {
	if throttleIntervalMs < 0 {
		return ErrInvalidOption
	}

	_, err := c.db.Exec(ctx, "SELECT pgmq.update_notify_insert($1, $2)", queue, throttleIntervalMs)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// ListNotifyInsertThrottles returns notification throttle settings for enabled queues.
func (c *Client) ListNotifyInsertThrottles(ctx context.Context) ([]NotifyInsertThrottle, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.list_notify_insert_throttles()")
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var throttles []NotifyInsertThrottle
	for rows.Next() {
		var throttle NotifyInsertThrottle
		if err := rows.Scan(&throttle.QueueName, &throttle.ThrottleIntervalMs, &throttle.LastNotifiedAt); err != nil {
			return nil, wrapPostgresError(err)
		}
		throttles = append(throttles, throttle)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return throttles, nil
}
