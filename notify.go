package pgmq

import "context"

// EnableNotifyInsert enables PostgreSQL LISTEN/NOTIFY on message inserts
// for the specified queue. When enabled, a NOTIFY event is fired when new
// messages are inserted, allowing consumers to react immediately instead of
// polling.
//
// The throttleIntervalMs parameter controls the minimum interval between
// notifications in milliseconds. Use 0 for the default of 250ms.
func (c *Client) EnableNotifyInsert(ctx context.Context, queue string, throttleIntervalMs int) error {
	if throttleIntervalMs <= 0 {
		throttleIntervalMs = 250
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
