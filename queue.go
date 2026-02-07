package pgmq

import (
	"context"
	"fmt"
)

// CreateQueue creates a new standard queue with the given name.
// This sets up the queue's tables, indexes, and metadata.
func (c *Client) CreateQueue(ctx context.Context, queue string) error {
	_, err := c.db.Exec(ctx, "SELECT pgmq.create($1)", queue)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// CreateUnloggedQueue creates a new unlogged queue. Unlogged queues use
// unlogged tables which do not write to WAL, providing better performance
// at the cost of durability (data is lost on crash).
func (c *Client) CreateUnloggedQueue(ctx context.Context, queue string) error {
	_, err := c.db.Exec(ctx, "SELECT pgmq.create_unlogged($1)", queue)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// CreatePartitionedQueue creates a new partitioned queue. Requires the
// pg_partman extension. partitionInterval controls the range of each
// partition (e.g. "10000" for ID-based), and retentionInterval controls
// how long partitions are kept (e.g. "100000").
func (c *Client) CreatePartitionedQueue(ctx context.Context, queue string, partitionInterval string, retentionInterval string) error {
	_, err := c.db.Exec(ctx, "SELECT pgmq.create_partitioned($1, $2, $3)", queue, partitionInterval, retentionInterval)
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// DropQueue deletes the given queue, including its tables, indexes, and
// metadata. Returns ErrQueueNotFound if the queue does not exist.
func (c *Client) DropQueue(ctx context.Context, queue string) error {
	var exists bool
	err := c.db.QueryRow(ctx, "SELECT pgmq.drop_queue($1)", queue).Scan(&exists)
	if err != nil {
		return wrapPostgresError(err)
	}

	if !exists {
		return fmt.Errorf("%w: %s", ErrQueueNotFound, queue)
	}

	return nil
}

// ListQueues returns metadata about all existing PGMQ queues.
func (c *Client) ListQueues(ctx context.Context) ([]QueueInfo, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.list_queues()")
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var queues []QueueInfo
	for rows.Next() {
		var q QueueInfo
		if err := rows.Scan(&q.QueueName, &q.IsPartitioned, &q.IsUnlogged, &q.CreatedAt); err != nil {
			return nil, wrapPostgresError(err)
		}
		queues = append(queues, q)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return queues, nil
}

// Purge removes all messages from the specified queue. Returns the number
// of messages that were purged.
func (c *Client) Purge(ctx context.Context, queue string) (int64, error) {
	var count int64
	err := c.db.QueryRow(ctx, "SELECT pgmq.purge_queue($1)", queue).Scan(&count)
	if err != nil {
		return 0, wrapPostgresError(err)
	}

	return count, nil
}
