package pgmq

import (
	"context"
	"time"
)

// Delete permanently deletes a single message from the queue by its ID.
// Returns true if the message was found and deleted, false otherwise.
// Use Archive instead if you want to retain the message for auditing.
func (c *Client) Delete(ctx context.Context, queue string, msgID int64) (bool, error) {
	var deleted bool
	err := c.db.QueryRow(ctx, "SELECT pgmq.delete($1, $2::bigint)", queue, msgID).Scan(&deleted)
	if err != nil {
		return false, wrapPostgresError(err)
	}

	return deleted, nil
}

// DeleteBatch permanently deletes multiple messages from the queue by their IDs.
// Returns the IDs of messages that were actually deleted.
func (c *Client) DeleteBatch(ctx context.Context, queue string, msgIDs []int64) ([]int64, error) {
	rows, err := c.db.Query(ctx, "SELECT pgmq.delete($1, $2::bigint[])", queue, msgIDs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var deleted []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, wrapPostgresError(err)
		}
		deleted = append(deleted, id)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return deleted, nil
}

// Archive moves a single message from the queue to the archive table.
// Returns true if the message was found and archived, false otherwise.
// Archived messages can be viewed with: SELECT * FROM pgmq.a_<queue_name>
func (c *Client) Archive(ctx context.Context, queue string, msgID int64) (bool, error) {
	var archived bool
	err := c.db.QueryRow(ctx, "SELECT pgmq.archive($1, $2::bigint)", queue, msgID).Scan(&archived)
	if err != nil {
		return false, wrapPostgresError(err)
	}

	return archived, nil
}

// ArchiveBatch moves multiple messages from the queue to the archive table.
// Returns the IDs of messages that were actually archived.
func (c *Client) ArchiveBatch(ctx context.Context, queue string, msgIDs []int64) ([]int64, error) {
	rows, err := c.db.Query(ctx, "SELECT pgmq.archive($1, $2::bigint[])", queue, msgIDs)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var archived []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, wrapPostgresError(err)
		}
		archived = append(archived, id)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return archived, nil
}

// SetVT sets the visibility timeout of a single message to vt seconds from now.
// Returns the updated message record.
// Returns ErrNoRows if the message is not found.
func (c *Client) SetVT(ctx context.Context, queue string, msgID int64, vt int64) (*Message, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.set_vt($1, $2::bigint, $3::int)", queue, msgID, vt)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, ErrNoRows
	}

	return scanMessage(rows)
}

// SetVTTimestamp sets the visibility timeout of a single message to a specific
// timestamp. Returns the updated message record.
// Returns ErrNoRows if the message is not found.
func (c *Client) SetVTTimestamp(ctx context.Context, queue string, msgID int64, vt time.Time) (*Message, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.set_vt($1, $2::bigint, $3::timestamptz)", queue, msgID, vt)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, ErrNoRows
	}

	return scanMessage(rows)
}

// SetVTBatch sets the visibility timeout of multiple messages to vt seconds
// from now. Returns the updated message records.
func (c *Client) SetVTBatch(ctx context.Context, queue string, msgIDs []int64, vt int64) ([]*Message, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.set_vt($1, $2::bigint[], $3::int)", queue, msgIDs, vt)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	return collectMessages(rows)
}

// SetVTBatchTimestamp sets the visibility timeout of multiple messages to a
// specific timestamp. Returns the updated message records.
func (c *Client) SetVTBatchTimestamp(ctx context.Context, queue string, msgIDs []int64, vt time.Time) ([]*Message, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.set_vt($1, $2::bigint[], $3::timestamptz)", queue, msgIDs, vt)
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	return collectMessages(rows)
}
