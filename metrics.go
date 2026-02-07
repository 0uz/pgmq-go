package pgmq

import "context"

// Metrics returns statistics for the specified queue including queue length,
// message ages, and total message count.
func (c *Client) Metrics(ctx context.Context, queue string) (*Metrics, error) {
	var m Metrics
	err := c.db.QueryRow(ctx, "SELECT * FROM pgmq.metrics($1)", queue).Scan(
		&m.QueueName,
		&m.QueueLength,
		&m.NewestMsgAgeSec,
		&m.OldestMsgAgeSec,
		&m.TotalMessages,
		&m.ScrapeTime,
		&m.QueueVisibleLength,
	)
	if err != nil {
		return nil, wrapPostgresError(err)
	}

	return &m, nil
}

// MetricsAll returns statistics for all existing queues.
func (c *Client) MetricsAll(ctx context.Context) ([]Metrics, error) {
	rows, err := c.db.Query(ctx, "SELECT * FROM pgmq.metrics_all()")
	if err != nil {
		return nil, wrapPostgresError(err)
	}
	defer rows.Close()

	var metrics []Metrics
	for rows.Next() {
		var m Metrics
		if err := rows.Scan(
			&m.QueueName,
			&m.QueueLength,
			&m.NewestMsgAgeSec,
			&m.OldestMsgAgeSec,
			&m.TotalMessages,
			&m.ScrapeTime,
			&m.QueueVisibleLength,
		); err != nil {
			return nil, wrapPostgresError(err)
		}
		metrics = append(metrics, m)
	}

	if err := rows.Err(); err != nil {
		return nil, wrapPostgresError(err)
	}

	return metrics, nil
}
