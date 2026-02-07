// Package pgmq provides a Go client for Postgres Message Queue (PGMQ) v1.10.0+.
//
// It supports PostgreSQL 16, 17, and 18 with full coverage of the PGMQ SQL API
// including queue management, message sending/reading, metrics, and LISTEN/NOTIFY.
//
// The client works with any pgx-compatible connection type: *pgxpool.Pool, *pgx.Conn, or pgx.Tx.
package pgmq

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

const vtDefault = 30

// DB is an interface satisfied by *pgxpool.Pool, *pgx.Conn, and pgx.Tx.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Client is a PGMQ client that wraps a DB connection.
type Client struct {
	db DB
}

// New creates a new PGMQ Client using the provided DB interface.
// The DB can be a *pgxpool.Pool, *pgx.Conn, or pgx.Tx.
func New(db DB) *Client {
	return &Client{db: db}
}

// NewFromConnString creates a new PGMQ Client by establishing a connection pool
// from the given connection string.
func NewFromConnString(ctx context.Context, connString string) (*Client, error) {
	pool, err := newPgxPool(ctx, connString)
	if err != nil {
		return nil, err
	}

	return &Client{db: pool}, nil
}

// DB returns the underlying DB interface for advanced usage such as
// starting transactions or running custom queries.
func (c *Client) DB() DB {
	return c.db
}

// Ping verifies the database connection is alive.
func (c *Client) Ping(ctx context.Context) error {
	_, err := c.db.Exec(ctx, "SELECT 1")
	if err != nil {
		return wrapPostgresError(err)
	}

	return nil
}

// CreateExtension creates the PGMQ extension if it does not already exist.
func (c *Client) CreateExtension(ctx context.Context) error {
	_, err := c.db.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pgmq CASCADE")
	if err != nil {
		return fmt.Errorf("pgmq: error creating extension: %w", err)
	}

	return nil
}

// newPgxPool creates a new *pgxpool.Pool from a connection string.
func newPgxPool(ctx context.Context, connString string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(connString)
	if err != nil {
		return nil, fmt.Errorf("pgmq: error parsing connection string: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("pgmq: error creating pool: %w", err)
	}

	return pool, nil
}

// scanMessage scans a row from pgx.Rows into a Message struct.
// It handles variable column counts across PGMQ versions.
func scanMessage(rows pgx.Rows) (*Message, error) {
	var msg Message

	fields := rows.FieldDescriptions()
	dest := make([]any, len(fields))

	for i, f := range fields {
		switch f.Name {
		case "msg_id":
			dest[i] = &msg.MsgID
		case "read_ct":
			dest[i] = &msg.ReadCount
		case "enqueued_at":
			dest[i] = &msg.EnqueuedAt
		case "last_read_at":
			dest[i] = &msg.LastReadAt
		case "vt":
			dest[i] = &msg.VT
		case "message":
			dest[i] = &msg.Message
		case "headers":
			dest[i] = &msg.Headers
		default:
			var discard any
			dest[i] = &discard
		}
	}

	if err := rows.Scan(dest...); err != nil {
		return nil, wrapPostgresError(err)
	}

	return &msg, nil
}
