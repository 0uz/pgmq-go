package pgmq

import (
	"errors"
	"fmt"
)

var (
	// ErrNoRows is returned when a read or pop operation finds no available messages.
	ErrNoRows = errors.New("pgmq: no rows in result set")

	// ErrQueueNotFound is returned when an operation targets a queue that does not exist.
	ErrQueueNotFound = errors.New("pgmq: queue not found")

	// ErrInvalidOption is returned when conflicting or invalid options are provided.
	ErrInvalidOption = errors.New("pgmq: invalid option")
)

// wrapPostgresError wraps a postgres-level error with context.
func wrapPostgresError(err error) error {
	return fmt.Errorf("pgmq: postgres error: %w", err)
}
