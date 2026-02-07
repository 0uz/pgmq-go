# pgmq-go

A Go client for [Postgres Message Queue](https://github.com/pgmq/pgmq) (PGMQ).

Supports **PGMQ v1.10.0** and **PostgreSQL 16, 17, 18**.

[![CI](https://github.com/ouz/pgmq-go/actions/workflows/ci.yaml/badge.svg)](https://github.com/ouz/pgmq-go/actions/workflows/ci.yaml)

## Features

- Full PGMQ v1.10.0 API coverage
- Struct-based `Client` with method API
- Functional options for send and read operations
- Works with `*pgxpool.Pool`, `*pgx.Conn`, and `pgx.Tx`
- Message headers support
- Long-polling reads (`read_with_poll`)
- FIFO grouped reads (standard and round-robin)
- Batch operations (send, read, pop, delete, archive, set_vt)
- Queue metrics and listing
- LISTEN/NOTIFY support
- Visibility timeout with int or timestamp
- Partitioned and unlogged queues
- Comprehensive test suite

## Installation

```bash
go get github.com/ouz/pgmq-go
```

## Quick Start

Start a Postgres instance with PGMQ:

```bash
docker run -d --name postgres -e POSTGRES_PASSWORD=password -p 5432:5432 quay.io/tembo/pgmq-pg:latest
```

```go
package main

import (
    "context"
    "encoding/json"
    "fmt"
    "log"

    pgmq "github.com/ouz/pgmq-go"
)

func main() {
    ctx := context.Background()

    // Create a client from a connection string.
    client, err := pgmq.NewFromConnString(ctx, "postgres://postgres:password@localhost:5432/postgres")
    if err != nil {
        log.Fatal(err)
    }

    // Create the PGMQ extension.
    if err := client.CreateExtension(ctx); err != nil {
        log.Fatal(err)
    }

    // Create a queue.
    if err := client.CreateQueue(ctx, "my_queue"); err != nil {
        log.Fatal(err)
    }

    // Send a message.
    msgID, err := client.Send(ctx, "my_queue", json.RawMessage(`{"hello": "world"}`))
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Sent message: %d\n", msgID)

    // Read a message (30 second visibility timeout).
    msg, err := client.Read(ctx, "my_queue", 30)
    if err != nil {
        log.Fatal(err)
    }
    fmt.Printf("Read message: %s\n", msg.Message)

    // Archive the message.
    if _, err := client.Archive(ctx, "my_queue", msg.MsgID); err != nil {
        log.Fatal(err)
    }
}
```

## API Reference

### Client

```go
// Create from an existing pgx-compatible DB (pool, conn, or tx).
client := pgmq.New(pool)

// Create from a connection string (creates a pool internally).
client, err := pgmq.NewFromConnString(ctx, connString)

// Create the PGMQ extension.
client.CreateExtension(ctx)

// Ping the database.
client.Ping(ctx)
```

### Queue Management

```go
client.CreateQueue(ctx, "my_queue")
client.CreateUnloggedQueue(ctx, "my_queue")
client.CreatePartitionedQueue(ctx, "my_queue", "10000", "100000")
client.DropQueue(ctx, "my_queue")

queues, _ := client.ListQueues(ctx)
count, _ := client.Purge(ctx, "my_queue")
```

### Sending Messages

```go
// Simple send.
id, _ := client.Send(ctx, "my_queue", msg)

// With delay (seconds).
id, _ := client.Send(ctx, "my_queue", msg, pgmq.WithDelay(10))

// With delay (timestamp).
id, _ := client.Send(ctx, "my_queue", msg, pgmq.WithDelayTimestamp(time.Now().Add(time.Minute)))

// With headers.
id, _ := client.Send(ctx, "my_queue", msg, pgmq.WithHeaders(json.RawMessage(`{"key": "val"}`)))

// Combined.
id, _ := client.Send(ctx, "my_queue", msg, pgmq.WithHeaders(headers), pgmq.WithDelay(5))

// Batch send.
ids, _ := client.SendBatch(ctx, "my_queue", msgs)
ids, _ := client.SendBatch(ctx, "my_queue", msgs, pgmq.WithDelay(10))
ids, _ := client.SendBatch(ctx, "my_queue", msgs, pgmq.WithBatchHeaders(headerSlice))
```

### Reading Messages

```go
// Read one message (visibility timeout in seconds, 0 = default 30s).
msg, _ := client.Read(ctx, "my_queue", 30)

// Read batch.
msgs, _ := client.ReadBatch(ctx, "my_queue", 30, 10)

// Read with long-polling.
msgs, _ := client.ReadWithPoll(ctx, "my_queue", 30, 10,
    pgmq.WithMaxPollSeconds(5),
    pgmq.WithPollIntervalMs(100),
)

// Pop (read + delete).
msg, _ := client.Pop(ctx, "my_queue")
msgs, _ := client.PopBatch(ctx, "my_queue", 10)

// Grouped reads (FIFO by x-pgmq-group header).
msgs, _ := client.ReadGrouped(ctx, "my_queue", 30, 10)
msgs, _ := client.ReadGroupedRoundRobin(ctx, "my_queue", 30, 10)
```

### Message Management

```go
// Delete.
ok, _ := client.Delete(ctx, "my_queue", msgID)
deleted, _ := client.DeleteBatch(ctx, "my_queue", msgIDs)

// Archive.
ok, _ := client.Archive(ctx, "my_queue", msgID)
archived, _ := client.ArchiveBatch(ctx, "my_queue", msgIDs)

// Visibility timeout.
msg, _ := client.SetVT(ctx, "my_queue", msgID, 60)             // seconds
msg, _ := client.SetVTTimestamp(ctx, "my_queue", msgID, ts)     // timestamp
msgs, _ := client.SetVTBatch(ctx, "my_queue", msgIDs, 60)
msgs, _ := client.SetVTBatchTimestamp(ctx, "my_queue", msgIDs, ts)
```

### Metrics

```go
m, _ := client.Metrics(ctx, "my_queue")
fmt.Printf("Queue length: %d, Total messages: %d\n", m.QueueLength, m.TotalMessages)

all, _ := client.MetricsAll(ctx)
```

### LISTEN/NOTIFY

```go
// Enable notifications on insert (throttle = 250ms).
client.EnableNotifyInsert(ctx, "my_queue", 250)

// Disable notifications.
client.DisableNotifyInsert(ctx, "my_queue")
```

### Transaction Support

```go
tx, _ := pool.Begin(ctx)
txClient := pgmq.New(tx)

id, _ := txClient.Send(ctx, "my_queue", msg)
msg, _ := txClient.Read(ctx, "my_queue", 30)
txClient.Archive(ctx, "my_queue", id)

tx.Commit(ctx)
```

## Types

### Message

```go
type Message struct {
    MsgID      int64
    ReadCount  int64
    EnqueuedAt time.Time
    LastReadAt *time.Time      // PGMQ v1.10.0+
    VT         time.Time
    Message    json.RawMessage
    Headers    json.RawMessage
}
```

### QueueInfo

```go
type QueueInfo struct {
    QueueName     string
    IsPartitioned bool
    IsUnlogged    bool
    CreatedAt     time.Time
}
```

### Metrics

```go
type Metrics struct {
    QueueName          string
    QueueLength        int64
    NewestMsgAgeSec    *int64
    OldestMsgAgeSec    *int64
    TotalMessages      int64
    ScrapeTime         time.Time
    QueueVisibleLength int64
}
```

## Errors

```go
pgmq.ErrNoRows       // No messages available
pgmq.ErrQueueNotFound // Queue does not exist
pgmq.ErrInvalidOption // Conflicting options provided
```