package pgmq_test

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	pgmq "github.com/0uz/pgmq-go"
	"github.com/avast/retry-go/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

var (
	testMsg1 = json.RawMessage(`{"foo": "bar1"}`)
	testMsg2 = json.RawMessage(`{"foo": "bar2"}`)
)

// testDB holds a test database container and pgmq client.
type testDB struct {
	client    *pgmq.Client
	pool      *pgxpool.Pool
	image     string
	container testcontainers.Container
}

func (d *testDB) init() {
	ctx := context.Background()
	req := testcontainers.ContainerRequest{
		Image:        d.image,
		ExposedPorts: []string{"5432/tcp"},
		Env: map[string]string{
			"POSTGRES_USER":     "postgres",
			"POSTGRES_PASSWORD": "password",
		},
		WaitingFor: wait.ForLog("database system is ready to accept connections"),
	}

	var err error
	d.container, err = testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		panic(fmt.Sprintf("failed to start container %s: %v", d.image, err))
	}

	host, err := d.container.Host(ctx)
	if err != nil {
		panic(err)
	}

	port, err := d.container.MappedPort(ctx, "5432/tcp")
	if err != nil {
		panic(err)
	}

	connString := fmt.Sprintf("postgres://postgres:password@%s:%s/postgres", host, port.Port())

	d.pool, err = retry.DoWithData(func() (*pgxpool.Pool, error) {
		cfg, err := pgxpool.ParseConfig(connString)
		if err != nil {
			return nil, err
		}

		pool, err := pgxpool.NewWithConfig(ctx, cfg)
		if err != nil {
			return nil, err
		}

		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return nil, err
		}

		return pool, nil
	})
	if err != nil {
		panic(fmt.Sprintf("failed to connect to %s: %v", d.image, err))
	}

	d.client = pgmq.New(d.pool)
	if err := d.client.CreateExtension(ctx); err != nil {
		panic(fmt.Sprintf("failed to create extension on %s: %v", d.image, err))
	}

	if _, err := d.pool.Exec(ctx, "CREATE EXTENSION IF NOT EXISTS pg_partman"); err != nil {
		panic(fmt.Sprintf("failed to create pg_partman on %s: %v", d.image, err))
	}
}

func (d *testDB) close() {
	if d.pool != nil {
		d.pool.Close()
	}
	if d.container != nil {
		_ = d.container.Terminate(context.Background())
	}
}

var (
	pg17 = &testDB{image: "ghcr.io/pgmq/pg17-pgmq:v1.13.1"}
)

func TestMain(m *testing.M) {
	pg17.init()
	code := m.Run()
	pg17.close()
	os.Exit(code)
}

// --- Ping ---

func TestPing(t *testing.T) {
	ctx := context.Background()
	err := pg17.client.Ping(ctx)
	require.NoError(t, err)
}

// --- Queue Management ---

func TestCreateAndDropQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)

	err = pg17.client.DropQueue(ctx, queue)
	require.NoError(t, err)
}

func TestCreateNonPartitionedQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateNonPartitionedQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	queues, err := pg17.client.ListQueues(ctx)
	require.NoError(t, err)

	found := false
	for _, q := range queues {
		if q.QueueName == queue {
			found = true
			assert.False(t, q.IsPartitioned)
			assert.False(t, q.IsUnlogged)
			break
		}
	}
	assert.True(t, found, "queue %s not found in list", queue)
}

func TestDropQueueNotExist(t *testing.T) {
	ctx := context.Background()
	err := pg17.client.DropQueue(ctx, "nonexistent_queue_xyz")
	require.ErrorIs(t, err, pgmq.ErrQueueNotFound)
}

func TestCreatePartitionedQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreatePartitionedQueue(ctx, queue, "10000", "100000", 6)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	m, err := pg17.client.Metrics(ctx, queue)
	require.NoError(t, err)
	require.NotNil(t, m.DefaultPartitionLength)
	assert.EqualValues(t, 0, *m.DefaultPartitionLength)
}

func TestCreatePartitionedQueueInvalidPremake(t *testing.T) {
	ctx := context.Background()
	err := pg17.client.CreatePartitionedQueue(ctx, t.Name(), "10000", "100000", 0)
	require.ErrorIs(t, err, pgmq.ErrInvalidOption)
}

func TestCreateUnloggedAndDropQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateUnloggedQueue(ctx, queue)
	require.NoError(t, err)

	err = pg17.client.DropQueue(ctx, queue)
	require.NoError(t, err)
}

func TestListQueues(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	queues, err := pg17.client.ListQueues(ctx)
	require.NoError(t, err)

	found := false
	for _, q := range queues {
		if q.QueueName == queue {
			found = true
			assert.False(t, q.IsPartitioned)
			assert.False(t, q.IsUnlogged)
			assert.False(t, q.CreatedAt.IsZero())
			break
		}
	}
	assert.True(t, found, "queue %s not found in list", queue)
}

func TestPurge(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	_, err = pg17.client.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	count, err := pg17.client.Purge(ctx, queue)
	require.NoError(t, err)
	assert.EqualValues(t, 2, count)

	_, err = pg17.client.Read(ctx, queue, 1)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

// --- Send ---

func TestSend(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)
	assert.EqualValues(t, 1, id)

	id, err = pg17.client.Send(ctx, queue, testMsg2)
	require.NoError(t, err)
	assert.EqualValues(t, 2, id)
}

func TestSendWithDelay(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1, pgmq.WithDelay(5))
	require.NoError(t, err)
	assert.EqualValues(t, 1, id)

	// Message should not be visible yet.
	_, err = pg17.client.Read(ctx, queue, 1)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

func TestSendWithDelayTimestamp(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1, pgmq.WithDelayTimestamp(time.Now().Add(5*time.Second)))
	require.NoError(t, err)
	assert.EqualValues(t, 1, id)

	// Message should not be visible yet.
	_, err = pg17.client.Read(ctx, queue, 1)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

func TestSendWithHeaders(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	headers := json.RawMessage(`{"x-custom": "value"}`)
	id, err := pg17.client.Send(ctx, queue, testMsg1, pgmq.WithHeaders(headers))
	require.NoError(t, err)
	assert.EqualValues(t, 1, id)

	msg, err := pg17.client.Read(ctx, queue, 1)
	require.NoError(t, err)
	assert.Equal(t, testMsg1, msg.Message)
	assert.JSONEq(t, `{"x-custom": "value"}`, string(msg.Headers))
}

func TestSendMarshalledStruct(t *testing.T) {
	type TestPayload struct {
		Val int `json:"val"`
	}

	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	payload := TestPayload{Val: 42}
	b, err := json.Marshal(payload)
	require.NoError(t, err)

	_, err = pg17.client.Send(ctx, queue, b)
	require.NoError(t, err)

	msg, err := pg17.client.Read(ctx, queue, 1)
	require.NoError(t, err)

	var result TestPayload
	err = json.Unmarshal(msg.Message, &result)
	require.NoError(t, err)
	assert.Equal(t, payload, result)
}

func TestSendInvalidJSON(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	_, err = pg17.client.Send(ctx, queue, json.RawMessage(`{"foo":}`))
	require.Error(t, err)
}

func TestSendInvalidOptions(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	_, err = pg17.client.Send(ctx, queue, testMsg1, pgmq.WithDelay(5), pgmq.WithDelayTimestamp(time.Now()))
	require.ErrorIs(t, err, pgmq.ErrInvalidOption)
}

// --- SendBatch ---

func TestSendBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	ids, err := pg17.client.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)
	assert.Equal(t, []int64{1, 2}, ids)
}

func TestSendBatchWithDelay(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	ids, err := pg17.client.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2}, pgmq.WithDelay(5))
	require.NoError(t, err)
	assert.Len(t, ids, 2)

	// Messages should not be visible yet.
	msgs, err := pg17.client.ReadBatch(ctx, queue, 1, 5)
	require.NoError(t, err)
	assert.Empty(t, msgs)
}

func TestSendBatchWithDelayTimestamp(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	ids, err := pg17.client.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2}, pgmq.WithDelayTimestamp(time.Now().Add(5*time.Second)))
	require.NoError(t, err)
	assert.Len(t, ids, 2)
}

func TestSendBatchEmptyMessages(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	_, err = pg17.client.SendBatch(ctx, queue, []json.RawMessage{})
	require.ErrorIs(t, err, pgmq.ErrInvalidOption)
}

func TestSendBatchHeaderLengthMismatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	headers := []json.RawMessage{
		json.RawMessage(`{"x": 1}`),
	}

	_, err = pg17.client.SendBatch(
		ctx,
		queue,
		[]json.RawMessage{testMsg1, testMsg2},
		pgmq.WithBatchHeaders(headers),
	)
	require.ErrorIs(t, err, pgmq.ErrInvalidOption)
}

// --- Read ---

func TestRead(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	msg, err := pg17.client.Read(ctx, queue, 30)
	require.NoError(t, err)
	assert.Equal(t, id, msg.MsgID)
	assert.Equal(t, testMsg1, msg.Message)
	assert.EqualValues(t, 1, msg.ReadCount)
	assert.False(t, msg.EnqueuedAt.IsZero())
	assert.False(t, msg.VT.IsZero())

	// Message is now invisible.
	_, err = pg17.client.Read(ctx, queue, 30)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

func TestReadEmptyQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	_, err = pg17.client.Read(ctx, queue, 30)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

func TestReadBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	_, err = pg17.client.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	time.Sleep(time.Second)

	msgs, err := pg17.client.ReadBatch(ctx, queue, 30, 5)
	require.NoError(t, err)
	assert.Len(t, msgs, 2)
	assert.Equal(t, testMsg1, msgs[0].Message)
	assert.Equal(t, testMsg2, msgs[1].Message)

	// Messages are now invisible.
	msgs, err = pg17.client.ReadBatch(ctx, queue, 30, 5)
	require.NoError(t, err)
	assert.Empty(t, msgs)
}

func TestReadWithPoll(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	// Send a message first.
	_, err = pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	msgs, err := pg17.client.ReadWithPoll(ctx, queue, 5, 1, pgmq.WithMaxPollSeconds(2))
	require.NoError(t, err)
	assert.Len(t, msgs, 1)
	assert.Equal(t, testMsg1, msgs[0].Message)
}

func TestReadWithPollTimeout(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	// No messages - should timeout and return empty.
	start := time.Now()
	msgs, err := pg17.client.ReadWithPoll(ctx, queue, 5, 1, pgmq.WithMaxPollSeconds(1))
	elapsed := time.Since(start)
	require.NoError(t, err)
	assert.Empty(t, msgs)
	assert.GreaterOrEqual(t, elapsed, 900*time.Millisecond, "poll should wait at least ~1 second")
}

func TestReadGroupedHead(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	msgs := []json.RawMessage{testMsg1, testMsg2, json.RawMessage(`{"foo": "bar3"}`)}
	headers := []json.RawMessage{
		json.RawMessage(`{"x-pgmq-group": "a"}`),
		json.RawMessage(`{"x-pgmq-group": "a"}`),
		json.RawMessage(`{"x-pgmq-group": "b"}`),
	}
	_, err = pg17.client.SendBatch(ctx, queue, msgs, pgmq.WithBatchHeaders(headers))
	require.NoError(t, err)

	head, err := pg17.client.ReadGroupedHead(ctx, queue, 30, 10)
	require.NoError(t, err)
	require.Len(t, head, 2)
	groups := map[string]bool{}
	for _, msg := range head {
		var h struct {
			Group string `json:"x-pgmq-group"`
		}
		require.NoError(t, json.Unmarshal(msg.Headers, &h))
		groups[h.Group] = true
	}
	assert.True(t, groups["a"])
	assert.True(t, groups["b"])
}

// --- Pop ---

func TestPop(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	msg, err := pg17.client.Pop(ctx, queue)
	require.NoError(t, err)
	assert.Equal(t, id, msg.MsgID)
	assert.Equal(t, testMsg1, msg.Message)

	// Message should be gone.
	_, err = pg17.client.Read(ctx, queue, 1)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

func TestPopEmptyQueue(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	_, err = pg17.client.Pop(ctx, queue)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

func TestPopBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	_, err = pg17.client.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	msgs, err := pg17.client.PopBatch(ctx, queue, 5)
	require.NoError(t, err)
	assert.Len(t, msgs, 2)

	// All messages should be gone.
	_, err = pg17.client.Read(ctx, queue, 1)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

// --- Archive ---

func TestArchive(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	archived, err := pg17.client.Archive(ctx, queue, id)
	require.NoError(t, err)
	assert.True(t, archived)

	// Check archive table.
	stmt := fmt.Sprintf("SELECT * FROM pgmq.a_%s", queue)
	tag, err := pg17.pool.Exec(ctx, stmt)
	require.NoError(t, err)
	assert.EqualValues(t, 1, tag.RowsAffected())

	// Original queue should be empty.
	_, err = pg17.client.Read(ctx, queue, 1)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

func TestArchiveNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	archived, err := pg17.client.Archive(ctx, queue, 999)
	require.NoError(t, err)
	assert.False(t, archived)
}

func TestArchiveBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	ids, err := pg17.client.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	archived, err := pg17.client.ArchiveBatch(ctx, queue, ids)
	require.NoError(t, err)
	assert.Equal(t, ids, archived)

	stmt := fmt.Sprintf("SELECT * FROM pgmq.a_%s", queue)
	tag, err := pg17.pool.Exec(ctx, stmt)
	require.NoError(t, err)
	assert.EqualValues(t, 2, tag.RowsAffected())
}

func TestConvertArchivePartitioned(t *testing.T) {
	ctx := context.Background()
	queue := strings.ToLower(t.Name())

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	_, err = pg17.client.Archive(ctx, queue, id)
	require.NoError(t, err)

	err = pg17.client.ConvertArchivePartitioned(ctx, queue, "10000", "100000", 10)
	require.NoError(t, err)
}

func TestDetachArchive(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	_, err = pg17.client.Archive(ctx, queue, id)
	require.NoError(t, err)

	err = pg17.client.DetachArchive(ctx, queue)
	require.NoError(t, err)
}

// --- Delete ---

func TestDelete(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	deleted, err := pg17.client.Delete(ctx, queue, id)
	require.NoError(t, err)
	assert.True(t, deleted)

	_, err = pg17.client.Read(ctx, queue, 1)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

func TestDeleteNotExist(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	deleted, err := pg17.client.Delete(ctx, queue, 999)
	require.NoError(t, err)
	assert.False(t, deleted)
}

func TestDeleteBatch(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	ids, err := pg17.client.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	deleted, err := pg17.client.DeleteBatch(ctx, queue, ids)
	require.NoError(t, err)
	assert.Equal(t, ids, deleted)

	_, err = pg17.client.Read(ctx, queue, 1)
	require.ErrorIs(t, err, pgmq.ErrNoRows)
}

// --- SetVT ---

func TestSetVT(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	// Read to make it invisible.
	_, err = pg17.client.Read(ctx, queue, 30)
	require.NoError(t, err)

	// Should be invisible now.
	_, err = pg17.client.Read(ctx, queue, 30)
	require.ErrorIs(t, err, pgmq.ErrNoRows)

	// Set VT to 0 to make it visible immediately.
	msg, err := pg17.client.SetVT(ctx, queue, id, 0)
	require.NoError(t, err)
	assert.Equal(t, id, msg.MsgID)

	// Should be visible now.
	_, err = pg17.client.Read(ctx, queue, 1)
	require.NoError(t, err)
}

func TestSetVTTimestamp(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	id, err := pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	// Read to make it invisible.
	_, err = pg17.client.Read(ctx, queue, 30)
	require.NoError(t, err)

	// Set VT to now (make visible immediately).
	msg, err := pg17.client.SetVTTimestamp(ctx, queue, id, time.Now())
	require.NoError(t, err)
	assert.Equal(t, id, msg.MsgID)

	// Should be visible now.
	_, err = pg17.client.Read(ctx, queue, 1)
	require.NoError(t, err)
}

// --- Metrics ---

func TestMetrics(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	_, err = pg17.client.SendBatch(ctx, queue, []json.RawMessage{testMsg1, testMsg2})
	require.NoError(t, err)

	m, err := pg17.client.Metrics(ctx, queue)
	require.NoError(t, err)
	assert.Equal(t, queue, m.QueueName)
	assert.EqualValues(t, 2, m.QueueLength)
	assert.EqualValues(t, 2, m.TotalMessages)
	assert.False(t, m.ScrapeTime.IsZero())
	assert.Nil(t, m.DefaultPartitionLength, "non-partitioned queue should have no default partition")
}

func TestMetricsAll(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	metrics, err := pg17.client.MetricsAll(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, metrics)

	found := false
	for _, m := range metrics {
		if m.QueueName == queue {
			found = true
			break
		}
	}
	assert.True(t, found, "queue %s not found in metrics_all", queue)
}

// --- Notify ---

func TestEnableDisableNotify(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	err = pg17.client.EnableNotifyInsert(ctx, queue, 250)
	require.NoError(t, err)

	err = pg17.client.DisableNotifyInsert(ctx, queue)
	require.NoError(t, err)
}

func TestEnableNotifyInsertNoThrottle(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	err = pg17.client.EnableNotifyInsert(ctx, queue, 0)
	require.NoError(t, err)

	err = pg17.client.DisableNotifyInsert(ctx, queue)
	require.NoError(t, err)
}

func TestUpdateNotifyInsert(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	err = pg17.client.EnableNotifyInsert(ctx, queue, 250)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DisableNotifyInsert(ctx, queue) }()

	err = pg17.client.UpdateNotifyInsert(ctx, queue, 1000)
	require.NoError(t, err)

	throttles, err := pg17.client.ListNotifyInsertThrottles(ctx)
	require.NoError(t, err)

	found := false
	for _, throttle := range throttles {
		if throttle.QueueName == queue {
			found = true
			assert.Equal(t, 1000, throttle.ThrottleIntervalMs)
			break
		}
	}
	assert.True(t, found, "queue %s not found in notify insert throttles", queue)
}

func TestNotifyInsertMixedCase(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	queue := "TestNotifyMixedCase"
	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(context.Background(), queue) }()

	err = pg17.client.EnableNotifyInsert(ctx, queue, 0)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DisableNotifyInsert(context.Background(), queue) }()

	conn, err := pg17.pool.Acquire(ctx)
	require.NoError(t, err)
	defer conn.Release()

	channel := "pgmq.q_" + queue + ".INSERT"
	_, err = conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize())
	require.NoError(t, err)

	_, err = pg17.client.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	notification, err := conn.Conn().WaitForNotification(ctx)
	require.NoError(t, err)
	assert.Equal(t, channel, notification.Channel)
}

// --- Topic Routing ---

func TestTopicRouting(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	pattern := "orders.*.created"
	err = pg17.client.BindTopic(ctx, pattern, queue)
	require.NoError(t, err)

	bindings, err := pg17.client.ListTopicBindingsForQueue(ctx, queue)
	require.NoError(t, err)
	require.Len(t, bindings, 1)
	assert.Equal(t, pattern, bindings[0].Pattern)

	routes, err := pg17.client.TestRouting(ctx, "orders.eu.created")
	require.NoError(t, err)
	require.Len(t, routes, 1)
	assert.Equal(t, queue, routes[0].QueueName)

	matched, err := pg17.client.SendTopic(ctx, "orders.eu.created", json.RawMessage(`{"order": 1}`))
	require.NoError(t, err)
	assert.EqualValues(t, 1, matched)

	msg, err := pg17.client.Read(ctx, queue, 1)
	require.NoError(t, err)
	assert.JSONEq(t, `{"order": 1}`, string(msg.Message))

	sent, err := pg17.client.SendBatchTopic(
		ctx,
		"orders.eu.created",
		[]json.RawMessage{testMsg1, testMsg2},
		pgmq.WithBatchHeaders([]json.RawMessage{
			json.RawMessage(`{"x-trace": "1"}`),
			json.RawMessage(`{"x-trace": "2"}`),
		}),
	)
	require.NoError(t, err)
	require.Len(t, sent, 2)
	assert.Equal(t, queue, sent[0].QueueName)
	assert.Equal(t, queue, sent[1].QueueName)

	removed, err := pg17.client.UnbindTopic(ctx, pattern, queue)
	require.NoError(t, err)
	assert.True(t, removed)

	bindings, err = pg17.client.ListTopicBindingsForQueue(ctx, queue)
	require.NoError(t, err)
	assert.Empty(t, bindings)
}

// --- FIFO Indexes ---

func TestCreateFIFOIndex(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	err = pg17.client.CreateFIFOIndex(ctx, queue)
	require.NoError(t, err)
}

func TestCreateFIFOIndexesAll(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	err = pg17.client.CreateFIFOIndexesAll(ctx)
	require.NoError(t, err)
}

// --- Transaction Support ---

func TestTransactionSupport(t *testing.T) {
	ctx := context.Background()
	queue := t.Name()

	err := pg17.client.CreateQueue(ctx, queue)
	require.NoError(t, err)
	defer func() { _ = pg17.client.DropQueue(ctx, queue) }()

	// Start a transaction.
	tx, err := pg17.pool.Begin(ctx)
	require.NoError(t, err)

	txClient := pgmq.New(tx)

	id, err := txClient.Send(ctx, queue, testMsg1)
	require.NoError(t, err)

	msg, err := txClient.Read(ctx, queue, 1)
	require.NoError(t, err)
	assert.Equal(t, id, msg.MsgID)

	_, err = txClient.Archive(ctx, queue, id)
	require.NoError(t, err)

	err = tx.Commit(ctx)
	require.NoError(t, err)
}
