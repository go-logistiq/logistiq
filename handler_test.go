package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	natsURL     = "nats://10.172.0.1:4223"
	testSubject = "logs.test.app1"
)

func TestHandler_New(t *testing.T) {
	opts := Options{
		Level:     slog.LevelInfo,
		BatchSize: 10,
		Timeout:   100 * time.Millisecond,
		NATSURL:   natsURL,
		Subject:   testSubject,
	}

	h, err := New(opts)
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer h.Close()

	assert.NotNil(t, h, "Handler should be created")
	assert.Equal(t, opts.Level, h.level, "Level should match")
	assert.Equal(t, opts.BatchSize, h.batchSize, "BatchSize should match")
	assert.Equal(t, opts.Timeout, h.timeout, "Timeout should match")
	assert.Equal(t, opts.Subject, h.subject, "Subject should match")
	assert.NotNil(t, h.natsConn, "NATS connection should be initialized")
}

func TestHandler_Enabled(t *testing.T) {
	h, err := New(Options{
		Level:     slog.LevelWarn,
		BatchSize: 10,
		Timeout:   100 * time.Millisecond,
		NATSURL:   natsURL,
		Subject:   testSubject,
	})
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer h.Close()

	assert.False(t, h.Enabled(context.Background(), slog.LevelInfo), "Info should be disabled")
	assert.True(t, h.Enabled(context.Background(), slog.LevelWarn), "Warn should be enabled")
	assert.True(t, h.Enabled(context.Background(), slog.LevelError), "Error should be enabled")
}

func TestHandler_HandleAndFlush_BatchSize(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer nc.Close()

	opts := Options{
		Level:     slog.LevelInfo,
		BatchSize: 2,
		Timeout:   1 * time.Second,
		NATSURL:   natsURL,
		Subject:   testSubject,
	}
	h, err := New(opts)
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer h.Close()

	var received []logRecord
	var mu sync.Mutex
	_, err = nc.Subscribe(testSubject, func(msg *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()
		var records []logRecord
		err := json.Unmarshal(msg.Data, &records)
		assert.NoError(t, err, "Failed to unmarshal NATS message")
		received = append(received, records...)
	})
	require.NoError(t, err, "Failed to subscribe to NATS")

	err = h.Handle(context.Background(), slog.Record{
		Time:    time.Now(),
		Message: "Test message 1",
		Level:   slog.LevelInfo,
	})
	require.NoError(t, err)
	err = h.Handle(context.Background(), slog.Record{
		Time:    time.Now(),
		Message: "Test message 2",
		Level:   slog.LevelWarn,
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 2, "Should have received 2 records")
	assert.Equal(t, "Test message 1", received[0].Message, "First message should match")
	assert.Equal(t, int(slog.LevelInfo), received[0].Level, "First level should match")
	assert.Equal(t, "Test message 2", received[1].Message, "Second message should match")
	assert.Equal(t, int(slog.LevelWarn), received[1].Level, "Second level should match")
}

func TestHandler_HandleAndFlush_Timeout(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer nc.Close()

	opts := Options{
		Level:     slog.LevelInfo,
		BatchSize: 10,
		Timeout:   50 * time.Millisecond,
		NATSURL:   natsURL,
		Subject:   testSubject,
	}
	h, err := New(opts)
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer h.Close()

	var received []logRecord
	var mu sync.Mutex
	_, err = nc.Subscribe(testSubject, func(msg *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()
		var records []logRecord
		err := json.Unmarshal(msg.Data, &records)
		assert.NoError(t, err, "Failed to unmarshal NATS message")
		received = append(received, records...)
	})
	require.NoError(t, err, "Failed to subscribe to NATS")

	err = h.Handle(context.Background(), slog.Record{
		Time:    time.Now(),
		Message: "Test message",
		Level:   slog.LevelInfo,
	})
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 1, "Should have received 1 record")
	assert.Equal(t, "Test message", received[0].Message, "Message should match")
	assert.Equal(t, int(slog.LevelInfo), received[0].Level, "Level should match")
}

func TestHandler_HandleWithAttributes(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer nc.Close()

	opts := Options{
		Level:     slog.LevelInfo,
		BatchSize: 1,
		Timeout:   1 * time.Second,
		NATSURL:   natsURL,
		Subject:   testSubject,
	}
	h, err := New(opts)
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer h.Close()

	var received []logRecord
	var mu sync.Mutex
	_, err = nc.Subscribe(testSubject, func(msg *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()
		var records []logRecord
		err := json.Unmarshal(msg.Data, &records)
		assert.NoError(t, err, "Failed to unmarshal NATS message")
		received = append(received, records...)
	})
	require.NoError(t, err, "Failed to subscribe to NATS")

	r := slog.NewRecord(time.Now(), slog.LevelInfo, "Test with attrs", 0)
	r.AddAttrs(slog.String("key", "value"), slog.Int("number", 42))

	err = h.Handle(context.Background(), r)
	require.NoError(t, err)

	time.Sleep(100 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 1, "Should have received 1 record")
	assert.Equal(t, "Test with attrs", received[0].Message, "Message should match")
	assert.Equal(t, int(slog.LevelInfo), received[0].Level, "Level should match")
	assert.Equal(t, "value", received[0].Attributes["key"], "Attribute 'key' should match")
	assert.Equal(t, float64(42), received[0].Attributes["number"], "Attribute 'number' should match as float64")
}

func TestHandler_Close(t *testing.T) {
	nc, err := nats.Connect(natsURL)
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}
	defer nc.Close()

	opts := Options{
		Level:     slog.LevelInfo,
		BatchSize: 10,
		Timeout:   10 * time.Millisecond,
		NATSURL:   natsURL,
		Subject:   testSubject,
	}
	h, err := New(opts)
	if err != nil {
		t.Skipf("Skipping test: failed to connect to NATS at %s: %v", natsURL, err)
	}

	var received []logRecord
	var mu sync.Mutex
	_, err = nc.Subscribe(testSubject, func(msg *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()
		var records []logRecord
		err := json.Unmarshal(msg.Data, &records)
		assert.NoError(t, err, "Failed to unmarshal NATS message")
		received = append(received, records...)
	})
	require.NoError(t, err, "Failed to subscribe to NATS")

	err = h.Handle(context.Background(), slog.Record{
		Time:    time.Now(),
		Message: "Test before close",
		Level:   slog.LevelInfo,
	})
	require.NoError(t, err)

	err = h.Close()
	require.NoError(t, err)

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 1, "Should have received 1 record after close")
	if len(received) > 0 {
		assert.Equal(t, "Test before close", received[0].Message, "Message should match")
	}
	assert.True(t, h.natsConn.IsClosed(), "NATS connection should be closed")
}
