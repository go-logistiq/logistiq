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

func waitForConnection(t *testing.T, h *Handler, timeout time.Duration) {
	t.Helper()
	start := time.Now()
	for {
		if time.Since(start) >= timeout {
			t.Skipf("Skipping test: timeout waiting for NATS connection at %s", natsURL)
		}
		h.mutex.Lock()
		if h.natsConn != nil && h.natsConn.IsConnected() {
			h.mutex.Unlock()
			return
		}
		h.mutex.Unlock()
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHandler_New(t *testing.T) {
	var handlerSet bool
	setHandler := func(slog.Handler) {
		handlerSet = true
	}

	opts := Options{
		Level:      slog.LevelInfo,
		BatchSize:  10,
		Timeout:    100 * time.Millisecond,
		NATSURL:    natsURL,
		Subject:    testSubject,
		SetHandler: setHandler,
	}

	h := New(opts)
	defer h.Close()

	waitForConnection(t, h, 5*time.Second)

	assert.NotNil(t, h, "Handler should be created")
	assert.Equal(t, opts.Level, h.level, "Level should match")
	assert.Equal(t, opts.BatchSize, h.batchSize, "BatchSize should match")
	assert.Equal(t, opts.Timeout, h.timeout, "Timeout should match")
	assert.Equal(t, opts.Subject, h.subject, "Subject should match")
	assert.NotNil(t, h.natsConn, "NATS connection should be initialized")
	assert.True(t, handlerSet, "SetHandler should have been called")
}

func TestHandler_Enabled(t *testing.T) {
	opts := Options{
		Level:     slog.LevelWarn,
		BatchSize: 10,
		Timeout:   100 * time.Millisecond,
		NATSURL:   natsURL,
		Subject:   testSubject,
	}
	h := New(opts)
	defer h.Close()

	waitForConnection(t, h, 5*time.Second)

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
	h := New(opts)
	defer h.Close()

	waitForConnection(t, h, 5*time.Second)

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

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 2, "Should have received 2 records")
	if len(received) >= 2 {
		assert.Equal(t, "Test message 1", received[0].Message, "First message should match")
		assert.Equal(t, int(slog.LevelInfo), received[0].Level, "First level should match")
		assert.Equal(t, "Test message 2", received[1].Message, "Second message should match")
		assert.Equal(t, int(slog.LevelWarn), received[1].Level, "Second level should match")
	}
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
	h := New(opts)
	defer h.Close()

	waitForConnection(t, h, 5*time.Second)

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

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 1, "Should have received 1 record")
	if len(received) > 0 {
		assert.Equal(t, "Test message", received[0].Message, "Message should match")
		assert.Equal(t, int(slog.LevelInfo), received[0].Level, "Level should match")
	}
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
	h := New(opts)
	defer h.Close()

	waitForConnection(t, h, 5*time.Second)

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

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 1, "Should have received 1 record")
	if len(received) > 0 {
		assert.Equal(t, "Test with attrs", received[0].Message, "Message should match")
		assert.Equal(t, int(slog.LevelInfo), received[0].Level, "Level should match")
		assert.Equal(t, "value", received[0].Attributes["key"], "Attribute 'key' should match")
		assert.Equal(t, float64(42), received[0].Attributes["number"], "Attribute 'number' should match as float64")
	}
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
	h := New(opts)
	defer h.Close()

	waitForConnection(t, h, 5*time.Second)

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

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, received, 1, "Should have received 1 record after close")
	if len(received) > 0 {
		assert.Equal(t, "Test before close", received[0].Message, "Message should match")
	}
}

func TestHandler_QueueLimit(t *testing.T) {
	opts := Options{
		Level:     slog.LevelInfo,
		BatchSize: 1000,
		Timeout:   1 * time.Second,
		NATSURL:   natsURL,
		Subject:   testSubject,
	}
	h := New(opts)
	defer h.Close()

	waitForConnection(t, h, 5*time.Second)

	for i := 0; i < 1001; i++ {
		err := h.Handle(context.Background(), slog.Record{
			Time:    time.Now(),
			Message: "Test message",
			Level:   slog.LevelInfo,
		})
		require.NoError(t, err)
	}

	time.Sleep(300 * time.Millisecond)

	h.mutex.Lock()
	defer h.mutex.Unlock()
	assert.LessOrEqual(t, len(h.queue), 1000, "Queue should not exceed 1000 entries")
}
