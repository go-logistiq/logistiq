package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestHandler(t *testing.T) {
	natsServer := "nats://localhost:4222"

	nc, err := nats.Connect(natsServer)
	if err != nil {
		t.Skip("NATS server not available, skipping test")
	}
	defer nc.Close()

	tests := []struct {
		name        string
		opts        Options
		logCount    int
		waitTime    time.Duration
		wantRecords int
	}{
		{
			name: "batch size trigger",
			opts: Options{
				Level:     slog.LevelInfo,
				BatchSize: 2,
				Timeout:   time.Second,
				NATSURL:   natsServer,
				Subject:   "logs.test.batch",
			},
			logCount:    4,
			waitTime:    100 * time.Millisecond,
			wantRecords: 4,
		},
		{
			name: "timeout trigger",
			opts: Options{
				Level:     slog.LevelInfo,
				BatchSize: 10,
				Timeout:   100 * time.Millisecond,
				NATSURL:   natsServer,
				Subject:   "logs.test.timeout",
			},
			logCount:    2,
			waitTime:    200 * time.Millisecond,
			wantRecords: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var receivedRecords int

			sub, err := nc.SubscribeSync(tt.opts.Subject)
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()

			h, err := New(tt.opts)
			if err != nil {
				t.Fatal(err)
			}
			defer h.Close()

			for i := 0; i < tt.logCount; i++ {
				h.Handle(context.Background(), slog.Record{
					Time:    time.Now(),
					Message: "test message",
					Level:   slog.LevelInfo,
				})
			}

			time.Sleep(tt.waitTime)

			timeout := time.After(50 * time.Millisecond)
		Collect:
			for {
				select {
				case <-timeout:
					break Collect
				default:
					msg, err := sub.NextMsg(10 * time.Millisecond)
					if err == nats.ErrTimeout {
						break Collect
					}
					if err != nil {
						t.Fatal(err)
					}
					var records []slog.Record
					if err := json.Unmarshal(msg.Data, &records); err == nil {
						receivedRecords += len(records)
					}
				}
			}

			if receivedRecords != tt.wantRecords {
				t.Errorf("got %d records, want %d", receivedRecords, tt.wantRecords)
			}

			if h.Enabled(context.Background(), slog.LevelDebug) {
				t.Error("Debug level should not be enabled with Info minimum")
			}
			if !h.Enabled(context.Background(), slog.LevelInfo) {
				t.Error("Info level should be enabled")
			}
		})
	}
}
