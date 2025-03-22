package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type Handler struct {
	level     slog.Level
	queue     []slog.Record
	mu        sync.Mutex
	batchSize int
	timeout   time.Duration
	subject   string
	nc        *nats.Conn
	stop      chan struct{}
	wg        sync.WaitGroup
}

type Options struct {
	Level     slog.Level
	BatchSize int
	Timeout   time.Duration
	NATSURL   string
	Subject   string
}

func New(opts Options) (*Handler, error) {
	nc, err := nats.Connect(opts.NATSURL)
	if err != nil {
		return nil, err
	}

	h := &Handler{
		level:     opts.Level,
		queue:     make([]slog.Record, 0, opts.BatchSize),
		batchSize: opts.BatchSize,
		timeout:   opts.Timeout,
		subject:   opts.Subject,
		nc:        nc,
		stop:      make(chan struct{}),
	}

	h.wg.Add(1)
	go h.worker()
	return h, nil
}

func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	h.queue = append(h.queue, r)
	shouldFlush := len(h.queue) >= h.batchSize
	h.mu.Unlock()

	if shouldFlush {
		h.flush()
	}
	return nil
}

func (h *Handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *Handler) WithGroup(name string) slog.Handler {
	return h
}

func (h *Handler) flush() {
	h.mu.Lock()
	if len(h.queue) == 0 {
		h.mu.Unlock()
		return
	}

	records := h.queue
	h.queue = make([]slog.Record, 0, h.batchSize)
	h.mu.Unlock()

	data, err := json.Marshal(records)
	if err != nil {
		return
	}

	go func() {
		err := h.nc.Publish(h.subject, data)
		if err != nil {
			return
		}
	}()
}

func (h *Handler) worker() {
	defer h.wg.Done()
	ticker := time.NewTicker(h.timeout)
	defer ticker.Stop()

	for {
		select {
		case <-h.stop:
			h.mu.Lock()
			h.flush()
			h.mu.Unlock()
			return
		case <-ticker.C:
			h.mu.Lock()
			h.flush()
			h.mu.Unlock()
		}
	}
}

func (h *Handler) Close() error {
	close(h.stop)
	h.wg.Wait()
	h.nc.Close()
	return nil
}
