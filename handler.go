package handler

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

type logRecord struct {
	Level      int            `json:"level"`
	LoggedAt   time.Time      `json:"loggedAt"`
	Message    string         `json:"message"`
	Attributes map[string]any `json:"attributes"`
}

type Handler struct {
	level      slog.Level
	queue      []slog.Record
	mutex      sync.Mutex
	batchSize  int
	timeout    time.Duration
	subject    string
	natsConn   *nats.Conn
	natsURL    string
	stop       chan struct{}
	notifyWork chan struct{}
	waitGroup  sync.WaitGroup
	setHandler func(slog.Handler)
	closed     bool
}

type Options struct {
	Level      slog.Level
	BatchSize  int
	Timeout    time.Duration
	NATSURL    string
	Subject    string
	SetHandler func(slog.Handler)
}

func New(opts Options) *Handler {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}

	h := &Handler{
		level:      opts.Level,
		queue:      make([]slog.Record, 0, opts.BatchSize),
		batchSize:  opts.BatchSize,
		timeout:    opts.Timeout,
		subject:    opts.Subject,
		natsURL:    opts.NATSURL,
		stop:       make(chan struct{}, 1),
		notifyWork: make(chan struct{}, 1),
		setHandler: opts.SetHandler,
	}

	h.waitGroup.Add(1)
	go h.startConnection()

	h.waitGroup.Add(1)
	go h.worker()

	return h
}

func (h *Handler) Enabled(_ context.Context, level slog.Level) bool {
	return level >= h.level
}

func (h *Handler) Handle(_ context.Context, r slog.Record) error {
	h.mutex.Lock()
	if len(h.queue) >= 1000 {
		slog.Warn("Queue full, dropping oldest log")
		h.queue = h.queue[1:]
	}
	h.queue = append(h.queue, r)
	shouldNotify := len(h.queue) >= h.batchSize
	h.mutex.Unlock()

	if shouldNotify {
		select {
		case h.notifyWork <- struct{}{}:
		default:
		}
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
	h.mutex.Lock()
	if len(h.queue) == 0 {
		h.mutex.Unlock()
		return
	}

	recordsToFlush := h.queue
	h.queue = make([]slog.Record, 0, h.batchSize)
	h.mutex.Unlock()

	entries := make([]logRecord, len(recordsToFlush))
	for i, r := range recordsToFlush {
		attrs := make(map[string]any)
		r.Attrs(func(a slog.Attr) bool {
			attrs[a.Key] = a.Value.Any()
			return true
		})
		entries[i] = logRecord{
			Level:      int(r.Level),
			LoggedAt:   r.Time,
			Message:    r.Message,
			Attributes: attrs,
		}
	}

	data, err := json.Marshal(entries)
	if err != nil {
		slog.Error("Failed to marshal logs", "error", err)
		return
	}

	if h.natsConn != nil {
		if err := h.natsConn.Publish(h.subject, data); err != nil {
			slog.Warn("Failed to publish to NATS, relying on internal queuing", "error", err)
		}
	}
}

func (h *Handler) connect() error {
	natsConn, err := nats.Connect(h.natsURL,
		nats.MaxReconnects(-1),
		nats.ReconnectWait(5*time.Second),
		nats.ReconnectBufSize(32*1024*1024),
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			slog.Warn("NATS disconnected", "error", err)
		}),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			slog.Info("NATS reconnected", "url", nc.ConnectedUrl())
		}),
		nats.ClosedHandler(func(nc *nats.Conn) {
			slog.Warn("NATS connection closed")
		}),
	)
	if err != nil {
		return err
	}

	h.mutex.Lock()
	h.natsConn = natsConn
	h.mutex.Unlock()

	if h.setHandler != nil {
		h.setHandler(h)
	}

	return nil
}

func (h *Handler) startConnection() {
	defer h.waitGroup.Done()

	for {
		if err := h.connect(); err != nil {
			slog.Warn("NATS connection attempt failed, retrying", "error", err)
			select {
			case <-h.stop:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}
		break
	}
}

func (h *Handler) worker() {
	defer h.waitGroup.Done()
	ticker := time.NewTicker(h.timeout)
	defer ticker.Stop()

	for {
		select {
		case <-h.stop:
			h.flush()
			return
		case <-ticker.C:
			h.flush()
		case <-h.notifyWork:
			h.flush()
		}
	}
}

func (h *Handler) Close() error {
	h.mutex.Lock()
	if h.closed {
		h.mutex.Unlock()
		return nil
	}
	h.closed = true
	h.mutex.Unlock()

	close(h.stop)
	h.waitGroup.Wait()

	if h.natsConn != nil {
		flushErr := h.natsConn.FlushTimeout(10 * time.Second)
		h.natsConn.Close()
		h.natsConn = nil
		if flushErr != nil {
			slog.Warn("Failed to flush NATS buffer on close", "error", flushErr)
			return flushErr
		}
	}

	return nil
}
