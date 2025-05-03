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

type Logistiq struct {
	Handler    slog.Handler
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
	closed     bool
}

type Options struct {
	Level     slog.Level
	BatchSize int
	Timeout   time.Duration
	NATSURL   string
	Subject   string
}

func New(opts Options) (*Logistiq, error) {
	if opts.BatchSize <= 0 {
		opts.BatchSize = 100
	}
	if opts.Timeout <= 0 {
		opts.Timeout = 5 * time.Second
	}

	l := &Logistiq{
		level:      opts.Level,
		queue:      make([]slog.Record, 0, opts.BatchSize),
		batchSize:  opts.BatchSize,
		timeout:    opts.Timeout,
		subject:    opts.Subject,
		natsURL:    opts.NATSURL,
		stop:       make(chan struct{}, 1),
		notifyWork: make(chan struct{}, 1),
	}

	l.Handler = l

	l.waitGroup.Add(1)
	go l.startConnection()

	l.waitGroup.Add(1)
	go l.worker()

	return l, nil
}

func (l *Logistiq) Enabled(_ context.Context, level slog.Level) bool {
	return level >= l.level
}

func (l *Logistiq) Handle(_ context.Context, r slog.Record) error {
	l.mutex.Lock()
	if len(l.queue) >= 1000 {
		slog.Warn("Queue full, dropping oldest log")
		l.queue = l.queue[1:]
	}
	l.queue = append(l.queue, r)
	shouldNotify := len(l.queue) >= l.batchSize
	l.mutex.Unlock()

	if shouldNotify {
		select {
		case l.notifyWork <- struct{}{}:
		default:
		}
	}

	return nil
}

func (l *Logistiq) WithAttrs(attrs []slog.Attr) slog.Handler {
	return l
}

func (l *Logistiq) WithGroup(name string) slog.Handler {
	return l
}

func (l *Logistiq) flush() {
	l.mutex.Lock()
	if len(l.queue) == 0 {
		l.mutex.Unlock()
		return
	}

	recordsToFlush := l.queue
	l.queue = make([]slog.Record, 0, l.batchSize)
	l.mutex.Unlock()

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

	if l.natsConn != nil {
		if err := l.natsConn.Publish(l.subject, data); err != nil {
			slog.Warn("Failed to publish to NATS, relying on internal queuing", "error", err)
		}
	}
}

func (l *Logistiq) connect() error {
	natsConn, err := nats.Connect(l.natsURL,
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

	l.mutex.Lock()
	l.natsConn = natsConn
	l.mutex.Unlock()

	return nil
}

func (l *Logistiq) startConnection() {
	defer l.waitGroup.Done()

	for {
		if err := l.connect(); err != nil {
			slog.Warn("NATS connection attempt failed, retrying", "error", err)
			select {
			case <-l.stop:
				return
			case <-time.After(5 * time.Second):
				continue
			}
		}
		break
	}
}

func (l *Logistiq) worker() {
	defer l.waitGroup.Done()
	ticker := time.NewTicker(l.timeout)
	defer ticker.Stop()

	for {
		select {
		case <-l.stop:
			l.flush()
			return
		case <-ticker.C:
			l.flush()
		case <-l.notifyWork:
			l.flush()
		}
	}
}

func (l *Logistiq) Close() error {
	l.mutex.Lock()
	if l.closed {
		l.mutex.Unlock()
		return nil
	}
	l.closed = true
	l.mutex.Unlock()

	close(l.stop)
	l.waitGroup.Wait()

	if l.natsConn != nil {
		flushErr := l.natsConn.FlushTimeout(10 * time.Second)
		l.natsConn.Close()
		l.natsConn = nil
		if flushErr != nil {
			slog.Warn("Failed to flush NATS buffer on close", "error", flushErr)
			return flushErr
		}
	}

	return nil
}
