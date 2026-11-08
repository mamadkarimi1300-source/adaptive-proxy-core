package storage

import (
	"sync"
	"sync/atomic"
)

// Logger accepts events without making proxy request handling wait for disk I/O.
type Logger struct {
	repository Repository
	queue      chan Event
	done       chan struct{}
	closeOnce  sync.Once
	dropped    atomic.Uint64
}

// NewLogger starts a bounded asynchronous event writer.
func NewLogger(repository Repository, queueSize int) *Logger {
	if queueSize < 1 {
		queueSize = 256
	}
	logger := &Logger{
		repository: repository,
		queue:      make(chan Event, queueSize),
		done:       make(chan struct{}),
	}
	go logger.writeLoop()
	return logger
}

// Record queues an event. Events are dropped rather than blocking proxy traffic
// when the bounded queue is full.
func (l *Logger) Record(event Event) {
	if l == nil {
		return
	}
	select {
	case <-l.done:
		l.dropped.Add(1)
	default:
	}
	select {
	case l.queue <- event:
	default:
		l.dropped.Add(1)
	}
}

// Dropped returns the number of events discarded because the queue was full
// or the logger had already been closed.
func (l *Logger) Dropped() uint64 {
	if l == nil {
		return 0
	}
	return l.dropped.Load()
}

// Close stops accepting events, flushes queued events, and closes the repository.
// Call it after proxy handlers have stopped.
func (l *Logger) Close() error {
	if l == nil {
		return nil
	}
	var closeErr error
	l.closeOnce.Do(func() {
		close(l.done)
		close(l.queue)
	})
	<-l.done
	return closeErr
}

func (l *Logger) writeLoop() {
	defer func() {
		_ = l.repository.Close()
	}()
	for event := range l.queue {
		_ = l.repository.InsertEvent(event)
	}
}
