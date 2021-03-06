package qw

import (
	// "fmt"
	"context"
	"sync"
	"time"
)

type WorkHandler func(context.Context, interface{}) error
type StopHandler func() error

type QueueWorker interface {
	// Stop close the channel
	Stop()

	// Stopped until all jobs were done
	Stopped() bool

	Errors() chan error

	// Push to the channel
	Push(interface{}) error

	// Pop from the channel
	Pop() (interface{}, bool, error)

	// Block pop from the channel
	BPop() (interface{}, bool)

	// Get the queue channel
	QueueCh() chan interface{}

	// The length of queue
	Len() int32
}

type Options struct {
	// The inner queue size (default 1000)
	QueueSize int32
	// The inner worker num (default 10)
	WorkerNum int32
	// This code is copied from https://github.com/Shopify/sarama/blob/master/config.go#L122
	// Thanks sarama
	Retry struct {
		// The total number of times to retry a metadata request when the
		// cluster is in the middle of a leader election (default 3).
		Max int
		// How long to wait for leader election to occur before retrying
		// (default 250ms). Similar to the JVM's `retry.backoff.ms`.
		Backoff time.Duration
		// Called to compute backoff time dynamically. Useful for implementing
		// more sophisticated backoff strategies. This takes precedence over
		// `Backoff` if set.
		BackoffFunc func(retries, maxRetries int) time.Duration
	}

	// Context per worker
	ContextFunc func() context.Context

	// When worker closed
	DropFunc func(ctx context.Context) error
}

func NewOptions() *Options {
	opt := new(Options)

	opt.QueueSize = 1000
	opt.WorkerNum = 10

	opt.Retry.Max = 3
	opt.Retry.Backoff = 250 * time.Millisecond

	return opt
}

type queueWorker struct {
	opt     *Options
	errCh   chan error
	stopped chan struct{}

	Queue Queue
}

func NewQueueWorker(opt *Options, wh WorkHandler) QueueWorker {
	qw := &queueWorker{
		opt:     opt,
		errCh:   make(chan error, 1),
		Queue:   NewQueue(opt.QueueSize),
		stopped: make(chan struct{}, 1),
	}
	go qw.start(wh)
	return qw
}

func (qw *queueWorker) start(wh WorkHandler) {
	var wg sync.WaitGroup
	for i := int32(0); i < qw.opt.WorkerNum; i++ {
		wg.Add(1)
		ctx := context.Background()
		if qw.opt.ContextFunc != nil {
			ctx = qw.opt.ContextFunc()
		}
		go func(ctx context.Context, qw *queueWorker, wh WorkHandler) {
			defer wg.Done()
			for data := range qw.QueueCh() {
				retry := qw.opt.Retry.Max
				for {
					err := wh(ctx, data)
					// fmt.Println("retry: ", retry)
					if err == nil {
						break
					}

					select {
					case qw.Errors() <- err:
					default:
					}

					retry--

					if retry < 0 {
						break
					}

					if qw.opt.Retry.BackoffFunc != nil {
						maxRetries := qw.opt.Retry.Max
						retries := maxRetries - retry
						time.Sleep(qw.opt.Retry.BackoffFunc(retries, maxRetries))
					} else {
						time.Sleep(qw.opt.Retry.Backoff)
					}
				}
			}
			if qw.opt.DropFunc != nil {
				qw.opt.DropFunc(ctx)
			}
		}(ctx, qw, wh)
	}
	wg.Wait()
	qw.stopped <- struct{}{}
}

func (qw *queueWorker) Errors() chan error {
	return qw.errCh
}

func (qw *queueWorker) Stop() {
	qw.Queue.Close()
}

func (qw *queueWorker) Stopped() bool {
	<-qw.stopped
	return true
}

func (qw *queueWorker) Push(t interface{}) error {
	return qw.Queue.Push(t)
}

func (qw *queueWorker) Pop() (interface{}, bool, error) {
	return qw.Queue.Pop()
}

func (qw *queueWorker) BPop() (interface{}, bool) {
	return qw.Queue.BPop()
}

func (qw *queueWorker) QueueCh() chan interface{} {
	return qw.Queue.QueueCh()
}

func (qw *queueWorker) Len() int32 {
	return qw.Queue.Len()
}
