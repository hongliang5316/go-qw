package qw

import (
	"context"
	// "fmt"
	"sync"
	"time"
)

type (
	WorkHandler func(context.Context, []any) error
	StopHandler func() error
)

type QueueWorker interface {
	// Stop close the channel
	Stop()

	// Stopped until all jobs were done
	Stopped() bool

	Errors() chan error

	// Push to the channel
	Push(any) error

	// Block push to the channel
	BPush(any)

	// Pop from the channel
	Pop() (any, bool, error)

	// Block pop from the channel
	BPop() (any, bool)

	// Get the queue channel
	QueueCh() chan any

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

	BatchSize     int32
	PeriodicFlush time.Duration

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

	if opt.BatchSize <= 0 {
		opt.BatchSize = 1
	}

	if opt.PeriodicFlush <= 0 {
		opt.PeriodicFlush = 5 * time.Second
	}

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

			dataList := []any{}
			batchSize := qw.opt.BatchSize
			periodicFlush := qw.opt.PeriodicFlush

			t := time.NewTimer(periodicFlush)
			defer t.Stop()

		L:
			for {
				select {
				case <-t.C:
					if len(dataList) > 0 {
						_ = wh(ctx, dataList)
						dataList = []any{}
					}
					t.Reset(periodicFlush)
				case data, ok := <-qw.QueueCh():
					if !ok {
						break L
					}

					dataList = append(dataList, data)
					if int32(len(dataList)) < batchSize {
						continue
					}

					t.Reset(periodicFlush)

					retry := qw.opt.Retry.Max
					for {
						err := wh(ctx, dataList)
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

					dataList = []any{}
				}
			}

			if len(dataList) > 0 {
				// process the remaining data
				_ = wh(ctx, dataList)
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

func (qw *queueWorker) Push(t any) error {
	return qw.Queue.Push(t)
}

func (qw *queueWorker) BPush(t any) {
	qw.Queue.BPush(t)
}

func (qw *queueWorker) Pop() (any, bool, error) {
	return qw.Queue.Pop()
}

func (qw *queueWorker) BPop() (any, bool) {
	return qw.Queue.BPop()
}

func (qw *queueWorker) QueueCh() chan any {
	return qw.Queue.QueueCh()
}

func (qw *queueWorker) Len() int32 {
	return qw.Queue.Len()
}
