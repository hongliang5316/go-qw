package queueworker

import (
	"sync"
)

type WorkHandler func(interface{}) error
type StopHandler func() error

type QueueWorker interface {
	// Consume the channel
	// Start(queue, WorkHandler)

	// stopped until all jobs were done
	Stop()

	Stopped() bool

	Errors() chan error

	// Push to the channel
	Push(interface{}) error

	// Pop from the channel
	Pop() (interface{}, error)

	// Block pop from the channel
	BPop() (interface{}, bool)

	// Get the queue channel
	QueueCh() chan interface{}

	// The length of queue
	Len() int32
}

type Options struct {
	QueueSize int32
	WorkerNum int32
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
		stopped: make(chan struct{}, 0),
	}
	go qw.start(wh)
	return qw
}

func (qw *queueWorker) start(wh WorkHandler) {
	var wg sync.WaitGroup
	for i := int32(0); i < qw.opt.WorkerNum; i++ {
		wg.Add(1)
		go func(qw *queueWorker, wh WorkHandler) {
			defer wg.Done()
			for {
				// fmt.Println(fmt.Sprintf("%p", qw))
				data, ok := qw.BPop()
				if ok {
					if err := wh(data); err != nil {
						select {
						case qw.Errors() <- err:
						default:
						}
					}
				} else { // channel is closed, just exit this goroutine
					return
				}
			}
		}(qw, wh)
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

func (qw *queueWorker) Pop() (interface{}, error) {
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
