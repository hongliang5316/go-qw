package queueworker

import (
	"errors"
)

var (
	ErrQueueFull   = errors.New("the queue is full")
	ErrQueueEmpty  = errors.New("the queue is empty")
	ErrQueueClosed = errors.New("the queue is closed")
)

type Queue interface {
	// Push to the channel
	Push(interface{}) error

	// Pop from the channel
	Pop() (interface{}, bool, error)

	// Block push to the channel
	BPush(interface{})

	// Block pop from the channel
	BPop() (interface{}, bool)

	// Get the queue channel
	QueueCh() chan interface{}

	// The length of queue
	Len() int32

	Close()
}

type queue struct {
	queueCh chan interface{}
	size    int32
}

func NewQueue(size int32) Queue {
	return &queue{
		queueCh: make(chan interface{}, size),
		size:    size,
	}
}

func (q *queue) Push(t interface{}) error {
	select {
	case q.queueCh <- t:
		return nil
	default:
		return errors.New("queue is full")
	}
}

func (q *queue) Pop() (interface{}, bool, error) {
	select {
	case t, ok := <-q.queueCh:
		if !ok {
			return nil, false, ErrQueueClosed
		}
		return t, ok, nil
	default:
		return nil, false, ErrQueueEmpty
	}
}

func (q *queue) BPush(t interface{}) {
	q.queueCh <- t
}

func (q *queue) BPop() (t interface{}, ok bool) {
	t, ok = <-q.queueCh
	return
}

func (q *queue) QueueCh() chan interface{} {
	return q.queueCh
}

func (q *queue) Len() int32 {
	return int32(len(q.queueCh))
}

func (q *queue) Close() {
	close(q.queueCh)
}
