package queueworker

import (
	"errors"
)

type Queue interface {
	// Push to the channel
	Push(interface{}) error

	// Pop from the channel
	Pop() (interface{}, error)

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

func (q *queue) Pop() (interface{}, error) {
	select {
	case t := <-q.queueCh:
		return t, nil
	default:
		return nil, errors.New("queue is empty")
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
