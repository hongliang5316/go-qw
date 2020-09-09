package qw

import (
	"fmt"
	"testing"
	"time"
)

func job(i interface{}) error {
	fmt.Printf("do job: %s\n", i.(string))
	time.Sleep(100 * time.Millisecond)
	return nil
}

func TestNewQueueWorker(t *testing.T) {
	opt := NewOptions()
	opt.Retry.Max = 2
	opt.Retry.Backoff = time.Second
	qw := NewQueueWorker(opt, job)
	qw.Push("data")
	qw.Push("data")
	qw.Stop()
	if qw.Stopped() {
		return
	}
}
