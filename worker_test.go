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
	opt := Options{2, 1}
	qw := NewQueueWorker(&opt, job)
	qw.Push("data")
	qw.Push("data")
	qw.Stop()
	if qw.Stopped() {
		return
	}
}
