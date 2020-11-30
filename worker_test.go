package qw

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func initContext() context.Context {
	ctx := context.Background()
	var index int64 = 0
	return context.WithValue(ctx, "IndexPtr", &index)
}

func drop(ctx context.Context) error {
	fmt.Printf("drop")
	return nil
}

func job(ctx context.Context, i interface{}) error {
	indexPtr, _ := ctx.Value("IndexPtr").(*int64)
	fmt.Printf("do job: %s, context index: %d\n", i.(string), *indexPtr)
	*indexPtr++
	time.Sleep(100 * time.Millisecond)
	return nil
}

func TestNewQueueWorker(t *testing.T) {
	opt := NewOptions()
	opt.WorkerNum = 1
	opt.Retry.Max = 2
	opt.Retry.Backoff = time.Second
	opt.ContextFunc = initContext
	opt.DropFunc = drop
	qw := NewQueueWorker(opt, job)
	qw.Push("data")
	qw.Push("data")
	qw.Push("data")
	qw.Push("data")
	qw.Push("data")
	qw.Stop()
	if qw.Stopped() {
		return
	}
}
