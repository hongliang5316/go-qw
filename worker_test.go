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
	fmt.Println("drop")
	return nil
}

func job(ctx context.Context, i []any) error {
	indexPtr, _ := ctx.Value("IndexPtr").(*int64)
	fmt.Printf("do job: %v, context index: %d\n", i, *indexPtr)
	*indexPtr++
	time.Sleep(500 * time.Millisecond)
	return nil
}

func TestNewQueueWorker(t *testing.T) {
	opt := NewOptions()
	opt.WorkerNum = 2
	opt.Retry.Max = 2
	opt.BatchSize = 10
	opt.PeriodicFlush = time.Second
	opt.ContextFunc = initContext
	opt.DropFunc = drop
	qw := NewQueueWorker(opt, job)

	go func() {
		i := 0
		for {
			i++
			if i > 5 {
				break
			}

			time.Sleep(time.Second)
			qw.Push("timer data")
		}
	}()

	qw.Push("data")
	qw.Push("data")
	qw.Push("data")
	qw.Push("data")
	qw.BPush("data")
	qw.BPush("data")
	qw.BPush("data")

	time.Sleep(10 * time.Second)

	qw.Stop()
	if qw.Stopped() {
		return
	}
}
