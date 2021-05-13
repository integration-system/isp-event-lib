package ibm

import (
	"context"
	"sync"
	"time"

	"github.com/integration-system/go-amqp"
	"github.com/integration-system/isp-lib/v2/atomic"
)

type consumer interface {
	start()
	stop()
	awaitCancel(timeout time.Duration)
}

var _ consumer = (*byOneConsumer)(nil)

type byOneConsumer struct {
	ctx        context.Context
	cancelFunc context.CancelFunc

	consumer     *amqp.Receiver
	callback     func(delivery Delivery)
	errorHandler func(error)

	close           *atomic.AtomicBool
	startReturnedCh chan struct{}
	wg              sync.WaitGroup
}

func (c *byOneConsumer) start() {
	defer close(c.startReturnedCh)
	for !c.close.Get() {
		message, err := c.consumer.Receive(c.ctx)
		if err != nil {
			if err == c.ctx.Err() {
				return
			}
			if c.errorHandler != nil {
				c.errorHandler(err)
			}
			return
		}
		c.wg.Add(1)
		c.callback(Delivery{wg: &c.wg, delivery: message})
	}
}

func (c *byOneConsumer) stop() {
	c.cancelFunc()
	c.close.Set(true)
}

func (c *byOneConsumer) awaitCancel(timeout time.Duration) {
	ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
	defer cancelFunc()
	defer func() {
		_ = c.consumer.Close(ctx)
	}()

	waitWgCh := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waitWgCh)
	}()

	select {
	case <-c.startReturnedCh:
	case <-ctx.Done():
		return
	}

	select {
	case <-waitWgCh:
	case <-ctx.Done():
	}
}
