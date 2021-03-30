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

	close *atomic.AtomicBool
	wg    sync.WaitGroup
}

func (c *byOneConsumer) start() {
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
	defer func() {
		ctx, cancelFunc := context.WithTimeout(context.Background(), timeout)
		defer cancelFunc()
		_ = c.consumer.Close(ctx)
	}()

	wait := make(chan struct{})
	go func() {
		for {
			if c.doWait() {
				close(wait)
				return
			}
		}
	}()

	select {
	case <-time.After(timeout):
		return
	case <-wait:
		return
	}
}

func (c *byOneConsumer) doWait() (waitComplete bool) {
	defer func() {
		// panic "sync: WaitGroup is reused before previous Wait has returned"
		r := recover()
		if r != nil {
			waitComplete = false
		}
	}()

	waitComplete = true
	c.wg.Wait()
	return waitComplete
}
