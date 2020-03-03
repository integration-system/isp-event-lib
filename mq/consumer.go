package mq

import (
	"github.com/integration-system/cony"
	"github.com/integration-system/isp-lib/v2/atomic"
	"sync"
	"time"
)

type consumer interface {
	start()
	stop()
	awaitCancel(timeout time.Duration)
}

var _ consumer = (*byOneConsumer)(nil)

type byOneConsumer struct {
	consumer     *cony.Consumer
	callback     func(delivery Delivery)
	errorHandler func(error)
	close        *atomic.AtomicBool
	wg           sync.WaitGroup
}

func (c *byOneConsumer) start() {
	for {
		select {
		case delivery := <-c.consumer.Deliveries():
			if c.close.Get() {
				return
			}
			c.wg.Add(1)
			c.callback(Delivery{wg: &c.wg, delivery: delivery})
		case err := <-c.consumer.Errors():
			if c.close.Get() {
				return
			}
			if c.errorHandler != nil {
				c.errorHandler(err)
			}
		}
	}
}

func (c *byOneConsumer) stop() {
	c.close.Set(true)
}

func (c *byOneConsumer) awaitCancel(timeout time.Duration) {
	defer func() {
		c.consumer.Cancel()
		c.awaitStopDelivery(timeout)
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
		r := recover() //panic("sync: WaitGroup is reused before previous Wait has returned")
		if r != nil {
			waitComplete = false
		}
	}()

	waitComplete = true
	c.wg.Wait()
	return waitComplete
}

func (c *byOneConsumer) awaitStopDelivery(timeout time.Duration) {
	for {
		select {
		case _, open := <-c.consumer.Deliveries():
			if !open {
				return
			}
		case <-time.After(timeout):
			return
		}
	}
}
