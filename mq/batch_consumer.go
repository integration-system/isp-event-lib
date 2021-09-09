package mq

import (
	"sync"
	"time"

	"github.com/integration-system/isp-lib/v2/atomic"
	"github.com/streadway/amqp"

	"github.com/integration-system/cony"
)

const (
	defaultPurgeTimeout = 3 * time.Second
	defaultSize         = 1000
)

var _ consumer = (*batchConsumer)(nil)

type batchConsumer struct {
	consumer     *cony.Consumer
	onBatch      func(batch []Delivery)
	errorHandler func(error)
	size         int
	purgeTimeout time.Duration

	close *atomic.AtomicBool
	wg    sync.WaitGroup

	reConsume func(consumer *cony.Consumer)
	bo        cony.Backoffer
}

func (c *batchConsumer) start() {
	if c.size <= 0 {
		c.size = defaultSize
	}
	if c.purgeTimeout <= 0 {
		c.purgeTimeout = defaultPurgeTimeout
	}

	purgeTicker := time.NewTicker(c.purgeTimeout)
	defer purgeTicker.Stop()

	deliveries := make([]Delivery, c.size)
	currentSize, attempt := 0, 0

	for {
		select {
		case delivery, open := <-c.consumer.Deliveries():
			if c.close.Get() {
				c.handleBatch(deliveries[0:currentSize])
				return
			}

			if !open {
				continue
			}

			c.wg.Add(1)
			deliveries[currentSize] = Delivery{wg: &c.wg, delivery: delivery}
			currentSize++

			if currentSize%c.size == 0 {
				c.handleBatch(deliveries)
				currentSize = 0
			}
		case err := <-c.consumer.Errors():
			if c.close.Get() {
				c.handleBatch(deliveries[0:currentSize])
				return
			}

			if e, ok := err.(*amqp.Error); ok {
				if e.Code == amqp.NotFound {
					attempt++
					time.Sleep(c.bo.Backoff(attempt))
					c.reConsume(c.consumer)
				}
			}

			if c.errorHandler != nil {
				c.errorHandler(err)
			}
		case <-purgeTicker.C:
			if c.close.Get() {
				c.handleBatch(deliveries[0:currentSize])
				return
			}

			c.handleBatch(deliveries[0:currentSize])
			currentSize = 0
		}
	}
}

func (c *batchConsumer) stop() {
	c.close.Set(true)
}

func (c *batchConsumer) handleBatch(deliveries []Delivery) {
	if len(deliveries) == 0 {
		return
	}
	c.onBatch(deliveries)
}

func (c *batchConsumer) awaitCancel(timeout time.Duration) {
	defer func() {
		c.awaitStopDelivery(timeout)
		c.consumer.Cancel()
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

func (c *batchConsumer) doWait() (waitComplete bool) {
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

func (c *batchConsumer) awaitStopDelivery(timeout time.Duration) {
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
