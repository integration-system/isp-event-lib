package mq

import (
	"github.com/integration-system/isp-lib/v2/atomic"
	"sync"
	"time"

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
	close        *atomic.AtomicBool

	wg sync.WaitGroup
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
	currentSize := 0

	for {
		select {
		case delivery, open := <-c.consumer.Deliveries():
			if c.close.Get() {
				c.handleBatch(deliveries[0:currentSize])
				currentSize = 0
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
				currentSize = 0
				return
			}

			if c.errorHandler != nil {
				c.errorHandler(err)
			}
		case <-purgeTicker.C:
			if c.close.Get() {
				c.handleBatch(deliveries[0:currentSize])
				currentSize = 0
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
		r := recover() //panic("sync: WaitGroup is reused before previous Wait has returned")
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
