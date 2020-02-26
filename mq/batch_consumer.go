package mq

import (
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

	wg    sync.WaitGroup
	close chan struct{}
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
			if !open {
				return
			}

			c.wg.Add(1)
			deliveries[currentSize] = Delivery{wg: &c.wg, delivery: delivery}
			currentSize++

			if currentSize%c.size == 0 {
				c.handleBatch(deliveries)
				currentSize = 0
			}

		case err := <-c.consumer.Errors():
			if c.errorHandler != nil {
				c.errorHandler(err)
			}
		case <-purgeTicker.C:
			c.handleBatch(deliveries[0:currentSize])
			currentSize = 0
		case <-c.close:
			c.handleBatch(deliveries[0:currentSize])
			currentSize = 0
			return
		}
	}
}

func (c *batchConsumer) stop() {
	close(c.close)
}

func (c *batchConsumer) handleBatch(deliveries []Delivery) {
	if len(deliveries) == 0 {
		return
	}
	c.onBatch(deliveries)
}

func (c *batchConsumer) wait(timeout time.Duration) {
	defer c.consumer.Cancel()
	wait := make(chan struct{})
	go func() {
		defer close(wait)
		c.wg.Wait()
	}()

	select {
	case <-time.After(timeout):
		return
	case <-wait:
		return
	}
}
