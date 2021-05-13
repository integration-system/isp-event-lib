package mq

import (
	"sync"
	"time"

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

	closeCh         chan struct{}
	startReturnedCh chan struct{}
	wg              sync.WaitGroup

	reConsume func(consumer *cony.Consumer)
	bo        cony.Backoffer
}

func (c *batchConsumer) start() {
	defer close(c.startReturnedCh)
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
	handleBatch := func() {
		batch := deliveries[0:currentSize]
		if len(batch) == 0 {
			return
		}
		c.onBatch(batch)
		currentSize = 0
	}
	defer handleBatch()

	for {
		// set priority to close channel to avoid random choice in next select
		select {
		case <-c.closeCh:
			return
		default:
		}

		select {
		case <-c.closeCh:
			return
		case delivery, open := <-c.consumer.Deliveries():
			if !open {
				continue
			}

			c.wg.Add(1)
			deliveries[currentSize] = Delivery{wg: &c.wg, delivery: delivery}
			currentSize++

			if currentSize%c.size == 0 {
				handleBatch()
			}
		case err := <-c.consumer.Errors():
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
			handleBatch()
		}
	}
}

func (c *batchConsumer) stop() {
	close(c.closeCh)
}

func (c *batchConsumer) awaitCancel(timeout time.Duration) {
	defer c.consumer.Cancel()

	timeoutCh := time.After(timeout)
	waitWgCh := make(chan struct{})
	go func() {
		c.wg.Wait()
		close(waitWgCh)
	}()

	select {
	case <-c.startReturnedCh:
	case <-timeoutCh:
		return
	}

	select {
	case <-waitWgCh:
	case <-timeoutCh:
	}
}
