package mq

import (
	"sync"
	"time"

	"github.com/integration-system/cony"
	"github.com/streadway/amqp"
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

	closeCh         chan struct{}
	startReturnedCh chan struct{}
	wg              sync.WaitGroup

	reConsume func(consumer *cony.Consumer)
	bo        cony.Backoffer
}

func (c *byOneConsumer) start() {
	defer close(c.startReturnedCh)
	attempt := 0

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
		case delivery := <-c.consumer.Deliveries():
			c.wg.Add(1)
			c.callback(Delivery{wg: &c.wg, delivery: delivery})
		case err := <-c.consumer.Errors():
			if c.errorHandler != nil {
				c.errorHandler(err)
			}
			if e, ok := err.(*amqp.Error); ok {
				if e.Code == amqp.NotFound {
					attempt++
					time.Sleep(c.bo.Backoff(attempt))
					c.reConsume(c.consumer)
				}
			}
		}
	}
}

func (c *byOneConsumer) stop() {
	close(c.closeCh)
}

func (c *byOneConsumer) awaitCancel(timeout time.Duration) {
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
