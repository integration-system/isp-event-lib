package mq

import (
	"sync"
	"time"

	"github.com/integration-system/cony"
)

type consumer interface {
	start()
	stop()
	wait(timeout time.Duration)
}

var _ consumer = (*byOneConsumer)(nil)

type byOneConsumer struct {
	consumer     *cony.Consumer
	callback     func(delivery Delivery)
	errorHandler func(error)
	wg           sync.WaitGroup
	close        chan struct{}
}

func (c *byOneConsumer) start() {
	for {
		select {
		case delivery := <-c.consumer.Deliveries():
			c.wg.Add(1)
			c.callback(Delivery{wg: &c.wg, delivery: delivery})
		case err := <-c.consumer.Errors():
			if c.errorHandler != nil {
				c.errorHandler(err)
			}
		case <-c.close:
			return
		}
	}
}

func (c *byOneConsumer) stop() {
	close(c.close)
}

func (c *byOneConsumer) wait(timeout time.Duration) {
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
