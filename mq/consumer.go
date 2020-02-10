package rabbit

import (
	"github.com/integration-system/cony"
	"sync"
	"time"
)

type consumer struct {
	consumer     *cony.Consumer
	callback     func(delivery Delivery)
	errorHandler func(error)
	wg           sync.WaitGroup
	close        chan struct{}
}

func createConsumer(conyConsumer *cony.Consumer, configuration Consumer) *consumer {
	return &consumer{
		consumer:     conyConsumer,
		callback:     configuration.Callback,
		errorHandler: configuration.ErrorHandler,
	}
}

func (c *consumer) start() {
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

func (c *consumer) cancel() {
	if c.close != nil {
		close(c.close)
	}
}

func (c *consumer) wait(timeout time.Duration) {
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
