package ibm

import (
	"context"
	"time"

	"github.com/integration-system/go-amqp"
	"github.com/integration-system/isp-event-lib/mq"
)

type publisher struct {
	publisher *amqp.Sender
	queue     string
	timeout   time.Duration
}

func createPublisher(conyPublisher *amqp.Sender, cfg mq.PublisherCfg, timeout time.Duration) *publisher {
	return &publisher{
		publisher: conyPublisher,
		timeout:   timeout,
		queue:     cfg.RoutingKey,
	}
}

func (p *publisher) Publish(publishing *amqp.Message) error {
	ctx, cancelFunc := context.WithTimeout(context.Background(), p.timeout)
	defer cancelFunc()
	if publishing.Properties == nil {
		publishing.Properties = &amqp.MessageProperties{
			To: p.queue,
		}
	} else {
		publishing.Properties.To = p.queue
	}
	return p.publisher.Send(ctx, publishing)
}

func (p *publisher) cancel() {
	ctx, cancelFunc := context.WithTimeout(context.Background(), p.timeout)
	defer cancelFunc()
	_ = p.publisher.Close(ctx)
}
