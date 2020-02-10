package mq

import (
	"github.com/integration-system/cony"
	"github.com/streadway/amqp"
)

type publisher struct {
	publisher *cony.Publisher
}

func createPublisher(conyPublisher *cony.Publisher) *publisher {
	return &publisher{
		publisher: conyPublisher,
	}
}

func (p *publisher) Publish(publishing amqp.Publishing) error {
	return p.publisher.Publish(publishing)
}

func (p *publisher) PublishWithRoutingKey(publishing amqp.Publishing, key string) error {
	return p.publisher.PublishWithRoutingKey(publishing, key)
}

func (p *publisher) cancel() {
	p.publisher.Cancel()
}
