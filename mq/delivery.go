package mq

import (
	"github.com/streadway/amqp"
	"sync"
)

type Delivery struct {
	delivery amqp.Delivery
	wg       *sync.WaitGroup
	ack      bool
	requeue  bool
}

func (d *Delivery) GetMessage() []byte {
	return d.delivery.Body
}

func (d *Delivery) Ack() *Delivery {
	d.ack = true
	return d
}

func (d *Delivery) Nack(requeue bool) *Delivery {
	d.ack = false
	d.requeue = requeue
	return d
}

func (d *Delivery) Release() error {
	defer d.wg.Done()
	if d.ack {
		return d.delivery.Ack(false)
	} else {
		return d.delivery.Nack(false, d.requeue)
	}
}
