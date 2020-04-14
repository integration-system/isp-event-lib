package ibm

import (
	"github.com/Azure/go-amqp"
	"sync"
)

type Delivery struct {
	delivery *amqp.Message
	wg       *sync.WaitGroup
	ack      bool
}

func (d *Delivery) GetMessage() *amqp.Message {
	return d.delivery
}

func (d *Delivery) Ack() *Delivery {
	d.ack = true
	return d
}

func (d *Delivery) Nack() *Delivery {
	d.ack = false
	return d
}

func (d *Delivery) Release() error {
	defer d.wg.Done()
	if d.ack {
		return d.delivery.Accept()
	} else {
		return d.delivery.Release()
	}
}