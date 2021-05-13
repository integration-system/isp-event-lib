package ibm

import (
	"context"

	"github.com/integration-system/go-amqp"
	"github.com/integration-system/isp-event-lib/mq"
	"github.com/integration-system/isp-lib/v2/atomic"
)

type ConsumerCfg interface {
	createConsumer(*amqp.Receiver) consumer
	getCommon() mq.CommonConsumerCfg
}

var _ ConsumerCfg = (*ByOneConsumerCfg)(nil)

type ByOneConsumerCfg struct {
	mq.CommonConsumerCfg
	Callback     func(delivery Delivery) `json:"-" schema:"Функция обработки сообщения из очереди"`
	ErrorHandler func(error)             `json:"-" schema:"Функция обработки ошибки при чтении очереди"`
}

func (c ByOneConsumerCfg) getCommon() mq.CommonConsumerCfg {
	return c.CommonConsumerCfg
}

func (c ByOneConsumerCfg) createConsumer(receiver *amqp.Receiver) consumer {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &byOneConsumer{
		ctx:             ctx,
		cancelFunc:      cancelFunc,
		consumer:        receiver,
		callback:        c.Callback,
		errorHandler:    c.ErrorHandler,
		close:           atomic.NewAtomicBool(false),
		startReturnedCh: make(chan struct{}),
	}
}
