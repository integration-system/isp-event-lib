package mq

import (
	"time"

	"github.com/integration-system/cony"
	"github.com/integration-system/isp-lib/v2/atomic"
)

type ConsumerCfg interface {
	createConsumer(*cony.Consumer, func(*cony.Consumer)) consumer
	getCommon() CommonConsumerCfg
}

var _ ConsumerCfg = (*ByOneConsumerCfg)(nil)

type ByOneConsumerCfg struct {
	CommonConsumerCfg
	Callback     func(delivery Delivery) `json:"-" schema:"Функция обработки сообщения из очереди"`
	ErrorHandler func(error)             `json:"-" schema:"Функция обработки ошибки при чтении очереди"`
}

func (c ByOneConsumerCfg) getCommon() CommonConsumerCfg {
	return c.CommonConsumerCfg
}

func (c ByOneConsumerCfg) createConsumer(conyConsumer *cony.Consumer, reConsume func(*cony.Consumer)) consumer {
	return &byOneConsumer{
		consumer:     conyConsumer,
		callback:     c.Callback,
		errorHandler: c.ErrorHandler,
		close:        atomic.NewAtomicBool(false),
		reConsume:    reConsume,
		bo:           cony.DefaultBackoff,
	}
}

var _ ConsumerCfg = (*BatchingConsumerCfg)(nil)

type BatchingConsumerCfg struct {
	CommonConsumerCfg
	BatchSize      int                    `schema:"Количество сообщений в батче для обработки"`
	PurgeTimeoutMs time.Duration          `schema:"Таймаут в миллисекундах через который вызывается Callback в случае неактивности очереди"`
	Callback       func(batch []Delivery) `json:"-" schema:"Функция обработки сообщений из очереди"`
	ErrorHandler   func(error)            `json:"-" schema:"Функция обработки ошибки при чтении очереди"`
}

func (c BatchingConsumerCfg) getCommon() CommonConsumerCfg {
	return c.CommonConsumerCfg
}

func (c BatchingConsumerCfg) createConsumer(conyConsumer *cony.Consumer, reConsume func(*cony.Consumer)) consumer {
	return &batchConsumer{
		consumer:     conyConsumer,
		onBatch:      c.Callback,
		purgeTimeout: c.PurgeTimeoutMs * time.Millisecond,
		size:         c.BatchSize,
		errorHandler: c.ErrorHandler,
		closed:       atomic.NewAtomicBool(false),
		closeCh:      make(chan struct{}),
		reConsume:    reConsume,
		bo:           cony.DefaultBackoff,
	}
}
