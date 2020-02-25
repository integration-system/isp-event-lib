package mq

import (
	"time"

	"github.com/integration-system/cony"
)

type ConsumerCfg interface {
	createConsumer(*cony.Consumer) consumer
	getCommon() CommonConsumerCfg
}

type CommonConsumerCfg struct {
	QueueName     string `valid:"required~Required" schema:"Название очереди"`
	PrefetchCount int    `schema:"Количество предзагруженных сообщений"`
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

func (c ByOneConsumerCfg) createConsumer(conyConsumer *cony.Consumer) consumer {
	return &byOneConsumer{
		consumer:     conyConsumer,
		callback:     c.Callback,
		errorHandler: c.ErrorHandler,
		close:        make(chan struct{}),
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

func (c BatchingConsumerCfg) createConsumer(conyConsumer *cony.Consumer) consumer {
	return &batchConsumer{
		consumer:     conyConsumer,
		onBatch:      c.Callback,
		purgeTimeout: c.PurgeTimeoutMs * time.Millisecond,
		size:         c.BatchSize,
		errorHandler: c.ErrorHandler,
		close:        make(chan struct{}),
	}
}

type PublisherCfg struct {
	Exchange     string `schema:"Название точки маршрутизации"`
	ExchangeType string `schema:"Тип точки маршрутизации,(direct, funout)"`
	RoutingKey   string `valid:"required~Required" schema:"Ключ маршрутизации,для публикации напрямую в очередь, указывается название очереди"`
	QueueName    string `schema:"Название очереди"`
	Declare      bool   `schema:"Автоматическое объявление очереди,точки маршрутизации,привязки"`
}
