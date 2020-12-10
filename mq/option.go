package mq

import (
	"fmt"
	"time"

	"github.com/integration-system/isp-log/stdcodes"

	log "github.com/integration-system/isp-log"
)

const (
	deadLetterArg     = "x-dead-letter-exchange"
	dlxQueueSuffix    = ".DLX"
	dlxExchangeSuffix = ".exchange.DLX"
	defaultTimeout    = 10 * time.Second
)

type Option func(opt *options)

type options struct {
	consumersConfiguration  map[string]ConsumerCfg
	publishersConfiguration map[string]PublisherCfg
	declareConfiguration    DeclareCfg
	timeout                 time.Duration
}

func (o *options) addDeadLetters() {
	for _, consumerCfg := range o.consumersConfiguration {
		common := consumerCfg.getCommon()
		if common.DeadLetter {
			qPtr := findQueue(o.declareConfiguration.Queues, common.QueueName)
			if qPtr == nil {
				log.Warn(stdcodes.InitializingRabbitMqError,
					fmt.Sprintf("can't find %s queue in declared queues", common.QueueName))
				continue
			}
			deadLetterDeclareCfg := qPtr.makeDeadLetterBranch()
			o.declareConfiguration = o.declareConfiguration.Join(deadLetterDeclareCfg)
		}
	}
}

func (q *Queue) makeDeadLetterBranch() DeclareCfg {
	if q.Args == nil {
		q.Args = make(map[string]interface{}, 1)
	} else if _, found := q.Args[deadLetterArg]; found {
		log.Warn(stdcodes.InitializingRabbitMqError, fmt.Sprint("queue ", q.Name, " already configured to DeadLetter by args"))
		return DeclareCfg{}
	}
	dur := true

	q.Args[deadLetterArg] = q.Name + dlxExchangeSuffix
	return DeclareCfg{
		Exchanges: []Exchange{{
			Name:    q.Name + dlxExchangeSuffix,
			Kind:    "fanout",
			Durable: &dur,
		}},
		Queues: []Queue{{
			Name:    q.Name + dlxQueueSuffix,
			Durable: &dur,
		}},
		Bindings: []Binding{{
			QueueName:    q.Name + dlxQueueSuffix,
			ExchangeName: q.Name + dlxExchangeSuffix,
			Key:          q.Name,
			Args:         nil,
		}},
	}
}

func findQueue(queues []Queue, name string) *Queue {
	for i := range queues {
		if queues[i].Name == name {
			return &queues[i]
		}
	}
	return nil
}

func WithConsumers(consumers map[string]ConsumerCfg) Option {
	return func(opt *options) {
		opt.consumersConfiguration = consumers
	}
}

func WithPublishers(publishers map[string]PublisherCfg) Option {
	return func(opt *options) {
		opt.publishersConfiguration = publishers
	}
}

func WithDeclares(declare DeclareCfg) Option {
	return func(opt *options) {
		opt.declareConfiguration = declare
	}
}

func WithAwaitConsumersTimeout(timeout time.Duration) Option {
	return func(opt *options) {
		opt.timeout = timeout
	}
}

func defaultOptionals() *options {
	return &options{
		consumersConfiguration:  make(map[string]ConsumerCfg),
		publishersConfiguration: make(map[string]PublisherCfg),
		timeout:                 defaultTimeout,
	}
}
