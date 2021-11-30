package ibm

import (
	"time"

	"github.com/integration-system/go-amqp"
	"github.com/integration-system/isp-event-lib/mq"
)

const defaultTimeout = 10 * time.Second

type Option func(opt *options)

type options struct {
	consumersConfiguration  map[string]ConsumerCfg
	publishersConfiguration map[string]mq.PublisherCfg
	connContainerId         string
	timeout                 time.Duration
	connOptions             []amqp.ConnOption
}

func WithConnContainerID(id string) Option {
	return func(opt *options) {
		opt.connContainerId = id
	}
}

func WithConnOptions(opts ...amqp.ConnOption) Option {
	return func(opt *options) {
		opt.connOptions = opts
	}
}

func WithConsumers(consumers map[string]ConsumerCfg) Option {
	return func(opt *options) {
		opt.consumersConfiguration = consumers
	}
}

func WithPublishers(publishers map[string]mq.PublisherCfg) Option {
	return func(opt *options) {
		opt.publishersConfiguration = publishers
	}
}

// timeout for await consumers, publish messages and close links and sessions
func WithDefaultTimeout(timeout time.Duration) Option {
	return func(opt *options) {
		opt.timeout = timeout
	}
}

func defaultOptions() *options {
	return &options{
		connContainerId:         "",
		consumersConfiguration:  make(map[string]ConsumerCfg),
		publishersConfiguration: make(map[string]mq.PublisherCfg),
		timeout:                 defaultTimeout,
	}
}
