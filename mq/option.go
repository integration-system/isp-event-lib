package mq

import "time"

const defaultTimeout = 10 * time.Second

type Option func(opt *options)

type options struct {
	consumersConfiguration  map[string]ConsumerCfg
	publishersConfiguration map[string]PublisherCfg
	timeout                 time.Duration
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
