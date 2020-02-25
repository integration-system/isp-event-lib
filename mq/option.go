package mq

import "time"

const defaultTimeout = 10 * time.Second

type Option func(opt *options)

type options struct {
	consumersConfig  map[string]ConsumerCfg
	publishersConfig map[string]Publisher
	timeout          time.Duration
}

func WithConsumers(consumers map[string]ConsumerCfg) Option {
	return func(opt *options) {
		opt.consumersConfig = consumers
	}
}

func WithPublishers(publishers map[string]Publisher) Option {
	return func(opt *options) {
		opt.publishersConfig = publishers
	}
}

func WithAwaitConsumersTimeout(timeout time.Duration) Option {
	return func(opt *options) {
		opt.timeout = timeout
	}
}

func defaultOptionals() *options {
	return &options{
		consumersConfig:  make(map[string]ConsumerCfg),
		publishersConfig: make(map[string]Publisher),
		timeout:          defaultTimeout,
	}
}
