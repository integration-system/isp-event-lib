package mq

import (
	"time"
)

const (
	defaultTimeout = 10 * time.Second
)

type Option func(opt *options)

type options struct {
	consumersConfiguration  map[string]ConsumerCfg
	publishersConfiguration map[string]PublisherCfg
	declareConfiguration    DeclareCfg
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

func defaultOptions() *options {
	return &options{
		consumersConfiguration:  make(map[string]ConsumerCfg),
		publishersConfiguration: make(map[string]PublisherCfg),
		timeout:                 defaultTimeout,
	}
}
