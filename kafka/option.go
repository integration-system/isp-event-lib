package kafka

type Option func(opt *options)

type options struct {
	consumersConfiguration  map[string]ConsumerCfg
	publishersConfiguration map[string]PublisherCfg
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

func defaultOptions() *options {
	return &options{
		consumersConfiguration:  make(map[string]ConsumerCfg),
		publishersConfiguration: make(map[string]PublisherCfg),
	}
}
