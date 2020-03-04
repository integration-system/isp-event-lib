package mq

import (
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/integration-system/cony"
	"github.com/integration-system/isp-lib/v2/structure"
	log "github.com/integration-system/isp-log"
	"github.com/integration-system/isp-log/stdcodes"
)

func NewRabbitClient() *RabbitMqClient {
	return &RabbitMqClient{
		publishers:              make(map[string]*publisher),
		publishersConfiguration: make(map[string]PublisherCfg),

		consumers:              make(map[string]consumer),
		consumersConfiguration: make(map[string]ConsumerCfg),

		lastConfig: structure.RabbitConfig{},
		lock:       sync.Mutex{},
	}
}

type RabbitMqClient struct {
	cli        *cony.Client
	lastConfig structure.RabbitConfig

	publishers              map[string]*publisher
	publishersConfiguration map[string]PublisherCfg

	consumers              map[string]consumer
	consumersConfiguration map[string]ConsumerCfg

	lock    sync.Mutex
	timeout time.Duration
}

func (r *RabbitMqClient) ReceiveConfiguration(rabbitConfig structure.RabbitConfig, opts ...Option) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !cmp.Equal(r.lastConfig, rabbitConfig) {
		r.close()
		cli := cony.NewClient(
			cony.URL(rabbitConfig.GetUri()),
			cony.Backoff(cony.DefaultBackoff),
		)
		err := cli.Ping(time.Second)
		if err != nil {
			log.Fatal(stdcodes.InitializingRabbitMqError, err)
		}
		r.cli = cli
		r.lastConfig = rabbitConfig
	}
	if r.cli == nil {
		return
	}

	options := defaultOptionals()
	for _, option := range opts {
		option(options)
	}
	r.timeout = options.timeout

	newPublishers, oldPublishers := r.newPublishers(options.publishersConfiguration)
	newConsumers, oldConsumers := r.newConsumers(options.consumersConfiguration)
	for _, c := range oldConsumers {
		c.awaitCancel(r.timeout)
	}
	for _, p := range oldPublishers {
		p.cancel()
	}

	r.consumers, r.consumersConfiguration = newConsumers, options.consumersConfiguration
	r.publishers, r.publishersConfiguration = newPublishers, options.publishersConfiguration
	go r.clientErrorsHandler()
}

func (r *RabbitMqClient) GetPublisher(name string) *publisher {
	return r.publishers[name]
}

func (r *RabbitMqClient) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.close()
}

func (r *RabbitMqClient) newPublishers(config map[string]PublisherCfg) (map[string]*publisher, map[string]*publisher) {
	newPublishers, oldPublisher := make(map[string]*publisher), make(map[string]*publisher)
	for key, publisher := range r.publishers {
		newConfiguration, found := config[key]
		if found && cmp.Equal(r.publishersConfiguration[key], newConfiguration) {
			newPublishers[key] = publisher
		} else {
			oldPublisher[key] = publisher
		}
	}
	for key, newConfiguration := range config {
		if _, found := newPublishers[key]; !found {
			newPublishers[key] = r.makePublisher(newConfiguration)
		}
	}
	return newPublishers, oldPublisher
}

func (r *RabbitMqClient) newConsumers(config map[string]ConsumerCfg) (map[string]consumer, map[string]consumer) {
	newConsumers, oldConsumer := make(map[string]consumer), make(map[string]consumer)
	for key, consumer := range r.consumers {
		newConfiguration, found := config[key]
		if found && cmp.Equal(r.consumersConfiguration[key], newConfiguration) {
			newConsumers[key] = consumer
		} else {
			consumer.stop()
			oldConsumer[key] = consumer
		}
	}
	for key, newConfiguration := range config {
		if _, found := newConsumers[key]; !found {
			newConsumers[key] = r.makeConsumer(newConfiguration)
		}
	}
	return newConsumers, oldConsumer
}

func (r *RabbitMqClient) close() {
	if len(r.publishers) != 0 {
		for _, publisher := range r.publishers {
			publisher.cancel()
		}
	}
	if len(r.consumers) != 0 {
		for _, consumer := range r.consumers {
			consumer.stop()
		}
		for _, consumer := range r.consumers {
			consumer.awaitCancel(r.timeout)
		}
	}
	if r.cli != nil {
		r.cli.Close()
		r.cli = nil
	}
	r.lastConfig = structure.RabbitConfig{}
	r.publishers = make(map[string]*publisher)
	r.publishersConfiguration = make(map[string]PublisherCfg)
	r.consumers = make(map[string]consumer)
	r.consumersConfiguration = make(map[string]ConsumerCfg)
}

func (r *RabbitMqClient) makeConsumer(consumerConfig ConsumerCfg) consumer {
	opts := make([]cony.ConsumerOpt, 0)
	cfg := consumerConfig.getCommon()
	if cfg.PrefetchCount > 0 {
		opts = append(opts, cony.Qos(cfg.PrefetchCount))
	}
	conyConsumer := cony.NewConsumer(&cony.Queue{Name: cfg.QueueName}, opts...)
	r.cli.Consume(conyConsumer)
	newConsumer := consumerConfig.createConsumer(conyConsumer, r.cli.Consume)
	go newConsumer.start()
	return newConsumer
}

func (r *RabbitMqClient) makePublisher(publisherConfig PublisherCfg) *publisher {
	if publisherConfig.Declare {
		declarations := make([]cony.Declaration, 0)
		var (
			exchange *cony.Exchange
			queue    *cony.Queue
		)
		if publisherConfig.Exchange != "" {
			exchange = &cony.Exchange{
				Name:       publisherConfig.Exchange,
				Durable:    true,
				AutoDelete: false,
				Kind:       publisherConfig.ExchangeType,
			}
			declarations = append(declarations, cony.DeclareExchange(*exchange))
		}
		if publisherConfig.QueueName != "" {
			queue = &cony.Queue{
				Name:       publisherConfig.QueueName,
				Durable:    true,
				AutoDelete: false,
				Exclusive:  false,
			}
			declarations = append(declarations, cony.DeclareQueue(queue))
		}
		if publisherConfig.RoutingKey != "" && queue != nil && exchange != nil {
			bind := cony.Binding{
				Queue:    queue,
				Exchange: *exchange,
				Key:      publisherConfig.RoutingKey,
			}
			declarations = append(declarations, cony.DeclareBinding(bind))
		}
		r.cli.Declare(declarations)
	}
	newPublisher := cony.NewPublisher(publisherConfig.Exchange, publisherConfig.RoutingKey)
	r.cli.Publish(newPublisher)
	return createPublisher(newPublisher)
}

func (r *RabbitMqClient) clientErrorsHandler() {
	for r.cli.Loop() {
		select {
		case err := <-r.cli.Errors():
			if err != nil {
				log.WithMetadata(map[string]interface{}{
					"message": err,
				}).Warnf(stdcodes.RabbitMqClientError, "rabbitmq error")
			}
		case blocked := <-r.cli.Blocking():
			if blocked.Active {
				log.WithMetadata(map[string]interface{}{
					"message": blocked.Reason,
				}).Warnf(stdcodes.RabbitMqBlockedConnection, "blocked")
			}
		}
	}
}
