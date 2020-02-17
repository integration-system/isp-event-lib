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
		publishers:                 make(map[string]*publisher),
		oldPublishersConfiguration: make(map[string]Publisher),

		consumers:                 make(map[string]*consumer),
		oldConsumersConfiguration: make(map[string]Consumer),

		lastConfig: structure.RabbitConfig{},
		lock:       sync.Mutex{},
	}
}

type RabbitMqClient struct {
	cli        *cony.Client
	lastConfig structure.RabbitConfig

	publishers                 map[string]*publisher
	oldPublishersConfiguration map[string]Publisher

	consumers                 map[string]*consumer
	oldConsumersConfiguration map[string]Consumer

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

	newPublishers, newPublishersConfiguration := make(map[string]*publisher), make(map[string]Publisher)
	newPublishersConfiguration = options.publishersConfig
	for key, publisher := range r.publishers {
		newConfiguration, found := options.publishersConfig[key]
		if found {
			if cmp.Equal(r.oldPublishersConfiguration[key], newConfiguration) {
				newPublishers[key] = publisher
			} else {
				publisher.cancel()
				newPublishers[key] = r.publish(newConfiguration)
			}
			delete(options.publishersConfig, key)
		} else {
			publisher.cancel()
		}
	}
	for key, newConfiguration := range options.publishersConfig {
		newPublishers[key] = r.publish(newConfiguration)
	}

	newConsumers, newConsumersConfiguration := make(map[string]*consumer), make(map[string]Consumer)
	awaitConsumer := make([]*consumer, 0)
	newConsumersConfiguration = options.consumersConfig
	for key, consumer := range r.consumers {
		newConfiguration, found := options.consumersConfig[key]
		if found {
			if cmp.Equal(r.oldConsumersConfiguration[key], newConfiguration) {
				newConsumers[key] = consumer
			} else {
				consumer.cancel()
				awaitConsumer = append(awaitConsumer, consumer)
				newConsumers[key] = r.consume(newConfiguration)
			}
			delete(options.consumersConfig, key)
		} else {
			consumer.cancel()
			awaitConsumer = append(awaitConsumer, consumer)
		}
	}
	for _, consumer := range awaitConsumer {
		consumer.wait(r.timeout)
	}
	for key, newConfiguration := range options.consumersConfig {
		newConsumers[key] = r.consume(newConfiguration)
	}

	r.consumers, r.oldConsumersConfiguration = newConsumers, newConsumersConfiguration
	r.publishers, r.oldPublishersConfiguration = newPublishers, newPublishersConfiguration
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

func (r *RabbitMqClient) close() {
	if len(r.publishers) != 0 {
		for _, publisher := range r.publishers {
			publisher.cancel()
		}
	}
	if len(r.consumers) != 0 {
		for _, consumer := range r.consumers {
			consumer.cancel()
		}
		for _, consumer := range r.consumers {
			consumer.wait(r.timeout)
		}
	}
	if r.cli != nil {
		r.cli.Close()
		r.cli = nil
	}
	r.lastConfig = structure.RabbitConfig{}
	r.publishers = make(map[string]*publisher)
	r.oldPublishersConfiguration = make(map[string]Publisher)
	r.consumers = make(map[string]*consumer)
	r.oldConsumersConfiguration = make(map[string]Consumer)
}

func (r *RabbitMqClient) consume(consumerConfig Consumer) *consumer {
	opts := make([]cony.ConsumerOpt, 0)
	if consumerConfig.PrefetchCount > 0 {
		opts = append(opts, cony.Qos(consumerConfig.PrefetchCount))
	}
	conyConsumer := cony.NewConsumer(&cony.Queue{Name: consumerConfig.QueueName}, opts...)
	r.cli.Consume(conyConsumer)
	newConsumer := createConsumer(conyConsumer, consumerConfig)
	go newConsumer.start()
	return newConsumer
}

func (r *RabbitMqClient) publish(publisherConfig Publisher) *publisher {
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
