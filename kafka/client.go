package kafka

import (
	"sync"

	"github.com/google/go-cmp/cmp"
	"github.com/integration-system/isp-event-lib/kafka/structure"
)

type KafkaClient struct {
	lastConfig structure.KafkaConfig

	publishers              map[string]*publisher
	publishersConfiguration map[string]PublisherCfg

	consumers              map[string]*consumer
	consumersConfiguration map[string]ConsumerCfg

	lock sync.Mutex
}

func NewKafkaClient() *KafkaClient {
	return &KafkaClient{
		publishers:              make(map[string]*publisher),
		publishersConfiguration: make(map[string]PublisherCfg),

		consumers:              make(map[string]*consumer),
		consumersConfiguration: make(map[string]ConsumerCfg),

		lastConfig: structure.KafkaConfig{},
		lock:       sync.Mutex{},
	}
}

func (r *KafkaClient) ReceiveConfiguration(kafkaConfig structure.KafkaConfig, opts ...Option) {
	r.lock.Lock()
	defer r.lock.Unlock()

	if !cmp.Equal(r.lastConfig, kafkaConfig) {
		r.Close()
		// todo try to connect to kafka
		r.lastConfig = kafkaConfig
	}

	options := defaultOptionals()
	for _, option := range opts {
		option(options)
	}

	newPublishers, oldPublishers := r.newPublishers(options.publishersConfiguration)
	newConsumers, oldConsumers := r.newConsumers(options.consumersConfiguration)
	for _, c := range oldConsumers {
		c.close()
	}
	for _, p := range oldPublishers {
		p.close()
	}

	r.consumers, r.consumersConfiguration = newConsumers, options.consumersConfiguration
	r.publishers, r.publishersConfiguration = newPublishers, options.publishersConfiguration
}

func (r *KafkaClient) GetPublisher(name string) *publisher {
	return r.publishers[name]
}

func (r *KafkaClient) newPublishers(config map[string]PublisherCfg) (map[string]*publisher, map[string]*publisher) {
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
			newPublishers[key] = r.makePublisher(newConfiguration, key)
		}
	}
	return newPublishers, oldPublisher
}

func (r *KafkaClient) newConsumers(config map[string]ConsumerCfg) (map[string]*consumer, map[string]*consumer) {
	newConsumers, oldConsumer := make(map[string]*consumer), make(map[string]*consumer)
	for key, consumer := range r.consumers {
		newConfiguration, found := config[key]
		if found && cmp.Equal(r.consumersConfiguration[key], newConfiguration) {
			newConsumers[key] = consumer
		} else {
			consumer.reader.Close()
			oldConsumer[key] = consumer
		}
	}
	for key, newConfiguration := range config {
		if _, found := newConsumers[key]; !found {
			newConsumers[key] = r.makeConsumer(newConfiguration, key)
		}
	}
	return newConsumers, oldConsumer
}

func (r *KafkaClient) makePublisher(publisherCfg PublisherCfg, namePublisher string) *publisher {
	return createPublisher(&publisherCfg, r.lastConfig, namePublisher)
}

func (r *KafkaClient) makeConsumer(consumerCfg ConsumerCfg, nameConsumer string) *consumer {
	newConsumer := createConsumer(&consumerCfg, r.lastConfig, nameConsumer)
	go newConsumer.start()
	return newConsumer
}

func (r *KafkaClient) Close() {
	if len(r.publishers) != 0 {
		for _, publisher := range r.publishers {
			publisher.close()
		}
	}
	if len(r.consumers) != 0 {
		for _, consumer := range r.consumers {
			consumer.close()
		}
	}
	r.lastConfig = structure.KafkaConfig{}
	r.publishers = make(map[string]*publisher)
	r.publishersConfiguration = make(map[string]PublisherCfg)
	r.consumers = make(map[string]*consumer)
	r.consumersConfiguration = make(map[string]ConsumerCfg)
}
