package kafka

import (
	"fmt"
	"sync"

	"github.com/google/go-cmp/cmp"
	"github.com/integration-system/isp-lib/v2/structure"
	log "github.com/integration-system/isp-log"
	"github.com/segmentio/kafka-go"
)

type Client struct {
	lastConfig structure.KafkaConfig
	addresses  []string

	publishers              map[string]*publisher
	publishersConfiguration map[string]PublisherCfg

	consumers              map[string]*consumer
	consumersConfiguration map[string]ConsumerCfg

	validBrokerAddress string
	lock               sync.Mutex
}

func NewClient() *Client {
	return &Client{
		publishers:              make(map[string]*publisher),
		publishersConfiguration: make(map[string]PublisherCfg),

		consumers:              make(map[string]*consumer),
		consumersConfiguration: make(map[string]ConsumerCfg),

		lastConfig: structure.KafkaConfig{},
		lock:       sync.Mutex{},
	}
}

func (r *Client) ReceiveConfiguration(kafkaConfig structure.KafkaConfig, opts ...Option) {
	r.lock.Lock()
	defer r.lock.Unlock()

	var err error
	if !cmp.Equal(r.lastConfig, kafkaConfig) {
		r.Close()
		r.validBrokerAddress, err = tryDial(kafkaConfig.AddressCfgs)
		if err != nil {
			log.Fatal(0, err)
		}
		r.lastConfig = kafkaConfig
		r.addresses = getAddresses(kafkaConfig)
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

func (r *Client) GetPublisher(name string) *publisher {
	return r.publishers[name]
}

func (r *Client) newPublishers(config map[string]PublisherCfg) (map[string]*publisher, map[string]*publisher) {
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

func (r *Client) newConsumers(config map[string]ConsumerCfg) (map[string]*consumer, map[string]*consumer) {
	newConsumers, oldConsumer := make(map[string]*consumer), make(map[string]*consumer)
	for key, consumer := range r.consumers {
		newConfiguration, found := config[key]
		if found && cmp.Equal(r.consumersConfiguration[key], newConfiguration) {
			newConsumers[key] = consumer
		} else {
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

func (r *Client) makePublisher(publisherCfg PublisherCfg, namePublisher string) *publisher {
	publisher := &publisher{writer: newWriter(publisherCfg)}
	publisher.writer.Addr = kafka.TCP(r.validBrokerAddress)
	publisher.writer.Transport = &kafka.Transport{SASL: getSASL(r.lastConfig.KafkaAuth)}
	publisher.writer.ErrorLogger = logger{loggerPrefix: fmt.Sprintf("[publisher: %s]", namePublisher)}
	return publisher
}

func (r *Client) makeConsumer(consumerCfg ConsumerCfg, nameConsumer string) *consumer {
	newConsumer := &consumer{name: nameConsumer}
	newConsumer.createReader(consumerCfg, r.addresses, r.lastConfig.KafkaAuth)
	newConsumer.config = consumerCfg

	go newConsumer.start()
	return newConsumer
}

func (r *Client) Close() {
	for _, publisher := range r.publishers {
		publisher.close()
	}
	for _, consumer := range r.consumers {
		consumer.close()
	}
	r.lastConfig = structure.KafkaConfig{}
	r.publishers = make(map[string]*publisher)
	r.publishersConfiguration = make(map[string]PublisherCfg)
	r.consumers = make(map[string]*consumer)
	r.consumersConfiguration = make(map[string]ConsumerCfg)
	r.addresses = nil
	r.validBrokerAddress = ""
}
