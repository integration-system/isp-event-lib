package kafka

import (
	"crypto/tls"
	"fmt"
	"sync"

	"github.com/google/go-cmp/cmp"
	log "github.com/integration-system/isp-log"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
)

type Client struct {
	lastConfig Config
	addresses  []string

	publishers              map[string]*publisher
	publishersConfiguration map[string]PublisherCfg

	consumers              map[string]*consumer
	consumersConfiguration map[string]ConsumerCfg

	saslMechanism *sasl.Mechanism
	tlsConfig     *tls.Config

	validBrokerAddress string
	lock               sync.Mutex
}

func NewClient() *Client {
	return &Client{
		publishers:              make(map[string]*publisher),
		publishersConfiguration: make(map[string]PublisherCfg),

		consumers:              make(map[string]*consumer),
		consumersConfiguration: make(map[string]ConsumerCfg),

		lastConfig: Config{},
		lock:       sync.Mutex{},
	}
}

func (c *Client) ReceiveConfiguration(kafkaConfig Config, opts ...Option) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var err error
	if !cmp.Equal(c.lastConfig, kafkaConfig) {
		c.Close()
		c.validBrokerAddress, err = tryDial(kafkaConfig.AddressCfgs)
		if err != nil {
			log.Fatal(0, err)
		}
		c.lastConfig = kafkaConfig
		c.addresses = getAddresses(kafkaConfig)

		c.saslMechanism = getSASL(kafkaConfig.KafkaAuth)
		c.tlsConfig, err = getTlsConfig(kafkaConfig.TlsConfig)
		if err != nil {
			log.Fatalf(0, "received tls configuration can't be used:", err)
		}
	}

	options := defaultOptions()
	for _, option := range opts {
		option(options)
	}

	newPublishers, oldPublishers := c.newPublishers(options.publishersConfiguration)
	newConsumers, oldConsumers := c.newConsumers(options.consumersConfiguration)
	for _, c := range oldConsumers {
		c.close()
	}
	for _, p := range oldPublishers {
		p.close()
	}

	c.consumers, c.consumersConfiguration = newConsumers, options.consumersConfiguration
	c.publishers, c.publishersConfiguration = newPublishers, options.publishersConfiguration
}

func (c *Client) GetPublisher(name string) *publisher {
	return c.publishers[name]
}

func (c *Client) newPublishers(config map[string]PublisherCfg) (map[string]*publisher, map[string]*publisher) {
	newPublishers, oldPublisher := make(map[string]*publisher), make(map[string]*publisher)
	for key, publisher := range c.publishers {
		newConfiguration, found := config[key]
		if found && cmp.Equal(c.publishersConfiguration[key], newConfiguration) {
			newPublishers[key] = publisher
		} else {
			oldPublisher[key] = publisher
		}
	}
	for key, newConfiguration := range config {
		if _, found := newPublishers[key]; !found {
			newPublishers[key] = c.makePublisher(newConfiguration, key)
		}
	}
	return newPublishers, oldPublisher
}

func (c *Client) newConsumers(config map[string]ConsumerCfg) (map[string]*consumer, map[string]*consumer) {
	newConsumers, oldConsumer := make(map[string]*consumer), make(map[string]*consumer)
	for key, consumer := range c.consumers {
		newConfiguration, found := config[key]
		if found && cmp.Equal(c.consumersConfiguration[key], newConfiguration) {
			newConsumers[key] = consumer
		} else {
			oldConsumer[key] = consumer
		}
	}
	for key, newConfiguration := range config {
		if _, found := newConsumers[key]; !found {
			newConsumers[key] = c.makeConsumer(newConfiguration, key)
		}
	}
	return newConsumers, oldConsumer
}

func (c *Client) makePublisher(publisherCfg PublisherCfg, namePublisher string) *publisher {
	publisher := &publisher{writer: newWriter(publisherCfg)}
	publisher.writer.Addr = kafka.TCP(c.validBrokerAddress)
	if transport := c.newSecureTransport(); transport != nil {
		publisher.writer.Transport = transport
	}
	publisher.writer.ErrorLogger = logger{loggerPrefix: fmt.Sprintf("[publisher: %s]", namePublisher)}
	return publisher
}

func (c *Client) makeConsumer(consumerCfg ConsumerCfg, nameConsumer string) *consumer {
	newConsumer := &consumer{name: nameConsumer}

	readerCfg := makeReaderCfg(consumerCfg)
	readerCfg.Brokers = c.addresses
	readerCfg.Topic = consumerCfg.TopicName
	readerCfg.GroupID = consumerCfg.GroupID
	readerCfg.QueueCapacity = consumerCfg.PrefetchCount
	readerCfg.Dialer = c.newSecureDialer()
	if consumerCfg.Callback == nil {
		log.Fatalf(0, "no callback was set to ConsumerCfg")
	}
	if consumerCfg.ErrorHandler == nil {
		log.Fatalf(0, "no ErrorHandler was set to ConsumerCfg")
	}
	readerCfg.StartOffset = kafka.LastOffset
	readerCfg.ErrorLogger = logger{loggerPrefix: fmt.Sprintf("[consumer %s]", nameConsumer)}

	newConsumer.callback = consumerCfg.Callback
	newConsumer.errorHandler = consumerCfg.ErrorHandler
	newConsumer.reader = kafka.NewReader(readerCfg)

	newConsumer.config = consumerCfg

	go newConsumer.start()
	return newConsumer
}

func (c *Client) newSecureTransport() *kafka.Transport {
	if c.saslMechanism == nil && c.tlsConfig == nil {
		return nil
	}
	transport := kafka.Transport{}
	if c.saslMechanism != nil {
		transport.SASL = *c.saslMechanism
	}
	transport.TLS = c.tlsConfig
	return &transport
}

func (c *Client) newSecureDialer() *kafka.Dialer {
	if c.saslMechanism == nil && c.tlsConfig == nil {
		return nil
	}
	dialer := kafka.Dialer{}
	if c.saslMechanism != nil {
		dialer.SASLMechanism = *c.saslMechanism
	}
	dialer.TLS = c.tlsConfig
	return &dialer
}

func (c *Client) Close() {
	for _, publisher := range c.publishers {
		publisher.close()
	}
	for _, consumer := range c.consumers {
		consumer.close()
	}
	c.lastConfig = Config{}
	c.saslMechanism = nil
	c.tlsConfig = nil
	c.publishers = make(map[string]*publisher)
	c.publishersConfiguration = make(map[string]PublisherCfg)
	c.consumers = make(map[string]*consumer)
	c.consumersConfiguration = make(map[string]ConsumerCfg)
	c.addresses = nil
	c.validBrokerAddress = ""
}
