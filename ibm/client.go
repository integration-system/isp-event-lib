package ibm

import (
	"context"
	"github.com/Azure/go-amqp"
	"github.com/google/go-cmp/cmp"
	"github.com/integration-system/isp-event-lib/mq"
	"github.com/integration-system/isp-lib/v2/structure"
	log "github.com/integration-system/isp-log"
	"github.com/integration-system/isp-log/stdcodes"
	"github.com/pkg/errors"
	"math"
	"sync"
	"time"
)

func NewAMQPClient() *AMQPClient {
	return &AMQPClient{
		publishers:              make(map[string]*publisher),
		publishersConfiguration: make(map[string]mq.PublisherCfg),

		consumers:              make(map[string]consumer),
		consumersConfiguration: make(map[string]ConsumerCfg),

		lastConfig: structure.RabbitConfig{},
		lock:       sync.Mutex{},
	}
}

type AMQPClient struct {
	cli        *amqp.Client
	ses        *amqp.Session
	lastConfig structure.RabbitConfig

	publishers              map[string]*publisher
	publishersConfiguration map[string]mq.PublisherCfg

	consumers              map[string]consumer
	consumersConfiguration map[string]ConsumerCfg

	lock    sync.Mutex
	timeout time.Duration
}

//nolint exitAfterDefer
func (r *AMQPClient) ReceiveConfiguration(rabbitConfig structure.RabbitConfig, opts ...Option) {
	r.lock.Lock()
	defer func() {
		if err := recover(); err != nil {
			log.Fatal(stdcodes.InitializingRabbitMqError, err)
		}
		r.lock.Unlock()
	}()

	options := defaultOptions()
	for _, option := range opts {
		option(options)
	}

	if !cmp.Equal(r.lastConfig, rabbitConfig) {
		r.close()
		connOpts := []amqp.ConnOption{
			amqp.ConnIdleTimeout(0),
		}
		if options.connContainerId != "" {
			connOpts = append(connOpts, amqp.ConnContainerID(options.connContainerId))
		}

		cli, err := amqp.Dial(rabbitConfig.GetUri(), connOpts...)
		if err != nil {
			log.Fatal(stdcodes.InitializingRabbitMqError, errors.WithMessage(err, "create client"))
		}
		ses, err := cli.NewSession()
		if err != nil {
			log.Fatal(stdcodes.InitializingRabbitMqError, errors.WithMessage(err, "create session"))
		}
		r.cli = cli
		r.ses = ses
		r.lastConfig = rabbitConfig
	}
	if r.cli == nil {
		return
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
}

func (r *AMQPClient) GetPublisher(name string) *publisher {
	return r.publishers[name]
}

func (r *AMQPClient) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.close()
}

func (r *AMQPClient) newPublishers(config map[string]mq.PublisherCfg) (map[string]*publisher, map[string]*publisher) {
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

func (r *AMQPClient) newConsumers(config map[string]ConsumerCfg) (map[string]consumer, map[string]consumer) {
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

func (r *AMQPClient) close() {
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
		_ = r.ses.Close(context.Background())
		_ = r.cli.Close()
		r.cli = nil
	}
	r.lastConfig = structure.RabbitConfig{}
	r.publishers = make(map[string]*publisher)
	r.publishersConfiguration = make(map[string]mq.PublisherCfg)
	r.consumers = make(map[string]consumer)
	r.consumersConfiguration = make(map[string]ConsumerCfg)
}

func (r *AMQPClient) makeConsumer(consumer ConsumerCfg) consumer {
	cfg := consumer.getCommon()
	opts := []amqp.LinkOption{
		amqp.LinkSourceAddress(cfg.QueueName),
		amqp.LinkName(cfg.QueueName),
		amqp.LinkSourceExpiryPolicy(amqp.ExpiryLinkDetach),
		amqp.LinkSourceTimeout(math.MaxUint32),
	}

	if cfg.PrefetchCount > 0 {
		opts = append(opts, amqp.LinkCredit(uint32(cfg.PrefetchCount)))
	}
	conyConsumer, err := r.ses.NewReceiver(opts...)
	if err != nil {
		panic(errors.WithMessage(err, "create receiver"))
	}
	newConsumer := consumer.createConsumer(conyConsumer)
	go newConsumer.start()
	return newConsumer
}

func (r *AMQPClient) makePublisher(publisher mq.PublisherCfg) *publisher {
	opts := []amqp.LinkOption{
		amqp.LinkTargetAddress(publisher.RoutingKey),
		amqp.LinkName(publisher.RoutingKey),
	}
	sender, err := r.ses.NewSender(opts...)
	if err != nil {
		panic(errors.WithMessage(err, "create sender"))
	}
	return createPublisher(sender, publisher, r.timeout)
}
