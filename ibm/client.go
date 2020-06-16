package ibm

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/integration-system/cony"
	"github.com/integration-system/go-amqp"
	"github.com/integration-system/isp-event-lib/mq"
	"github.com/integration-system/isp-lib/v2/structure"
	log "github.com/integration-system/isp-log"
	"github.com/integration-system/isp-log/stdcodes"
	"github.com/pkg/errors"
)

func NewAMQPClient() *AMQPClient {
	c := &AMQPClient{
		publishers:              make(map[string]*publisher),
		publishersConfiguration: make(map[string]mq.PublisherCfg),

		consumers:              make(map[string]consumer),
		consumersConfiguration: make(map[string]ConsumerCfg),

		lastConfig:   structure.RabbitConfig{},
		lock:         sync.Mutex{},
		disconnectCh: make(chan error, 1),
	}

	go c.startReconnecting()
	return c
}

type AMQPClient struct {
	cli        *amqp.Client
	ses        *amqp.Session
	lastConfig structure.RabbitConfig
	lastOpts   []Option

	publishers              map[string]*publisher
	publishersConfiguration map[string]mq.PublisherCfg

	consumers              map[string]consumer
	consumersConfiguration map[string]ConsumerCfg

	lock         sync.Mutex
	timeout      time.Duration
	disconnectCh chan error
}

func (r *AMQPClient) ReceiveConfiguration(rabbitConfig structure.RabbitConfig, opts ...Option) {
	r.lock.Lock()
	defer func() {
		if err := recover(); err != nil {
			var e error
			switch obj := err.(type) {
			case error:
				e = obj
			default:
				e = errors.New(fmt.Sprint(obj))
			}
			r.disconnectCh <- errors.WithMessage(e, "receive new configuration")

		}
		r.lock.Unlock()
	}()

	err := r.initClient(false, rabbitConfig, opts...)
	if err != nil {
		r.disconnectCh <- errors.WithMessage(err, "receive new configuration")
	}
}

func (r *AMQPClient) GetPublisher(name string) *publisher {
	return r.publishers[name]
}

func (r *AMQPClient) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()
	r.close()
}

func (r *AMQPClient) startReconnecting() {
	attempt := 0
	backoff := cony.DefaultBackoff
	var prevConfig structure.RabbitConfig
	for {
		select {
		case err := <-r.disconnectCh:
			log.WithMetadata(map[string]interface{}{
				"message": err,
			}).Error(stdcodes.InitializingRabbitMqError, "amqp client disconnected. trying to reconnect")

			func() {
				r.lock.Lock()
				defer func() {
					if err := recover(); err != nil {
						var e error
						switch obj := err.(type) {
						case error:
							e = obj
						default:
							e = errors.New(fmt.Sprint(obj))
						}
						r.disconnectCh <- e
					}
					r.lock.Unlock()
				}()

				if r.lastConfig == (structure.RabbitConfig{}) {
					return
				}

				if prevConfig != r.lastConfig {
					attempt = 0
				}
				attempt++
				time.Sleep(backoff.Backoff(attempt))

				prevConfig = r.lastConfig
				err := r.initClient(true, r.lastConfig, r.lastOpts...)
				if err != nil {
					r.disconnectCh <- err
				}
			}()
		}
	}
}

func (r *AMQPClient) initClient(force bool, rabbitConfig structure.RabbitConfig, opts ...Option) error {
	options := defaultOptions()
	for _, option := range opts {
		option(options)
	}
	r.lastOpts = opts
	r.timeout = options.timeout

	if force || !cmp.Equal(r.lastConfig, rabbitConfig) {
		r.close()
		r.lastConfig = rabbitConfig
		connOpts := []amqp.ConnOption{
			amqp.ConnIdleTimeout(0),
			amqp.ConnOnUnexpectedDisconnect(func(err error) {
				r.disconnectCh <- err
			}),
		}
		if options.connContainerId != "" {
			connOpts = append(connOpts, amqp.ConnContainerID(options.connContainerId))
		}

		cli, err := amqp.Dial(rabbitConfig.GetUri(), connOpts...)
		if err != nil {
			return errors.WithMessage(err, "create client")
		}
		ses, err := cli.NewSession()
		if err != nil {
			_ = cli.Close()
			return errors.WithMessage(err, "create session")
		}
		r.cli = cli
		r.ses = ses
	}

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

	return nil
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
