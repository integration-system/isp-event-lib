package ibm

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/go-cmp/cmp"
	"github.com/integration-system/cony"
	"github.com/integration-system/go-amqp"
	"github.com/integration-system/isp-event-lib/mq"
	log "github.com/integration-system/isp-log"
	"github.com/integration-system/isp-log/stdcodes"
	"github.com/pkg/errors"
)

type reconnectableClient struct {
	cli *amqp.Client
	ses *amqp.Session

	publishers map[string]*publisher
	consumers  map[string]consumer

	RabbitConfig Config
	opts         *options
	lock         sync.RWMutex

	disconnectCh     chan error
	closeCh          chan struct{}
	reconnectAttempt int32
}

func newReconnectableClient(config Config, opts *options) *reconnectableClient {
	c := &reconnectableClient{
		publishers:   make(map[string]*publisher),
		consumers:    make(map[string]consumer),
		RabbitConfig: config,
		opts:         opts,
		disconnectCh: make(chan error, 1),
		closeCh:      make(chan struct{}, 1),
	}

	go c.startReconnecting()

	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.initClient()
	if err != nil {
		c.disconnectCh <- errors.WithMessage(err, "initial connect")
		return c
	}
	err = c.initPubsCons()
	if err != nil {
		c.disconnectCh <- errors.WithMessage(err, "initial connect")
	}

	return c
}

func (r *reconnectableClient) UpdateOptions(opts *options) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.opts = opts

	// когда пришел новый конфиг, но еще ни разу не удалось подключиться
	if r.cli == nil || r.ses == nil {
		return
	}

	err := r.initPubsCons()
	if err != nil {
		// есть подозрение что если обновить не получилось, то коннект тоже должен отвалиться,
		// в таком случае сработает ConnOnUnexpectedDisconnect callback
		select {
		case r.disconnectCh <- errors.WithMessage(err, "update options"):
		default:
		}
	}
}

func (r *reconnectableClient) GetPublisher(name string) *publisher {
	r.lock.RLock()
	defer r.lock.RUnlock()

	return r.publishers[name]
}

func (r *reconnectableClient) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.close()
	close(r.closeCh)
}

func (r *reconnectableClient) startReconnecting() {
	backoff := cony.DefaultBackoff

	for {
		select {
		case <-r.closeCh:
			return
		case err := <-r.disconnectCh:
			log.WithMetadata(map[string]interface{}{
				"message": err,
			}).Error(stdcodes.InitializingRabbitMqError, "amqp client disconnected. trying to reconnect")

			attempt := atomic.AddInt32(&r.reconnectAttempt, 1)
			select {
			case <-r.closeCh:
				return
			case <-time.After(backoff.Backoff(int(attempt))):
				// sleep
			}

			r.lock.Lock()

			err = r.initClient()
			if err != nil {
				r.disconnectCh <- errors.WithMessage(err, "reconnect")
				r.lock.Unlock()
				continue
			}
			err = r.initPubsCons()
			if err != nil {
				r.disconnectCh <- errors.WithMessage(err, "reconnect")
			}
			r.lock.Unlock()
		}
	}
}

func (r *reconnectableClient) initClient() error {
	r.close()
	connOpts := []amqp.ConnOption{
		amqp.ConnIdleTimeout(0),
		amqp.ConnOnUnexpectedDisconnect(func(err error) {
			select {
			case r.disconnectCh <- err:
			default:
			}
		}),
	}
	if r.opts.connContainerId != "" {
		connOpts = append(connOpts, amqp.ConnContainerID(r.opts.connContainerId))
	}
	connOpts = append(connOpts, r.opts.connOptions...)

	cli, err := amqp.Dial(r.RabbitConfig.GetUri(), connOpts...)
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
	atomic.StoreInt32(&r.reconnectAttempt, 0)

	return nil
}

func (r *reconnectableClient) initPubsCons() error {
	newPublishers, oldPublishers, err := r.newPublishers(r.opts.publishersConfiguration)
	if err != nil {
		return err
	}
	newConsumers, oldConsumers, err := r.newConsumers(r.opts.consumersConfiguration)
	if err != nil {
		return err
	}
	for _, c := range oldConsumers {
		c.awaitCancel(r.opts.timeout)
	}
	for _, p := range oldPublishers {
		p.cancel()
	}

	r.consumers = newConsumers
	r.publishers = newPublishers

	return nil
}

func (r *reconnectableClient) newPublishers(config map[string]mq.PublisherCfg) (map[string]*publisher, map[string]*publisher, error) {
	newPublishers, oldPublisher := make(map[string]*publisher), make(map[string]*publisher)
	for key, publisher := range r.publishers {
		newConfiguration, found := config[key]
		if found && cmp.Equal(r.opts.publishersConfiguration[key], newConfiguration) {
			newPublishers[key] = publisher
		} else {
			oldPublisher[key] = publisher
		}
	}
	for key, newConfiguration := range config {
		if _, found := newPublishers[key]; !found {
			newPublisher, err := r.makePublisher(newConfiguration)
			if err != nil {
				for _, pub := range newPublishers {
					pub.cancel()
				}
				return nil, nil, err
			}
			newPublishers[key] = newPublisher
		}
	}
	return newPublishers, oldPublisher, nil
}

func (r *reconnectableClient) newConsumers(config map[string]ConsumerCfg) (map[string]consumer, map[string]consumer, error) {
	newConsumers, oldConsumer := make(map[string]consumer), make(map[string]consumer)
	for key, consumer := range r.consumers {
		newConfiguration, found := config[key]
		if found && cmp.Equal(r.opts.consumersConfiguration[key], newConfiguration) {
			newConsumers[key] = consumer
		} else {
			consumer.stop()
			oldConsumer[key] = consumer
		}
	}
	for key, newConfiguration := range config {
		if _, found := newConsumers[key]; !found {
			newConsumer, err := r.makeConsumer(newConfiguration)
			if err != nil {
				for _, cons := range newConsumers {
					cons.stop()
				}
				return nil, nil, err
			}
			newConsumers[key] = newConsumer
		}
	}

	return newConsumers, oldConsumer, nil
}

func (r *reconnectableClient) close() {
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
			consumer.awaitCancel(r.opts.timeout)
		}
	}
	if r.cli != nil {
		_ = r.ses.Close(context.Background())
		_ = r.cli.Close()
		r.cli = nil
	}

	r.publishers = make(map[string]*publisher)
	r.consumers = make(map[string]consumer)
}

func (r *reconnectableClient) makeConsumer(consumer ConsumerCfg) (consumer, error) {
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
	receiver, err := r.ses.NewReceiver(opts...)
	if err != nil {
		return nil, errors.WithMessage(err, "create receiver")
	}
	newConsumer := consumer.createConsumer(receiver)
	go newConsumer.start()
	return newConsumer, nil
}

func (r *reconnectableClient) makePublisher(publisher mq.PublisherCfg) (*publisher, error) {
	opts := []amqp.LinkOption{
		amqp.LinkTargetAddress(publisher.RoutingKey),
		amqp.LinkName(publisher.RoutingKey),
	}
	sender, err := r.ses.NewSender(opts...)
	if err != nil {
		return nil, errors.WithMessage(err, "create sender")
	}
	return newPublisher(sender, publisher, r.opts.timeout), nil
}
