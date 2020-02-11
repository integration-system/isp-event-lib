package nats

import (
	"github.com/integration-system/isp-event-lib/client"
	"github.com/integration-system/isp-lib/structure"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"sync"
	"time"
)

const (
	defaultPingInterval      = 1
	defaultMaxAttempts       = 5
	defaultConnectionTimeout = 3 * time.Second
)

type natsEventBustClient struct {
	stanConn stan.Conn
	natsConn *nats.Conn
	subs     map[string]*natsConsumer
	mu       sync.RWMutex
	cfg      structure.NatsConfig
	clientId string

	errorsCh chan error
}

func (c *natsEventBustClient) NewPublisher(topic string) client.Publisher {
	return &natsPublisher{
		publish: c.doPublish,
		subject: topic,
	}
}

func (c *natsEventBustClient) NewConsumer(topic, consumerId string) (client.Consumer, error) {
	return c.registerConsumer(topic, consumerId, "")
}

func (c *natsEventBustClient) NewExclusiveConsumer(topic string) (client.Consumer, error) {
	return c.registerConsumer(topic, topic, topic)
}

func (c *natsEventBustClient) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, sub := range c.subs {
		sub.Close()
	}
	err := c.stanConn.Close()
	c.notifyError(err)
	close(c.errorsCh)
}

func (c *natsEventBustClient) Errors() <-chan error {
	return c.errorsCh
}

func (c *natsEventBustClient) doPublish(subject string, data []byte, ack func(id string, err error)) error {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if ack == nil {
		return c.stanConn.Publish(subject, data)
	} else {
		_, err := c.stanConn.PublishAsync(subject, data, ack)
		return err
	}
}

func (c *natsEventBustClient) registerConsumer(topic, durable, queue string) (client.Consumer, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	id := nuid.Next()
	doRemove := func() {
		delete(c.subs, id)
	}
	sub, err := newNatsConsumer(c.stanConn, topic, queue, doRemove, stan.DurableName(durable), stan.SetManualAckMode())
	if err != nil {
		return nil, err
	}
	c.subs[id] = sub
	return sub, nil
}

func (c *natsEventBustClient) makeReconnectionCallback() nats.ConnHandler {
	return func(conn *nats.Conn) {
		c.mu.Lock()
		defer c.mu.Unlock()

		c.natsConn = conn
		stanConn, err := newStanConn(c.cfg, c.clientId, conn)

		if err != nil {
			c.notifyError(errors.WithMessage(err, "reconnect to stan"))
			return
		} else {
			c.stanConn = stanConn
		}

		for _, sub := range c.subs {
			sub.resubscribe(stanConn)
		}
	}
}

func (c *natsEventBustClient) notifyError(err error) {
	if err == nil {
		return
	}
	select {
	case c.errorsCh <- err:
	default:

	}
}

func NewNatsEventBusClient(cfg structure.NatsConfig, clientId string) (client.EventBusClient, error) {
	client := &natsEventBustClient{
		cfg:      cfg,
		clientId: clientId,
		errorsCh: make(chan error),
		subs:     make(map[string]*natsConsumer),
	}

	natsConn, err := nats.Connect(
		cfg.Address.GetAddress(),
		nats.Name(clientId),
		nats.MaxReconnects(-1),
		nats.ReconnectBufSize(-1),
		nats.DisconnectErrHandler(func(conn *nats.Conn, err error) {
			client.notifyError(errors.WithMessage(err, "nats disconnection"))
		}),
		nats.ReconnectHandler(client.makeReconnectionCallback()),
	)
	if err != nil {
		return nil, err
	}
	client.natsConn = natsConn

	stanConn, err := newStanConn(cfg, clientId, natsConn)
	if err != nil {
		client.natsConn.Close()
		return nil, err
	}
	client.stanConn = stanConn

	return client, nil
}

func newStanConn(natsConfig structure.NatsConfig, clientId string, natsConn *nats.Conn) (stan.Conn, error) {
	addr := natsConfig.Address.GetAddress()
	pingInterval := natsConfig.PintIntervalSec
	if pingInterval <= 0 {
		pingInterval = defaultPingInterval
	}
	pingAttempts := natsConfig.PingAttempts
	if pingAttempts <= 0 {
		pingAttempts = defaultMaxAttempts
	}
	return stan.Connect(
		natsConfig.ClusterId,
		clientId,
		stan.NatsConn(natsConn),
		stan.NatsURL(addr),
		stan.Pings(pingInterval, pingAttempts),
		stan.ConnectWait(defaultConnectionTimeout),
	)
}
