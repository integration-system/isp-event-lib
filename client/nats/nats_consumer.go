package nats

import (
	"github.com/integration-system/isp-event-lib/client"
	"github.com/nats-io/stan.go"
	"github.com/pkg/errors"
	"sync"
)

type subConfig struct {
	subject string
	queue   string
	opts    []stan.SubscriptionOption
}

func (cfg subConfig) makeSub(c stan.Conn, handler stan.MsgHandler) (stan.Subscription, error) {
	if cfg.queue == "" {
		return c.Subscribe(cfg.subject, handler, cfg.opts...)
	} else {
		return c.QueueSubscribe(cfg.subject, cfg.queue, handler, cfg.opts...)
	}
}

type natsConsumer struct {
	cfg        subConfig
	messagesCh chan client.Msg
	errorsCh   chan error
	sub        stan.Subscription
	mu         sync.RWMutex
	remove     func()
	closed     bool
}

func (c *natsConsumer) resubscribe(conn stan.Conn) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.closed {
		return
	}

	sub, err := c.cfg.makeSub(conn, c.notify)
	if err != nil {
		c.notifyError(errors.WithMessage(err, "resubscribe"))
	} else {
		c.sub = sub
	}
}

func (c *natsConsumer) notify(msg *stan.Msg) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if !c.closed {
		m := &natsMsg{msg: msg}
		c.messagesCh <- m
	}
}

func (c *natsConsumer) notifyError(err error) {
	if err == nil {
		return
	}

	select {
	case c.errorsCh <- err:
	default:
	}
}

func (c *natsConsumer) Close() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.closed = true
	if c.sub != nil {
		err := c.sub.Close()
		c.notifyError(errors.WithMessage(err, "close consumer"))
	}
	close(c.messagesCh)
	close(c.errorsCh)
	c.remove()
}

func (c *natsConsumer) Messages() <-chan client.Msg {
	return c.messagesCh
}

func (c *natsConsumer) Errors() <-chan error {
	return c.errorsCh
}

func newNatsConsumer(c stan.Conn, subject, queue string, remove func(), opts ...stan.SubscriptionOption) (*natsConsumer, error) {
	consumer := &natsConsumer{
		messagesCh: make(chan client.Msg),
		errorsCh:   make(chan error),
		closed:     false,
		remove:     remove,
		cfg: subConfig{
			subject: subject,
			queue:   queue,
			opts:    opts,
		},
	}

	sub, err := consumer.cfg.makeSub(c, consumer.notify)
	if err != nil {
		return nil, err
	} else {
		consumer.sub = sub
		return consumer, nil
	}
}
