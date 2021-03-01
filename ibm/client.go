package ibm

import (
	"sync"
	"sync/atomic"

	"github.com/google/go-cmp/cmp"
)

func NewAMQPClient() *AMQPClient {
	return &AMQPClient{}
}

type AMQPClient struct {
	client atomic.Value
	lock   sync.Mutex
}

// ReceiveConfiguration should not be called concurrently.
func (r *AMQPClient) ReceiveConfiguration(conf Config, opts ...Option) {
	options := defaultOptions()
	for _, option := range opts {
		option(options)
	}

	cli, _ := r.client.Load().(*reconnectableClient)

	if cli == nil {
		cli = newReconnectableClient(conf, options)
	} else if !cmp.Equal(cli.RabbitConfig, conf) {
		cli.Close()
		cli = newReconnectableClient(conf, options)
	} else {
		cli.UpdateOptions(options)
	}

	r.client.Store(cli)
}

func (r *AMQPClient) GetPublisher(name string) *publisher {
	cli, _ := r.client.Load().(*reconnectableClient)

	if cli == nil {
		return nil
	}

	return cli.GetPublisher(name)
}

func (r *AMQPClient) Close() {
	r.lock.Lock()
	defer r.lock.Unlock()

	cli, _ := r.client.Load().(*reconnectableClient)

	if cli != nil {
		cli.Close()
	}
}
