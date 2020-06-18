package ibm

import (
	"sync"
	"sync/atomic"

	"github.com/google/go-cmp/cmp"
	"github.com/integration-system/isp-lib/v2/structure"
)

func NewAMQPClient() *AMQPClient {
	return &AMQPClient{}
}

type AMQPClient struct {
	client atomic.Value
	lock   sync.Mutex
}

// ReceiveConfiguration should not be called concurrently.
func (r *AMQPClient) ReceiveConfiguration(rabbitConfig structure.RabbitConfig, opts ...Option) {
	options := defaultOptions()
	for _, option := range opts {
		option(options)
	}

	cli, _ := r.client.Load().(*reconnectableClient)

	if cli == nil {
		cli = newReconnectableClient(rabbitConfig, options)
	} else if !cmp.Equal(cli.RabbitConfig, rabbitConfig) {
		cli.Close()
		cli = newReconnectableClient(rabbitConfig, options)
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
