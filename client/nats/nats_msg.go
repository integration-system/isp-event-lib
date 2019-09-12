package nats

import (
	"github.com/nats-io/stan.go"
)

type natsMsg struct {
	msg *stan.Msg
}

func (n natsMsg) Sequence() uint64 {
	return n.msg.Sequence
}

func (n natsMsg) Data() []byte {
	return n.msg.Data
}

func (n natsMsg) Timestamp() int64 {
	return n.msg.Timestamp
}

func (n *natsMsg) Ack() error {
	return n.msg.Ack()
}

func (n *natsMsg) Nack() error {
	return nil
}
