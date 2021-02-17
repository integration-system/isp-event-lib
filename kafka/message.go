package kafka

import (
	"github.com/segmentio/kafka-go"
)

type Message struct {
	msg       kafka.Message
	committed *bool
}

func (m Message) Commit() {
	*m.committed = true
}

func (m *Message) GetValue() []byte {
	return m.msg.Value
}
func (m *Message) GetKey() []byte {
	return m.msg.Key
}
func (m *Message) GetFullMessage() kafka.Message {
	return m.msg
}
