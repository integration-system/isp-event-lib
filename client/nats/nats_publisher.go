package nats

type natsPublisher struct {
	publish func(subject string, data []byte, ack func(id string, err error)) error
	subject string
}

func (n *natsPublisher) Publish(msg []byte) error {
	return n.publish(n.subject, msg, nil)
}

func (n *natsPublisher) PublishAsync(msg []byte, callback func(id string, err error)) error {
	return n.publish(n.subject, msg, callback)
}
