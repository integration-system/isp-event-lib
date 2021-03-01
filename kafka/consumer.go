package kafka

import (
	"context"
	"fmt"
	"io"
	"time"

	log "github.com/integration-system/isp-log"
	"github.com/segmentio/kafka-go"
)

type consumer struct {
	reader       *kafka.Reader
	callback     func(msg Message)
	errorHandler func(error)
	config       ConsumerCfg
	name         string
}

type ConsumerCfg struct {
	ServiceConsumerCfg
	AdvancedCfg  *ConsumerAdvancedCfg
	Callback     func(msg Message)
	ErrorHandler func(error)
}

type ConsumerAdvancedCfg struct {
	// Min and max number of bytes to fetch from kafka in each request.
	MinBytes int
	// Default: 1Mb
	MaxBytes int

	// GroupBalancers is the priority-ordered list of client-side consumer group
	// balancing strategies that will be offered to the coordinator.  The first
	// strategy that all group members support will be chosen by the leader.
	//
	// Default: [Range, RoundRobin]
	GroupBalancers []kafka.GroupBalancer

	// CommitInterval indicates the interval at which offsets are committed to
	// the broker.  If 0, commits will be handled synchronously.
	//
	// Default: 0
	//
	// Only used when GroupID is set
	CommitInterval time.Duration

	// PartitionWatchInterval indicates how often a reader checks for partition changes.
	// If a reader sees a partition change (such as a partition add) it will rebalance the group
	// picking up new partitions.
	//
	// Default: 5s
	//
	// Only used when WatchPartitionChanges is set.
	PartitionWatchInterval time.Duration

	// WatchForPartitionChanges is used to inform kafka-go that a consumer group should be
	// polling the brokers and rebalancing if any partition changes happen to the topic.
	WatchPartitionChanges bool

	// Limit of how many attempts will be made before delivering the error.
	//
	// The default is to try 3 times.
	MaxAttempts int
}

func (c *consumer) start() {
	ctx := context.Background()
	var commit bool
	for {
		msg, err := c.reader.FetchMessage(ctx)
		if err != nil {
			if err == io.EOF {
				return
			}
			c.errorHandler(fmt.Errorf("while reading message from topic %v occurred an error, %w", c.config.TopicName, err))
		}
		commit = false
		c.callback(Message{msg: msg, committed: &commit}) // ctx??
		if commit {
			err = c.reader.CommitMessages(ctx, msg)
			c.errorHandler(fmt.Errorf("while committing message from topic %v occurred an error, %w", c.config.TopicName, err))
		}
	}
}

func (c *consumer) close() {
	err := c.reader.Close()
	if err != nil {
		log.Errorf(0, "can't close consumer %s with error: %v", c.config.TopicName, err)
	}
}

func (c *consumer) createReader(consumerCfg ConsumerCfg, addrs []string, kafkaAuth *Authentication) {
	readerCfg := makeReaderCfg(consumerCfg)

	readerCfg.Brokers = addrs
	readerCfg.Topic = consumerCfg.TopicName
	readerCfg.GroupID = consumerCfg.GroupID
	readerCfg.QueueCapacity = consumerCfg.PrefetchCount

	if kafkaAuth != nil {
		readerCfg.Dialer = &kafka.Dialer{SASLMechanism: getSASL(kafkaAuth)}
	}

	if consumerCfg.Callback == nil {
		log.Fatalf(0, "no callback was set to ConsumerCfg")
	}
	if consumerCfg.ErrorHandler == nil {
		log.Fatalf(0, "no ErrorHandler was set to ConsumerCfg")
	}
	c.callback = consumerCfg.Callback
	c.errorHandler = consumerCfg.ErrorHandler

	readerCfg.StartOffset = kafka.LastOffset
	readerCfg.ErrorLogger = logger{loggerPrefix: fmt.Sprintf("[consumer %s]", c.name)}

	c.reader = kafka.NewReader(readerCfg)
}

func makeReaderCfg(consumerCfg ConsumerCfg) kafka.ReaderConfig {
	if consumerCfg.AdvancedCfg == nil {
		return kafka.ReaderConfig{}
	}

	return kafka.ReaderConfig{
		MinBytes:               consumerCfg.AdvancedCfg.MinBytes,
		MaxBytes:               consumerCfg.AdvancedCfg.MaxBytes,
		GroupBalancers:         consumerCfg.AdvancedCfg.GroupBalancers,
		CommitInterval:         consumerCfg.AdvancedCfg.CommitInterval,
		PartitionWatchInterval: consumerCfg.AdvancedCfg.PartitionWatchInterval,
		WatchPartitionChanges:  consumerCfg.AdvancedCfg.WatchPartitionChanges,
		MaxAttempts:            consumerCfg.AdvancedCfg.MaxAttempts,
	}
}

type Message struct {
	msg       kafka.Message
	committed *bool
}

func (m Message) Commit() {
	*m.committed = true
}

func (m *Message) Value() []byte {
	return m.msg.Value
}
func (m *Message) Key() []byte {
	return m.msg.Key
}
func (m *Message) FullMessage() kafka.Message {
	return m.msg
}
