package kafka

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/integration-system/isp-event-lib/kafka/structure"
	log "github.com/integration-system/isp-log"
	"github.com/segmentio/kafka-go"
)

type consumer struct {
	reader       *kafka.Reader
	callback     func(msg Message)
	errorHandler func(error)
	config       *ConsumerCfg
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
			log.Errorf(0, "while reading message from topic %v occurred an error, %v", c.config.TopicName, err) // todo code; need if error logger used?
			c.errorHandler(err)
		}
		commit = false
		c.callback(Message{msg: msg, committed: &commit}) // ctx??
		if commit {
			err = c.reader.CommitMessages(ctx, msg)
			log.Errorf(0, "while committing message from topic %v occurred an error, %v", c.config.TopicName, err) // todo code; need if error logger used?
		}
	}
}

func (c *consumer) close() {
	err := c.reader.Close() // todo  async??
	if err != nil {
		log.Errorf(0, "can't close consumer %s with error: %v", c.config.TopicName, err) // todo code; need if error logger used?
	}
}

func (c *consumer) createReader(consumerCfg *ConsumerCfg, kafkaCfg structure.KafkaConfig, nameConsumer string) {
	readerCfg := makeReaderCfg(consumerCfg)

	readerCfg.Brokers = []string{kafkaCfg.GetAddress()}
	readerCfg.Topic = consumerCfg.TopicName
	readerCfg.GroupID = consumerCfg.GroupID
	readerCfg.QueueCapacity = consumerCfg.PrefetchCount

	if kafkaCfg.KafkaAuth != nil {
		readerCfg.Dialer = &kafka.Dialer{SASLMechanism: getSASL(kafkaCfg)}
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
	readerCfg.ErrorLogger = logger{loggerPrefix: fmt.Sprintf("[consumer %s]", nameConsumer)}

	c.reader = kafka.NewReader(readerCfg)
}

func makeReaderCfg(consumerCfg *ConsumerCfg) kafka.ReaderConfig {
	if consumerCfg.AdvancedCfg == nil {
		return kafka.ReaderConfig{}
	}

	return kafka.ReaderConfig{
		MinBytes:               consumerCfg.AdvancedCfg.MinBytes,
		MaxBytes:               consumerCfg.AdvancedCfg.MaxBytes,
		CommitInterval:         consumerCfg.AdvancedCfg.CommitInterval,
		PartitionWatchInterval: consumerCfg.AdvancedCfg.PartitionWatchInterval,
		WatchPartitionChanges:  consumerCfg.AdvancedCfg.WatchPartitionChanges,
		MaxAttempts:            consumerCfg.AdvancedCfg.MaxAttempts,
	}
}

func createConsumer(consumerCfg *ConsumerCfg, kafkaCfg structure.KafkaConfig, nameConsumer string) *consumer {
	consumer := &consumer{}
	consumer.createReader(consumerCfg, kafkaCfg, nameConsumer)
	consumer.config = consumerCfg
	return consumer
}
