package kafka

import (
	"context"
	"time"

	log "github.com/integration-system/isp-log"
	"github.com/segmentio/kafka-go"
)

type RequiredAcks int

const (
	requiredAckOffset = 10

	RequireNone = RequiredAcks(kafka.RequireNone + requiredAckOffset)
	RequireOne  = RequiredAcks(kafka.RequireOne + requiredAckOffset)
	RequireAll  = RequiredAcks(kafka.RequireAll + requiredAckOffset)
)

func (r RequiredAcks) mappingRequiredAcks() kafka.RequiredAcks {
	if r == 0 {
		return kafka.RequireAll
	}
	return kafka.RequiredAcks(int(r) - requiredAckOffset)
}

type publisher struct {
	writer *kafka.Writer
	config *PublisherCfg
}

type PublisherCfg struct {
	ServicePublisherCfg
	AdvancedCfg *PublisherAdvancedCfg
}

type PublisherAdvancedCfg struct {
	// The balancer used to distribute messages across partitions.
	//
	// The default is to use a round-robin distribution.
	Balancer kafka.Balancer

	// Limit on how many attempts will be made to deliver a message.
	//
	// The default is to try at most 10 times.
	MaxAttempts int

	// Time limit on how often incomplete message batches will be flushed to
	// kafka.
	//
	// The default is to flush at least every second.
	BatchTimeout time.Duration

	// Number of acknowledges from partition replicas required before receiving
	// a response to a produce request, the following values are supported:
	//
	//  RequireNone (10)  fire-and-forget, do not wait for acknowledgements from the
	//  RequireOne  (11)  wait for the leader to acknowledge the writes
	//  RequireAll  (9) wait for the full ISR to acknowledge the writes
	//
	// REDEFINED from kafka.RequireNone (0), kafka.RequireOne(1), kafka.RequireAll (-1) at kafka-go/produce.go
	// The default (0) is sets to RequireAll
	RequiredAcks kafka.RequiredAcks

	// Setting this flag to true causes the WriteMessages method to never block.
	// It also means that errors are ignored since the caller will not receive
	// the returned value. Use this only if you don't care about guarantees of
	// whether the messages were written to kafka.
	Async bool

	// Compression set the compression codec to be used to compress messages.
	Compression kafka.Compression
}

func (p *publisher) Publish(ctx context.Context, msgs ...kafka.Message) error {
	return p.writer.WriteMessages(ctx, msgs...)
}

func (p *publisher) close() {
	err := p.writer.Close()
	if err != nil {
		log.Errorf(0, "can't close consumer %s with error: %v", p.config.TopicName, err)
	}
}

func newWriter(publisherCfg PublisherCfg) *kafka.Writer {
	if publisherCfg.AdvancedCfg == nil {
		return &kafka.Writer{
			Topic:        publisherCfg.TopicName,
			BatchSize:    publisherCfg.BatchSize,
			RequiredAcks: RequireAll.mappingRequiredAcks(),
		}
	} else {
		return &kafka.Writer{
			Topic:        publisherCfg.TopicName,
			BatchSize:    publisherCfg.BatchSize,
			Balancer:     publisherCfg.AdvancedCfg.Balancer,
			MaxAttempts:  publisherCfg.AdvancedCfg.MaxAttempts,
			BatchTimeout: publisherCfg.AdvancedCfg.BatchTimeout,
			RequiredAcks: publisherCfg.AdvancedCfg.RequiredAcks,
			Async:        publisherCfg.AdvancedCfg.Async,
			Compression:  publisherCfg.AdvancedCfg.Compression,
		}
	}
}
