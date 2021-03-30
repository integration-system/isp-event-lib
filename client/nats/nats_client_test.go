package nats_test

import (
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integration-system/isp-event-lib/client"
	"github.com/integration-system/isp-event-lib/client/nats"
	"github.com/integration-system/isp-event-lib/event"
	"github.com/integration-system/isp-lib-test/ctx"
	"github.com/integration-system/isp-lib-test/docker"
	"github.com/stretchr/testify/assert"
)

const (
	natsImage = "docker.io/library/nats-streaming:linux"
)

type TestConfig struct {
}

func (c *TestConfig) GetBaseConfiguration() ctx.BaseTestConfiguration {
	return ctx.BaseTestConfiguration{}
}

func TestMain(m *testing.M) {
	cfg := TestConfig{}
	test, err := ctx.NewIntegrationTest(m, &cfg, setup)
	if err != nil {
		panic(err)
	}
	test.PrepareAndRun()
}

func setup(_ *ctx.TestContext, runTest func() int) int {
	cli, err := docker.NewClient()
	if err != nil {
		panic(err)
	}
	defer cli.Close()
	natsCtx, err := cli.RunContainer(
		natsImage,
		docker.PullImage("", ""),
		docker.WithLogger(os.Stdout),
		docker.WithPortBindings(map[string]string{
			"4222": "4222",
			"8222": "8222",
		}),
	)
	defer natsCtx.Close() //nolint
	if err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Second)

	return runTest()
}

func TestNatsEventBustClient_NewConsumer(t *testing.T) {
	assert := assert.New(t)

	cli, err := nats.NewNatsEventBusClient(nats.Config{
		ClusterId: "test-cluster",
		Address: event.AddressConfiguration{
			Port: "4222",
			IP:   "localhost",
		},
	}, "NewConsumer")
	if !assert.NoError(err) {
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(4)
	consumed := int32(0)

	consumer1, err := cli.NewConsumer("test", "first")
	if !assert.NoError(err) {
		return
	}
	go awaitConsuming(consumer1, wg, &consumed, assert)

	consumer2, err := cli.NewConsumer("test", "second")
	if !assert.NoError(err) {
		return
	}
	go awaitConsuming(consumer2, wg, &consumed, assert)

	publisher := cli.NewPublisher("test")
	err = publisher.Publish([]byte("test"))
	if !assert.NoError(err) {
		return
	}
	err = publisher.Publish([]byte("test"))
	if !assert.NoError(err) {
		return
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:

	case <-time.After(1 * time.Second):
		assert.Failf("", "must consumer totally 4 messages, but consumed %d", atomic.LoadInt32(&consumed))
	}
}

func TestNatsEventBustClient_NewExclusiveConsumer(t *testing.T) {
	assert := assert.New(t)

	cli, err := nats.NewNatsEventBusClient(nats.Config{
		ClusterId: "test-cluster",
		Address: event.AddressConfiguration{
			Port: "4222",
			IP:   "localhost",
		},
	}, "NewExclusiveConsumer")
	if !assert.NoError(err) {
		return
	}

	wg := &sync.WaitGroup{}
	wg.Add(2)
	consumed := int32(0)

	consumer1, err := cli.NewExclusiveConsumer("test2")
	if !assert.NoError(err) {
		return
	}
	go awaitConsuming(consumer1, wg, &consumed, assert)

	consumer2, err := cli.NewExclusiveConsumer("test2")
	if !assert.NoError(err) {
		return
	}
	go awaitConsuming(consumer2, wg, &consumed, assert)

	publisher := cli.NewPublisher("test2")
	err = publisher.Publish([]byte("test"))
	if !assert.NoError(err) {
		return
	}
	err = publisher.Publish([]byte("test"))
	if !assert.NoError(err) {
		return
	}

	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()

	select {
	case <-done:

	case <-time.After(1 * time.Second):
		assert.Failf("", "must consumer totally 2 messages, but consumed %d", atomic.LoadInt32(&consumed))
	}
}

func awaitConsuming(consumer client.Consumer, wg *sync.WaitGroup, consumed *int32, assert *assert.Assertions) {
	for msg := range consumer.Messages() {
		assert.NoError(msg.Ack())
		atomic.AddInt32(consumed, 1)
		wg.Done()
	}
}
