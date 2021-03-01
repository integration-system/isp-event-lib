package ibm

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integration-system/go-amqp"
	"github.com/integration-system/isp-event-lib/event"
	"github.com/integration-system/isp-event-lib/mq"
	"github.com/integration-system/isp-lib-test/ctx"
	"github.com/integration-system/isp-lib-test/docker"
	"github.com/stretchr/testify/assert"
)

const (
	activemqImage = "docker.io/rmohr/activemq:5.15.9"
)

var (
	activemqCtx *docker.ContainerContext
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
	activemqCtx, err = cli.RunContainer(
		activemqImage,
		docker.PullImage("", ""),
		docker.WithLogger(os.Stdout),
		docker.WithPortBindings(map[string]string{
			"127.0.0.3:5672": "5672", // AMQP
			"127.0.0.3:8161": "8161", // UI
		}),
	)
	defer activemqCtx.Close()
	if err != nil {
		panic(err)
	}

	time.Sleep(3 * time.Second)

	return runTest()
}

func TestAmqpClient_ConnOnUnexpectedDisconnect(t *testing.T) {
	a := assert.New(t)
	connErrors := int32(0)

	connOpts := []amqp.ConnOption{
		amqp.ConnIdleTimeout(0),
		amqp.ConnOnUnexpectedDisconnect(func(err error) {
			atomic.AddInt32(&connErrors, 1)
		}),
	}

	cfg := Config{
		Address: event.AddressConfiguration{
			Port: "5672",
			IP:   "127.0.0.3",
		},
		User:     "admin",
		Password: "admin",
	}

	cli, err := amqp.Dial(cfg.GetUri(), connOpts...)
	a.NoError(err)
	a.NoError(cli.Close())
	a.EqualValues(0, atomic.LoadInt32(&connErrors))

	cli, err = amqp.Dial(cfg.GetUri(), connOpts...)
	a.NoError(err)
	a.EqualValues(0, atomic.LoadInt32(&connErrors))

	a.NoError(activemqCtx.StopContainer(10 * time.Second))
	a.EqualValues(1, atomic.LoadInt32(&connErrors))
	time.Sleep(time.Second)

	a.NoError(activemqCtx.StartContainer())
	time.Sleep(3 * time.Second)
	a.EqualValues(1, atomic.LoadInt32(&connErrors))
}

func TestClient_Reconnect(t *testing.T) {
	a := assert.New(t)
	const queueName = "/test_publisher"
	const payload = "somestring2"
	acks := int32(0)
	consumerErrors := int32(0)

	cli, _ := createTestClient(queueName,
		func(delivery Delivery) {
			atomic.AddInt32(&acks, 1)
			data := delivery.GetMessage().GetData()
			a.Equal(payload, string(data))
			a.NoError(delivery.Ack().Release())
		},
		func(err error) {
			atomic.AddInt32(&consumerErrors, 1)
		},
	)
	defer cli.Close()

	err := cli.GetPublisher(queueName).Publish(amqp.NewMessage([]byte(payload)))
	a.NoError(err, "publishing first time")
	time.Sleep(time.Second)
	a.EqualValues(1, atomic.LoadInt32(&acks), "first message successfully handled")

	a.NoError(activemqCtx.StopContainer(10 * time.Second))
	a.NoError(activemqCtx.StartContainer())
	time.Sleep(5 * time.Second)

	err = cli.GetPublisher(queueName).Publish(amqp.NewMessage([]byte(payload)))
	a.NoError(err, "publishing second time")
	time.Sleep(time.Second)

	a.EqualValues(2, atomic.LoadInt32(&acks))
	a.EqualValues(1, atomic.LoadInt32(&consumerErrors))
}

func TestClient_ReconnectAfterFirstFail(t *testing.T) {
	a := assert.New(t)
	const queueName = "/test_publisher-2"
	const payload = "somestring333"
	acks := int32(0)
	consumerErrors := int32(0)

	a.NoError(activemqCtx.StopContainer(10 * time.Second))

	cli, _ := createTestClient(queueName,
		func(delivery Delivery) {
			atomic.AddInt32(&acks, 1)
			data := delivery.GetMessage().GetData()
			a.Equal(payload, string(data))
			a.NoError(delivery.Ack().Release())
		},
		func(err error) {
			atomic.AddInt32(&consumerErrors, 1)
		},
	)
	defer cli.Close()

	time.Sleep(time.Second)
	a.NoError(activemqCtx.StartContainer())
	time.Sleep(5 * time.Second)

	err := cli.GetPublisher(queueName).Publish(amqp.NewMessage([]byte(payload)))
	a.NoError(err)
	time.Sleep(time.Second)

	a.EqualValues(1, atomic.LoadInt32(&acks))
	a.EqualValues(0, atomic.LoadInt32(&consumerErrors))
}

func TestClient_UpdateOptions(t *testing.T) {
	a := assert.New(t)
	const queueName = "/test_publisher-3"
	const queueName2 = "/test_publisher-3_2"
	const payload = "somestring444"
	acks := int32(0)
	consumerErrors := int32(0)

	callback := func(delivery Delivery) {
		atomic.AddInt32(&acks, 1)
		data := delivery.GetMessage().GetData()
		a.Equal(payload, string(data))
		a.NoError(delivery.Ack().Release())
	}
	errorHandler := func(err error) {
		atomic.AddInt32(&consumerErrors, 1)
	}

	cli, cfg := createTestClient(queueName,
		callback,
		errorHandler,
	)
	defer cli.Close()

	err := cli.GetPublisher(queueName).Publish(amqp.NewMessage([]byte(payload)))
	a.NoError(err)
	time.Sleep(time.Second)

	publishers := map[string]mq.PublisherCfg{
		queueName2: {
			RoutingKey: queueName2,
		},
	}
	consumers := map[string]ConsumerCfg{
		queueName2: ByOneConsumerCfg{
			CommonConsumerCfg: mq.CommonConsumerCfg{
				QueueName: queueName2,
			},
			Callback:     callback,
			ErrorHandler: errorHandler,
		},
	}
	cli.ReceiveConfiguration(cfg, WithPublishers(publishers), WithConsumers(consumers), WithDefaultTimeout(time.Second))
	time.Sleep(time.Second)

	err = cli.GetPublisher(queueName2).Publish(amqp.NewMessage([]byte(payload)))
	a.NoError(err)
	time.Sleep(time.Second)
	a.Nil(cli.GetPublisher(queueName))

	a.EqualValues(2, atomic.LoadInt32(&acks))
	a.EqualValues(0, atomic.LoadInt32(&consumerErrors))
}

func createTestClient(queueName string, callback func(delivery Delivery), errorHandler func(err error),
) (*AMQPClient, Config) {
	publishers := map[string]mq.PublisherCfg{
		queueName: {
			RoutingKey: queueName,
		},
	}
	consumers := map[string]ConsumerCfg{
		queueName: ByOneConsumerCfg{
			CommonConsumerCfg: mq.CommonConsumerCfg{
				QueueName: queueName,
			},
			Callback:     callback,
			ErrorHandler: errorHandler,
		},
	}
	cfg := Config{
		Address: event.AddressConfiguration{
			Port: "5672",
			IP:   "127.0.0.3",
		},
		User:     "admin",
		Password: "admin",
	}

	cli := NewAMQPClient()
	cli.ReceiveConfiguration(cfg, WithPublishers(publishers), WithConsumers(consumers))

	return cli, cfg
}
