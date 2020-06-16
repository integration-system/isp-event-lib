package ibm

import (
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/integration-system/go-amqp"
	"github.com/integration-system/isp-event-lib/mq"
	"github.com/integration-system/isp-lib-test/ctx"
	"github.com/integration-system/isp-lib-test/docker"
	"github.com/integration-system/isp-lib/v2/structure"
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

func TestClient_Reconnect(t *testing.T) {
	a := assert.New(t)
	const queueName = "/test_publisher"
	const payload = "somestring2"
	acks := int32(0)
	consumerErrors := int32(0)

	cli := createClient(queueName,
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

	cli := createClient(queueName,
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

func createClient(queueName string, callback func(delivery Delivery), errorHandler func(err error)) *AMQPClient {
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
	cfg := structure.RabbitConfig{
		Address: structure.AddressConfiguration{
			Port: "5672",
			IP:   "127.0.0.3",
		},
		User:     "admin",
		Password: "admin",
	}

	cli := NewAMQPClient()
	cli.ReceiveConfiguration(cfg, WithPublishers(publishers), WithConsumers(consumers))

	return cli
}
