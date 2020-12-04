package mq

import (
	"fmt"

	"github.com/integration-system/isp-event-lib/event"
)

const (
	FanoutExchange = "fanout"
	DirectExchange = "direct"

	deadLetterArg            = "x-dead-letter-exchange"
	dlQueueSuffix            = ".DLQ"
	commonDeadLetterExchange = "CommonDLX"
)

type Config struct {
	Address  event.AddressConfiguration `valid:"required~Required" schema:"Адрес RabbitMQ"`
	Vhost    string                     `schema:"Виртуальный хост,для изоляции очередей"`
	User     string                     `schema:"Логин"`
	Password string                     `schema:"Пароль"`
}

func (rc Config) GetUri() string {
	if rc.User == "" {
		return fmt.Sprintf("amqp://%s/%s", rc.Address.GetAddress(), rc.Vhost)
	} else {
		return fmt.Sprintf("amqp://%s:%s@%s/%s", rc.User, rc.Password, rc.Address.GetAddress(), rc.Vhost)
	}
}

type CommonConsumerCfg struct {
	QueueName     string `valid:"required~Required" schema:"Название очереди"`
	PrefetchCount int    `schema:"Количество предзагруженных сообщений"`
}

type PublisherCfg struct {
	ExchangeName string `schema:"Название точки доступа"`
	RoutingKey   string `valid:"required~Required" schema:"Ключ маршрутизации,для публикации напрямую в очередь, указывается название очереди"`
	DeadLetter   bool   `schema:"Подключение Dead Letter Exchange"`
}

func (pc PublisherCfg) GetDefaultDeclarations() DeclareCfg {
	declarations := DeclareCfg{
		Exchanges: make([]Exchange, 0),
		Queues:    make([]Queue, 0),
		Bindings:  make([]Binding, 0),
	}

	dur := true
	if pc.RoutingKey != "" {
		queue := Queue{
			Name:       pc.RoutingKey,
			Durable:    &dur,
			AutoDelete: false,
			Exclusive:  false,
		}
		if pc.DeadLetter {
			queue.Args = make(map[string]interface{}, 1)
			queue.Args[deadLetterArg] = commonDeadLetterExchange
			declarations.addDeadLetterDeclarations(pc.RoutingKey)
		}
		declarations.Queues = append(declarations.Queues, queue)
	}
	if pc.ExchangeName != "" && pc.RoutingKey != "" {
		declarations.Exchanges = append(declarations.Exchanges, Exchange{
			Name:       pc.ExchangeName,
			Kind:       DirectExchange,
			Durable:    &dur,
			AutoDelete: false,
		})
		declarations.Bindings = append(declarations.Bindings, Binding{
			QueueName:    pc.RoutingKey,
			ExchangeName: pc.ExchangeName,
			Key:          pc.RoutingKey,
		})
	}
	return declarations
}

type DeclareCfg struct {
	Exchanges []Exchange `schema:"Настройка точек доступа"`
	Queues    []Queue    `schema:"Настройка очереди"`
	Bindings  []Binding  `schema:"Настройка связи между очередями и точками доступа"`
}

func (dc DeclareCfg) Join(add DeclareCfg) DeclareCfg {
	return DeclareCfg{
		Exchanges: append(dc.Exchanges, add.Exchanges...),
		Queues:    append(dc.Queues, add.Queues...),
		Bindings:  append(dc.Bindings, add.Bindings...),
	}
}

func (dc *DeclareCfg) addDeadLetterDeclarations(name string) {
	dur := true
	dlqName := name + dlQueueSuffix

	dc.Exchanges = append(dc.Exchanges, Exchange{
		Name:    commonDeadLetterExchange,
		Kind:    DirectExchange,
		Durable: &dur,
	})
	dc.Queues = append(dc.Queues, Queue{
		Name:    dlqName,
		Durable: &dur,
	})
	dc.Bindings = append(dc.Bindings, Binding{
		QueueName:    dlqName,
		ExchangeName: commonDeadLetterExchange,
		Key:          name,
		Args:         nil,
	})
}

type Exchange struct {
	Name       string                 `valid:"required~Required" schema:"Название точки доступа"`
	Kind       string                 `schema:"Тип точки доступа,(direct, funout)"`
	Durable    *bool                  `schema:"Устойчивость точки доступа"`
	AutoDelete bool                   `schema:"Автоматическое удаление"`
	Args       map[string]interface{} `schema:"Вспомогательные поля в таблице"`
}

type Queue struct {
	Name       string                 `valid:"required~Required" schema:"Название очереди"`
	Durable    *bool                  `schema:"Устойчивость очереди"`
	AutoDelete bool                   `schema:"Автоматическое удаление,автоматически удаляется когда отписывается последний потребитель"`
	Exclusive  bool                   `schema:"Эксклюзивность,очередь используется только одним соединением и будет удалена, когда соединение будет закрыто"`
	Args       map[string]interface{} `schema:"Вспомогательные поля в таблице"`
}

type Binding struct {
	QueueName    string                 `schema:"Название очереди"`
	ExchangeName string                 `schema:"Название точки доступа"`
	Key          string                 `valid:"required~Required" schema:"Ключ маршрутизации"`
	Args         map[string]interface{} `schema:"Вспомогательные поля в таблице"`
}
