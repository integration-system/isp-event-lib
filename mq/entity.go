package mq

const (
	FanoutExchange = "fanout"
	DirectExchange = "direct"
)

type CommonConsumerCfg struct {
	QueueName     string `valid:"required~Required" schema:"Название очереди"`
	PrefetchCount int    `schema:"Количество предзагруженных сообщений"`
}

type PublisherCfg struct {
	ExchangeName string `schema:"Название точки доступа"`
	RoutingKey   string `valid:"required~Required" schema:"Ключ маршрутизации,для публикации напрямую в очередь, указывается название очереди"`
}

func (pc PublisherCfg) GetDefaultDeclarations() DeclareCfg {
	exchanges := make([]Exchange, 0)
	queues := make([]Queue, 0)
	bindings := make([]Binding, 0)
	dur := true
	if pc.RoutingKey != "" {
		queues = append(queues, Queue{
			Name:       pc.RoutingKey,
			Durable:    &dur,
			AutoDelete: false,
			Exclusive:  false,
		})
	}
	if pc.ExchangeName != "" && pc.RoutingKey != "" {
		exchanges = append(exchanges, Exchange{
			Name:       pc.ExchangeName,
			Kind:       DirectExchange,
			Durable:    &dur,
			AutoDelete: false,
		})
		bindings = append(bindings, Binding{
			QueueName:    pc.RoutingKey,
			ExchangeName: pc.ExchangeName,
			Key:          pc.RoutingKey,
		})
	}
	return DeclareCfg{
		Exchanges: exchanges,
		Queues:    queues,
		Bindings:  bindings,
	}
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
