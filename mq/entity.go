package mq

type CommonConsumerCfg struct {
	QueueName     string `valid:"required~Required" schema:"Название очереди"`
	PrefetchCount int    `schema:"Количество предзагруженных сообщений"`
}

type PublisherCfg struct {
	ExchangeName string `schema:"Название точки доступа"`
	RoutingKey   string `valid:"required~Required" schema:"Ключ маршрутизации,для публикации напрямую в очередь, указывается название очереди"`
}

type DeclareCfg struct {
	Exchanges []Exchange `schema:"Настройка точек доступа"`
	Queues    []Queue    `schema:"Настройка очереди"`
	Bindings  []Binding  `schema:"Настройка связи между очередями и точками доступа"`
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
