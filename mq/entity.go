package rabbit

type (
	Consumer struct {
		QueueName    string                  `schema:"Название очереди"`
		Callback     func(delivery Delivery) `schema:"Функция обработки сообщения из очереди"`
		ErrorHandler func(error)             `schema:"Функция обработки ошибки при чтении очереди"`
	}

	Publisher struct {
		Exchange     string `schema:"Название точки маршрутизации"`
		ExchangeType string `schema:"Тип точки маршрутизации,(direct, funout)"`
		RoutingKey   string `valid:"required~Required" schema:"Ключ маршрутизации,для публикации напрямую в очередь, указывается название очереди"`
		QueueName    string `schema:"Название очереди"`
		Declare      bool   `schema:"Автоматическое объявление очереди,точки маршрутизации,привязки"`
	}
)
