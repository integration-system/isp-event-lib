# RabbitMQ Client

Клиент для взаимодейсвия с RabbitMQ

## Usage
### Client 
```go
var rabbitClientConfiguration = structure.RabbitConfig {
	User: "user",
	Address: structure.AddressConfiguration{
		IP:   "127.0.0.1",
		Port: "5672",
	},
	Password: "password",
}

func main() {
  client := NewRabbitClient()
  client.ReceiveConfiguration(rabbitClientConfiguration)
  client.Close()
}
```

### Consumer
```go
func callback(delivery Delivery) {
    defer func() { _ = delivery.Ack().Release() }() //обязательно вызывать Release, чтобы сообщения не копились
    fmt.Println(string(delivery.GetMessage()))
}

func errorHandler(err error) {
	fmt.Println(err)
}


func main() {
  mapConsumers := map[string]Consumer{
  		"example": {
  			QueueName:     "example.queue", //Название очереди
  			Callback:       callback,       //Функция обработки сообщений из очереди
  			ErrorHandler:   errorHandler,   //Функция обработки ошибок очереди
  		},
  	}

  client := NewRabbitClient()
  client.ReceiveConfiguration(rabbitClientConfiguration,
     WithConsumers(mapConsumers),
  )
  client.Close()
}
```

### Publisher
```go
func main() {
  mapPublishers := map[string]Publisher{
  		"example": {
  			Exchange:     "example",        //Название точки маршрутизации
  			ExchangeType: "direct",         //Тип точки маршрутизации (direct, funout)
  			RoutingKey:   "example.queue",  //Ключ маршрутизации
  			QueueName:    "example.queue",  //Название очереди
  			Declare:      true,             //Автоматическое объявление очереди
  		},
  	}

  client := NewRabbitClient()
  client.ReceiveConfiguration(rabbitClientConfiguration,
     WithPublishers(mapPublishers),
  )
  client.GetPublisher("example").Publish(amqp.Publishing{Body: []byte("example")})
  client.Close()
}
```

## Implemented methods
* `NewRabbitClient()`
* `(*rabbitMqClient) ReceiveConfiguration()`
* `(*rabbitMqClient) Close()`
* `(*rabbitMqClient) GetPublisher()`
* `WithAwaitConsumersTimeout()`
* `WithConsumers()`
* `WithPublishers()`
* `(*Delivery) GetMessage()`
* `(d *Delivery) Ack()`
* `(d *Delivery) Nack()`
* `(*Delivery) Release()`

### `NewRabbitClient() *rabbitMqClient`

Возвращает пустой объект rabbitMqClient
```go
client := NewRabbitClient()
```

### `(*rabbitMqClient)  ReceiveConfiguration(rabbitConfig structure.RabbitConfig, opts ...Option)`

Инициализирует объект rabbitMqClient
```go
client.ReceiveConfiguration(rabbitClientConfiguration,
	       WithPublishers(mapPublishers),
           WithConsumers(mapConsumers),
        )
```

### `(*rabbitMqClient) Close()`

Прекращает взаимодействие с Rabbit
```go
client.Close()
```


### `(*rabbitMqClient) GetPublisher(name string) *publisher`

Возвращает объект, публикующий сообщения в очередь
```go
client.GetPublisher("example")
```

### `WithAwaitConsumersTimeout(timeout time.Duration) Option`

Переопределяет время ожидания остановки работы потребителей сообщений
```go
client.WithAwaitConsumersTimeout(5 * time.Second)
```

### `WithConsumers(consumers map[string]Consumer) Option`

Инициализирует подписки на очереди, взаимодействие с сообщениями происходит через callback-функцию, принимающую на вход Delivery
```go
WithConsumers(mapConsumers)
```

### `WithPublishers(publishers map[string]Publisher) Option`

Инициализирует взаимодействия с очередями для публикаций сообщений
```go
WithPublishers(mapPublishers)
```

### `(*Delivery) GetMessage() []byte`

Возвращает тело сообщения
```go
delivery.GetMessage()
```

### `(d *Delivery) Ack() *Delivery` 

Изменяет флаг, указывающий что сообщение потребится из очереди
```go
delivery.Ack()
```


### `(d *Delivery) Nack(requeue bool) *Delivery`

Изменяет флаг, указывающий что сообщение вернется в очередь
```go
delivery.Nack(false)
```

### `(d *Delivery) Release(ack, multiple, requeue bool) error`

Потребляет сообщение из очереди или возвращает его обратно
```go
delivery.Release()
```

