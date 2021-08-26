# RabbitMQ Client

Клиент для взаимодейсвия с RabbitMQ

## Usage

### Client

```go
var rabbitClientConfiguration = mq.Config {
	User: "user",
	Address:  event.AddressConfiguration{
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
	mapConsumers := map[string]mq.ConsumerCfg{
		"exampleConsumer": mq.ByOneConsumerCfg{
			CommonConsumerCfg: mq.CommonConsumerCfg{
				QueueName:     "example.queue", //Название очереди
				PrefetchCount: 1000,
				DeadLetter:    true, //Включение Dead Letter Exchange
			},
			Callback:     callback,     //Функция обработки сообщений из очереди
			ErrorHandler: errorHandler, //Функция обработки ошибок очереди
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
	mapPublishers := map[string]mq.PublisherCfg{
		"examplePublisher": {
			ExchangeName: "example.exchange", //Название точки маршрутизации
			RoutingKey:   "example.queue",  //Ключ маршрутизации
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

### Example

Пример показывает применение Dead Letter Exchange Использовано мануальное объявление declareCfg

```go
const (
    //publisherExchange = "example.exchange"
    defaultQueueName = "exampleQ"
)

var rabbitClientConfiguration = structure.RabbitConfig {
    User: "user",
    Address: structure.AddressConfiguration{
        IP:   "127.0.0.1",
        Port: "5672",
    },
    Password: "password",
}

func callback(delivery mq.Delivery) {
    msg := string(delivery.GetMessage())
    if msg == "DLX" {
        log.Printf("Nack(false) Received a message: %s", msg)
        _ = delivery.Nack(false).Release()
        return
    }
    log.Printf("Ack         Received a message: %s", msg)
    _ = delivery.Ack().Release()
}

func errorHandler(err error) {
    fmt.Println(err)
}

func main() {
    mapConsumers := map[string]mq.ConsumerCfg{
        "exampleConsumer": mq.ByOneConsumerCfg{
            CommonConsumerCfg: mq.CommonConsumerCfg{
                QueueName:     defaultQueueName, //Название очереди
                PrefetchCount: 1000,
            },
            Callback:     callback, //Функция обработки сообщений из очереди
            ErrorHandler: errorHandler, //Функция обработки ошибок очереди
        },
    }
    
    mapPublishers := map[string]mq.PublisherCfg{
        "examplePublisher": {
            //ExchangeName: publisherExchange, //Название точки маршрутизации
            RoutingKey:   defaultQueueName, //Ключ маршрутизации
            DeadLetter:    true, //enable DLX
        },
    }
    
    declareCfg := mapPublishers["examplePublisher"].GetDefaultDeclarations()
    
    client := mq.NewRabbitClient()
    client.ReceiveConfiguration(rabbitClientConfiguration,
        mq.WithConsumers(mapConsumers),
        mq.WithPublishers(mapPublishers),
        mq.WithDeclares(declareCfg),
    )
    
    // Подключение происходит асинхронно, для данного примера проще просто подождать
    time.Sleep(200 *time.Millisecond)
    
    err := client.GetPublisher("examplePublisher").Publish(amqp.Publishing{Body: []byte("example")})
    if err != nil {
        log.Printf("%s: %s", "example", err)
    }
    err = client.GetPublisher("examplePublisher").Publish(amqp.Publishing{Body: []byte("DLX")})
    if err != nil {
        log.Printf("%s: %s", "DLX", err)
    }
    
    time.Sleep(500 * time.Millisecond)
    client.Close()
}
```

## Implemented methods

* `NewRabbitClient()`
* `(*RabbitMqClient) ReceiveConfiguration()`
* `(*RabbitMqClient) Close()`
* `(*RabbitMqClient) GetPublisher()`
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

### `(*RabbitMqClient)  ReceiveConfiguration(rabbitConfig RabbitConfig, opts ...Option)`

Инициализирует объект rabbitMqClient
```go
client.ReceiveConfiguration(rabbitClientConfiguration,
	       WithPublishers(mapPublishers),
           WithConsumers(mapConsumers),
           WithDeclares(declareCfg),
        )
```

### `(*RabbitMqClient) Close()`

Прекращает взаимодействие с Rabbit
```go
client.Close()
```

### `(*RabbitMqClient) GetPublisher(name string) *publisher`

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

### `func WithDeclares(declare DeclareCfg) Option`

Инициализирует очереди, обменники и привязки

```go
WithDeclares(declareCfg)
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

Изменяет флаг, указывающий что сообщение вернется в очередь,
если очередь поддерживает Dead Letter, то сообщение будет отправлено в очередь `имя_очереди.DLX`
```go
delivery.Nack(false)
```

### `(d *Delivery) Release(ack, multiple, requeue bool) error`

Потребляет сообщение из очереди или возвращает его обратно
```go
delivery.Release()
```

