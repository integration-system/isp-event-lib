package kafka

import (
	"github.com/segmentio/kafka-go"
)

type ServicePublisherCfg struct {
	TopicName string `valid:"required~Required" schema:"Название топика"`
	BatchSize int    `schema:"Количество накопленных перед отправкой сообщений"`
}

type ServiceConsumerCfg struct {
	TopicName      string                `valid:"required~Required" schema:"Название топика"`
	PrefetchCount  int                   `schema:"Количество предзагруженных сообщений, 100 если не определен (установлен в 0)"`
	GroupID        string                `schema:"Название группы потребителей, если указан - исключает 'Partition''"`
	GroupBalancers []kafka.GroupBalancer `schema:"Приоритетно ориентированный список стратегий балансировки для группы потребителей, первая стратегия которую поддерживают все потребители группы будет выбрана"`
}
