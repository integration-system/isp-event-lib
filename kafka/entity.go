package kafka

import (
	"github.com/integration-system/isp-event-lib/event"
)

type Config struct {
	AddressCfgs []event.AddressConfiguration `valid:"required~Required" schema:"Адреса Kafka"`
	KafkaAuth   *Authentication              `schema:"Настройка аутентификации, если требуется"`
}

type Authentication struct {
	AuthType string `schema:"Тип аутентификации" valid:"required~Required in(plain|scram_sha256|scram_sha512)"`
	User     string `schema:"Логин" valid:"required~Required"`
	Password string `schema:"Пароль" valid:"required~Required"`
}

type ServicePublisherCfg struct {
	TopicName string `valid:"required~Required" schema:"Название топика"`
	BatchSize int    `schema:"Количество накопленных перед отправкой сообщений"`
}

type ServiceConsumerCfg struct {
	TopicName     string `valid:"required~Required" schema:"Название топика"`
	PrefetchCount int    `schema:"Количество предзагруженных сообщений, 100 если не определен (установлен в 0)"`
	GroupID       string `schema:"Название группы потребителей, если указан - исключает 'Partition''"`
}
