package kafka

type ServicePublisherCfg struct {
	TopicName string `valid:"required~Required" schema:"Название топика"`
	BatchSize int    `schema:"Количество накопленных перед отправкой сообщений"`
}

type ServiceConsumerCfg struct {
	TopicName     string `valid:"required~Required" schema:"Название топика"`
	PrefetchCount int    `schema:"Количество предзагруженных сообщений, 100 если не определен (установлен в 0)"`
	GroupID       string `schema:"Название группы потребителей, если указан - исключает 'Partition''"`
}
