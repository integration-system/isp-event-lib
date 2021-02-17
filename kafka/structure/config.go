package structure

// удаляем
type AddressConfiguration struct {
	Port string `json:"port" schema:"Порт"`
	IP   string `json:"ip" schema:"Хост"`
}

// переносим в isp-lib/structure/config.go
type KafkaAuth struct {
	AuthType string `schema:"Тип аутентификации" valid:"required~Required in(plain|scram_sha256|scram_sha512)"`
	User     string `schema:"Логин" valid:"required~Required"`
	Password string `schema:"Пароль" valid:"required~Required"`
}

type KafkaConfig struct {
	Address   AddressConfiguration `valid:"required~Required" schema:"Адрес Kafka"`
	KafkaAuth *KafkaAuth           `schema:"Настройка аутентификации, если требуется"`
}

func (kc KafkaConfig) GetAddress() string {
	return kc.Address.IP + ":" + kc.Address.Port
}
