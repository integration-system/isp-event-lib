package nats

import (
	"github.com/integration-system/isp-event-lib/event"
)

type Config struct {
	ClusterId       string                     `valid:"required~Required" schema:"Идентификатор кластера"`
	Address         event.AddressConfiguration `valid:"required~Required" schema:"Адрес Nats"`
	PingAttempts    int                        `schema:"Максимальное количество попыток соединения,когда будет достигнут максимальное значение количества попыток соединение будет закрыто"`
	PintIntervalSec int                        `schema:"Интервал проверки соединения,значение в секундах, через которое происходит проверка соединения"`
	ClientId        string                     `json:"-"`
}
