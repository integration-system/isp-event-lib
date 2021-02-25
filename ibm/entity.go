package ibm

import (
	"fmt"
	"time"

	"github.com/integration-system/isp-event-lib/event"
)

type Config struct {
	Address  event.AddressConfiguration `valid:"required~Required" schema:"Адрес ibm mq"`
	User     string                     `schema:"Логин"`
	Password string                     `schema:"Пароль"`
}

func (rc Config) GetUri() string {
	if rc.User == "" {
		return fmt.Sprintf("amqp://%s", rc.Address.GetAddress())
	} else {
		return fmt.Sprintf("amqp://%s:%s@%s", rc.User, rc.Password, rc.Address.GetAddress())
	}
}

func (rc Config) ReconnectionTimeout() time.Duration {
	/*timeout := rc.ReconnectionTimeoutMs
	if timeout <= 0 {
		timeout = defaultReconnectionTimeout
	}*/
	return 3 * time.Millisecond
}
