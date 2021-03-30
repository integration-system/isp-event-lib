package event

import (
	"net"
)

type AddressConfiguration struct {
	IP   string `json:"ip" schema:"Хост"`
	Port string `json:"port" schema:"Порт"`
}

func (ac *AddressConfiguration) GetAddress() string {
	return net.JoinHostPort(ac.IP, ac.Port)
}
