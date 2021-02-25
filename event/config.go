package event

import (
	"net"
)

type AddressConfiguration struct {
	IP   string `json:"ip" schema:"Хост"`
	Port string `json:"port" schema:"Порт"`
}

func (AddressConfiguration *AddressConfiguration) GetAddress() string {
	return net.JoinHostPort(AddressConfiguration.IP, AddressConfiguration.Port)
}
