package client

import (
	"fmt"
	"strings"

	"github.com/integration-system/isp-event-lib/event"
)

func GetId(prefix, moduleName string, outerAddress event.AddressConfiguration) string {
	clearAddr := strings.ReplaceAll(outerAddress.GetAddress(), ".", "_")
	clearAddr = strings.ReplaceAll(clearAddr, ":", "-")
	return fmt.Sprintf("%s_%s__%s", prefix, moduleName, clearAddr)
}
