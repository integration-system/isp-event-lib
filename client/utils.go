package client

import (
	"fmt"
	"strings"

	"github.com/integration-system/isp-lib/v2/structure"
)

func GetId(prefix, moduleName string, outerAddress structure.AddressConfiguration) string {
	clearAddr := strings.ReplaceAll(outerAddress.GetAddress(), ".", "_")
	clearAddr = strings.ReplaceAll(clearAddr, ":", "-")
	return fmt.Sprintf("%s_%s__%s", prefix, moduleName, clearAddr)
}
