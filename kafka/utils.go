package kafka

import (
	"context"
	"time"

	"github.com/integration-system/isp-event-lib/event"
	log "github.com/integration-system/isp-log"
	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	AuthTypePlain       = "plain"
	AuthTypeScramSha256 = "scram_sha256"
	AuthTypeScramSha512 = "scram_sha512"

	dialTimeout = 2 * time.Second
)

type logger struct {
	loggerPrefix string
}

func (l logger) Printf(format string, args ...interface{}) {
	log.Errorf(0, l.loggerPrefix+format, args...)
}

func getSASL(kafkaAuth *Authentication) sasl.Mechanism {
	if kafkaAuth == nil {
		return nil
	}

	var (
		saslMechanism sasl.Mechanism
		err           error
	)

	switch kafkaAuth.AuthType {
	case AuthTypePlain:
		saslMechanism = plain.Mechanism{Username: kafkaAuth.User, Password: kafkaAuth.Password}
	case AuthTypeScramSha256, AuthTypeScramSha512:
		saslMechanism, err = scram.Mechanism(getScrumAlgo(kafkaAuth.AuthType),
			kafkaAuth.User,
			kafkaAuth.Password,
		)
		if err != nil {
			log.Fatalf(0, "can't set auth mechanism by error: %v", err)
		}
	default:
		log.Fatalf(0, "by Kafka auth type set: %s", kafkaAuth.AuthType)
	}
	return saslMechanism
}

func getScrumAlgo(algirithm string) scram.Algorithm {
	if algirithm == AuthTypeScramSha256 {
		return scram.SHA256
	} else if algirithm == AuthTypeScramSha256 {
		return scram.SHA512
	} else {
		panic("Scrum type mismatch")
	}
}

func getAddresses(kafkaConfig Config) []string {
	addresses := make([]string, 0, len(kafkaConfig.AddressCfgs))
	for _, adrCfg := range kafkaConfig.AddressCfgs {
		addresses = append(addresses, adrCfg.GetAddress())
	}
	return addresses
}

func tryDial(addressCfgs []event.AddressConfiguration) (string, error) {
	for _, adrCfg := range addressCfgs {
		adr := adrCfg.GetAddress()
		ctx, cancel := context.WithTimeout(context.Background(), dialTimeout)
		conn, err := kafka.DialContext(ctx, "tcp", adr)
		cancel()
		if err == nil && conn != nil {
			_ = conn.Close()
			return adr, nil
		}
	}
	return "", errors.New("can't connect to all brokers addresses configurations")
}
