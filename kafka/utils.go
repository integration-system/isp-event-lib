package kafka

import (
	"github.com/integration-system/isp-event-lib/kafka/structure"
	log "github.com/integration-system/isp-log"
	"github.com/segmentio/kafka-go/sasl"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

const (
	AuthTypePlain       = "plain"
	AuthTypeScramSha256 = "scram_sha256"
	AuthTypeScramSha512 = "scram_sha512"
)

type logger struct {
	loggerPrefix string
}

func (l logger) Printf(format string, args ...interface{}) {
	log.Errorf(0, l.loggerPrefix+format, args)
}

func getSASL(kc structure.KafkaConfig) sasl.Mechanism {
	if kc.KafkaAuth == nil {
		return nil
	}

	var (
		saslMechanism sasl.Mechanism
		err           error
	)
	if kc.KafkaAuth.AuthType == AuthTypePlain {
		saslMechanism = plain.Mechanism{Username: kc.KafkaAuth.User, Password: kc.KafkaAuth.Password}
	} else if kc.KafkaAuth.AuthType == AuthTypeScramSha256 || kc.KafkaAuth.AuthType == AuthTypeScramSha512 {

	} else {
		log.Fatalf(0, "by Kafka auth type set: %s", kc.KafkaAuth.AuthType) // todo этого не должно случиться -
	}

	switch kc.KafkaAuth.AuthType {
	case AuthTypePlain:
		saslMechanism = plain.Mechanism{Username: kc.KafkaAuth.User, Password: kc.KafkaAuth.Password}
	case AuthTypeScramSha256, AuthTypeScramSha512:
		saslMechanism, err = scram.Mechanism(getScrumAlgo(kc.KafkaAuth.AuthType), "username", "password")
		if err != nil {
			log.Errorf(0, "cat't set auth mechanism by error: %v", err)
		}
	default:
		log.Fatalf(0, "by Kafka auth type set: %s", kc.KafkaAuth.AuthType) // todo не слишком ли??
	}
	return saslMechanism
}

func getScrumAlgo(algirithm string) scram.Algorithm {
	if algirithm == "scram_sha256" {
		return scram.SHA256
	} else /*if algirithm == "scram_sha512"*/ {
		return scram.SHA512
	}
}
