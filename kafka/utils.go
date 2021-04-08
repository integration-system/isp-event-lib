package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"fmt"

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
	log.Errorf(0, l.loggerPrefix+format, args...)
}

func getTlsConfig(tlsConf *TlsConfiguration) (*tls.Config, error) {
	if tlsConf == nil {
		return nil, nil
	}

	serverCert, err := base64.StdEncoding.DecodeString(tlsConf.ServerCert)
	if err != nil {
		return nil, fmt.Errorf("can't decode ServerCert: %v", err)
	}
	clientCert, err := base64.StdEncoding.DecodeString(tlsConf.ClientCert)
	if err != nil {
		return nil, fmt.Errorf("can't decode ClientCert: %v", err)
	}
	clientKey, err := base64.StdEncoding.DecodeString(tlsConf.ClientKey)
	if err != nil {
		return nil, fmt.Errorf("can't decode ClientKey: %v", err)
	}

	tlsConfig := tls.Config{MinVersion: tls.VersionTLS12}
	cert, err := tls.X509KeyPair(clientCert, clientKey)
	if err != nil {
		return nil, fmt.Errorf("unable to parse client certificate keypair: %v", err)
	}
	tlsConfig.Certificates = []tls.Certificate{cert}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(serverCert)
	tlsConfig.RootCAs = caCertPool

	return &tlsConfig, nil
}

func getSASL(kafkaAuth *Authentication) *sasl.Mechanism {
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
		log.Fatalf(0, "unknown Kafka auth type: %s", kafkaAuth.AuthType)
	}
	return &saslMechanism
}

func getScrumAlgo(algorithm string) scram.Algorithm {
	switch algorithm {
	case AuthTypeScramSha256:
		return scram.SHA256
	case AuthTypeScramSha512:
		return scram.SHA512
	default:
		panic("Scrum type mismatch: " + algorithm)
	}
}

func getAddresses(kafkaConfig Config) []string {
	addresses := make([]string, 0, len(kafkaConfig.AddressCfgs))
	for _, adrCfg := range kafkaConfig.AddressCfgs {
		addresses = append(addresses, adrCfg.GetAddress())
	}
	return addresses
}
