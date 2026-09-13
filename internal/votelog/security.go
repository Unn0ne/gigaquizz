package votelog

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

// ClientSecurity is runtime-only. Config excludes it from persisted journal JSON.
// Treat it as immutable after publishing to clients. TLS always verifies peers.
type ClientSecurity struct {
	TLS           *tls.Config
	SASLMechanism string
	Username      string
	Password      string
}

type SecurityOptions struct {
	TLS                                   bool
	CAFile, CertFile, KeyFile, ServerName string
	SASLMechanism, Username, Password     string
}

func BuildSecurity(o SecurityOptions) (*ClientSecurity, error) {
	if !o.TLS && (o.CAFile != "" || o.CertFile != "" || o.KeyFile != "" || o.ServerName != "") {
		return nil, errors.New("Kafka TLS files require TLS enabled")
	}
	s := &ClientSecurity{SASLMechanism: strings.ToUpper(o.SASLMechanism), Username: o.Username, Password: o.Password}
	if o.TLS {
		s.TLS = &tls.Config{MinVersion: tls.VersionTLS12, ServerName: o.ServerName}
		if o.CAFile != "" {
			pem, err := os.ReadFile(o.CAFile)
			if err != nil {
				return nil, errors.New("cannot read Kafka CA file")
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(pem) {
				return nil, errors.New("invalid Kafka CA certificate")
			}
			s.TLS.RootCAs = roots
		}
		if (o.CertFile == "") != (o.KeyFile == "") {
			return nil, errors.New("Kafka client certificate and key must be configured together")
		}
		if o.CertFile != "" {
			cert, err := tls.LoadX509KeyPair(o.CertFile, o.KeyFile)
			if err != nil {
				return nil, errors.New("invalid Kafka client certificate/key")
			}
			s.TLS.Certificates = []tls.Certificate{cert}
		}
	}
	if _, err := s.options(); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *ClientSecurity) options() ([]kgo.Opt, error) {
	if s == nil {
		return nil, nil
	}
	opts := []kgo.Opt{}
	if s.TLS != nil {
		if s.TLS.InsecureSkipVerify || s.TLS.MinVersion < tls.VersionTLS12 {
			return nil, errors.New("Kafka TLS must verify peers and require TLS 1.2 or newer")
		}
		opts = append(opts, kgo.DialTLSConfig(s.TLS.Clone()))
	}
	if s.SASLMechanism == "" {
		if s.Username != "" || s.Password != "" {
			return nil, errors.New("Kafka credentials require a SASL mechanism")
		}
		return opts, nil
	}
	if s.TLS == nil || s.Username == "" || s.Password == "" {
		return nil, errors.New("Kafka SASL requires verified TLS and credentials")
	}
	switch s.SASLMechanism {
	case "PLAIN":
		opts = append(opts, kgo.SASL(plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism()))
	case "SCRAM-SHA-256":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism()))
	case "SCRAM-SHA-512":
		opts = append(opts, kgo.SASL(scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism()))
	default:
		return nil, errors.New("unsupported Kafka SASL mechanism")
	}
	return opts, nil
}

// NewClient applies the same runtime security to producers, administration and
// readers, including manual strict-audit fetches. It does not contact brokers.
func NewClient(c Config, options ...kgo.Opt) (*kgo.Client, error) {
	security, err := c.Security.options()
	if err != nil {
		return nil, err
	}
	opts := []kgo.Opt{kgo.SeedBrokers(c.Brokers...)}
	opts = append(opts, security...)
	opts = append(opts, options...)
	return kgo.NewClient(opts...)
}
