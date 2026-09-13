package votelog

import (
	"crypto/tls"
	"testing"
)

func TestSecurityRejectsCredentialDowngradeAndIncompleteTLS(t *testing.T) {
	for _, o := range []SecurityOptions{{SASLMechanism: "PLAIN", Username: "u", Password: "secret"}, {TLS: true, Username: "u", Password: "secret"}, {CAFile: "missing"}, {TLS: true, CertFile: "missing"}, {TLS: true, SASLMechanism: "bad", Username: "u", Password: "secret"}} {
		if _, err := BuildSecurity(o); err == nil {
			t.Fatal("unsafe or incomplete security configuration accepted")
		}
	}
	for _, mode := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		s, err := BuildSecurity(SecurityOptions{TLS: true, SASLMechanism: mode, Username: "u", Password: "secret"})
		if err != nil {
			t.Fatal(err)
		}
		c, _ := frameFixture()
		c.Security = s
		client, err := NewClient(c)
		if err != nil {
			t.Fatal(err)
		}
		client.Close()
	}
	for _, cfg := range []*tls.Config{{InsecureSkipVerify: true, MinVersion: tls.VersionTLS12}, {MinVersion: tls.VersionTLS10}} {
		if _, err := (&ClientSecurity{TLS: cfg}).options(); err == nil {
			t.Fatal("TLS verification/version downgrade accepted")
		}
	}
}
