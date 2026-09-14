package config

import "testing"

func TestListenerSyntaxDoesNotNeedNetwork(t *testing.T) {
	for _, address := range []string{":8080", "127.0.0.1:0", "[::1]:8091", "[fe80::1%eth0]:8091", "unresolved.example.invalid:8091"} {
		if err := validateListener(address); err != nil {
			t.Fatalf("%s: %v", address, err)
		}
	}
	for _, address := range []string{"", "localhost", "localhost:", "localhost:http", "localhost:-1", "localhost:+80", "host:65536", "white space:8091", "-host:8091", "[broken:ip]:8091", "https://host:8091"} {
		if err := validateListener(address); err == nil {
			t.Fatalf("invalid address accepted: %s", address)
		}
	}
}
