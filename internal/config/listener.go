package config

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
)

// validateListener checks syntax without resolving DNS or binding a socket.
// Actual address availability is checked by main before opening the store.
func validateListener(address string) error {
	host, port, err := net.SplitHostPort(address)
	n, parseErr := strconv.Atoi(port)
	if err != nil || parseErr != nil || n < 0 || n > 65535 || port == "" {
		return errors.New("HTTP_ADDR must be host:port with a numeric port from 0 to 65535")
	}
	for _, c := range port {
		if c < '0' || c > '9' {
			return errors.New("invalid HTTP_ADDR port")
		}
	}
	if host == "" {
		return nil
	}
	if _, err := netip.ParseAddr(host); err == nil {
		return nil
	}
	if len(host) > 253 {
		return errors.New("invalid HTTP_ADDR hostname")
	}
	for _, label := range strings.Split(strings.TrimSuffix(host, "."), ".") {
		if len(label) == 0 || len(label) > 63 || label[0] == '-' || label[len(label)-1] == '-' {
			return errors.New("invalid HTTP_ADDR hostname")
		}
		for _, c := range label {
			if !(c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '-') {
				return errors.New("HTTP_ADDR hostname must use ASCII or punycode")
			}
		}
	}
	return nil
}
