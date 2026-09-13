package httpapi

import (
	"errors"
	"net"
	"net/url"
	"strconv"
	"strings"
)

// canonicalOrigin compares origins, not their textual spellings. Unicode
// hostnames must be supplied in browser-compatible ASCII (IDNA/punycode) form;
// rejecting them here is safer than silently configuring an unusable origin.
func canonicalOrigin(raw string, allowPath bool) (string, error) {
	u, err := url.Parse(raw)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" || u.User != nil || (!allowPath && (u.RawQuery != "" || u.Fragment != "" || (u.Path != "" && u.Path != "/"))) {
		return "", errors.New("expected an HTTP(S) origin without credentials")
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return "", errors.New("origin host is required")
	}
	if ip := net.ParseIP(host); ip != nil {
		host = ip.String()
	} else {
		for _, c := range host {
			if !(c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '.') {
				return "", errors.New("origin hostname must use ASCII or punycode")
			}
		}
	}
	port := u.Port()
	if port != "" {
		n, err := strconv.Atoi(port)
		if err != nil || n < 1 || n > 65535 {
			return "", errors.New("invalid origin port")
		}
		port = strconv.Itoa(n)
		if (u.Scheme == "http" && n == 80) || (u.Scheme == "https" && n == 443) {
			port = ""
		}
	} else if strings.HasSuffix(u.Host, ":") {
		return "", errors.New("empty origin port")
	}
	if port != "" {
		host = net.JoinHostPort(host, port)
	} else if strings.Contains(host, ":") {
		host = "[" + host + "]"
	}
	return u.Scheme + "://" + host, nil
}
