package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

type Config struct {
	Addr          string
	PublicURL     string
	AdminPassword string
	DataDir       string
	MaxInflight   int
	MaxUnique     uint64
	BatchSize     int
	QueueVotes    int
	Linger        time.Duration
}

// LoadEnv never evaluates shell syntax or replaces an existing environment value.
func LoadEnv(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	s := bufio.NewScanner(f)
	for s.Scan() {
		line := strings.TrimSpace(s.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, value, ok := strings.Cut(line, "=")
		if !ok {
			return errors.New("invalid .env line")
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if strings.ContainsAny(key, " \t\r\n") || key == "" {
			return errors.New("invalid .env key")
		}
		if len(value) >= 2 && ((value[0] == '\'' && value[len(value)-1] == '\'') || (value[0] == '"' && value[len(value)-1] == '"')) {
			value = value[1 : len(value)-1]
		}
		if _, exists := os.LookupEnv(key); !exists {
			if err := os.Setenv(key, value); err != nil {
				return err
			}
		}
	}
	return s.Err()
}

func Load() (Config, error) {
	c := Config{Addr: env("HTTP_ADDR", "127.0.0.1:8080"), PublicURL: env("PUBLIC_URL", "http://127.0.0.1:8080"), AdminPassword: os.Getenv("ADMIN_PASSWORD"), DataDir: env("DATA_DIR", ".local/files"), MaxInflight: 4096, MaxUnique: 120000000, BatchSize: 4096, QueueVotes: 65536, Linger: 2 * time.Millisecond}
	if len(c.AdminPassword) < 16 || strings.Contains(c.AdminPassword, "CHANGE_ME") {
		return c, errors.New("set a unique ADMIN_PASSWORD of at least 16 characters")
	}
	for _, field := range []struct {
		name     string
		dst      *int
		min, max int
	}{
		{"MAX_INFLIGHT", &c.MaxInflight, 1, 100000},
		{"FILE_BATCH_VOTES", &c.BatchSize, 1, 131072},
		{"FILE_QUEUE_VOTES", &c.QueueVotes, 1, 1048576},
	} {
		if raw := os.Getenv(field.name); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < field.min || n > field.max {
				return c, fmt.Errorf("invalid %s", field.name)
			}
			*field.dst = n
		}
	}
	if raw := os.Getenv("MAX_UNIQUE_VOTERS"); raw != "" {
		n, err := strconv.ParseUint(raw, 10, 64)
		if err != nil || n < 1 || n > 200000000 {
			return c, errors.New("invalid MAX_UNIQUE_VOTERS")
		}
		c.MaxUnique = n
	}
	if raw := os.Getenv("FILE_GROUP_LINGER"); raw != "" {
		d, err := time.ParseDuration(raw)
		if err != nil || d < 0 || d > time.Second {
			return c, errors.New("invalid FILE_GROUP_LINGER")
		}
		c.Linger = d
	}
	return c, nil
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
