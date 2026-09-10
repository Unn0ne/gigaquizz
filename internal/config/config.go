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
	Addr             string
	PublicURL        string
	DatabaseURL      string
	AdminPassword    string
	VoteConnections  int32
	MaxInflight      int
	RequiredStandbys int
	StandbyNames     []string
	DatabaseSchema   string
	KafkaBrokers     []string
	KafkaPartitions  int
	KafkaAllowRemote bool
	MaxUnique        int
	MaxPolls         int
	PreparationLead  time.Duration
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
	c := Config{Addr: env("HTTP_ADDR", "127.0.0.1:8092"), PublicURL: env("PUBLIC_URL", "http://127.0.0.1:8092"), DatabaseURL: os.Getenv("DATABASE_URL"), AdminPassword: os.Getenv("ADMIN_PASSWORD"), VoteConnections: 64, MaxInflight: 1024,
		DatabaseSchema: env("GIGAQUIZZ_SCHEMA", "gigaquizz_kafka"), KafkaBrokers: strings.Split(env("KAFKA_BROKERS", "127.0.0.1:19092,127.0.0.1:19093,127.0.0.1:19094"), ","), KafkaPartitions: 4, MaxUnique: 120_000_000, MaxPolls: 10_000, PreparationLead: 20 * time.Second}
	if c.DatabaseURL == "" {
		return c, errors.New("DATABASE_URL is required; run make dev or configure .env")
	}
	if len(c.AdminPassword) < 16 || strings.Contains(c.AdminPassword, "CHANGE_ME") {
		return c, errors.New("set a unique ADMIN_PASSWORD of at least 16 characters")
	}
	if raw := os.Getenv("VOTE_DB_CONNECTIONS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 10000 {
			return c, fmt.Errorf("invalid VOTE_DB_CONNECTIONS")
		}
		c.VoteConnections = int32(n)
	}
	if raw := os.Getenv("MAX_INFLIGHT"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 1 || n > 100000 {
			return c, fmt.Errorf("invalid MAX_INFLIGHT")
		}
		c.MaxInflight = n
	}
	if raw := os.Getenv("DURABILITY_REQUIRED_STANDBYS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 0 || n > 16 {
			return c, errors.New("invalid DURABILITY_REQUIRED_STANDBYS")
		}
		c.RequiredStandbys = n
	}
	if raw := strings.TrimSpace(os.Getenv("DURABILITY_STANDBY_NAMES")); raw != "" {
		for _, name := range strings.Split(raw, ",") {
			c.StandbyNames = append(c.StandbyNames, strings.TrimSpace(name))
		}
	}
	for key, setting := range map[string]struct {
		value    *int
		min, max int
	}{
		"KAFKA_PARTITIONS":  {&c.KafkaPartitions, 1, 32},
		"MAX_UNIQUE_VOTERS": {&c.MaxUnique, 1, 120_000_000},
		"MAX_STORED_POLLS":  {&c.MaxPolls, 1, 100_000},
	} {
		if raw := os.Getenv(key); raw != "" {
			n, err := strconv.Atoi(raw)
			if err != nil || n < setting.min || n > setting.max {
				return c, fmt.Errorf("invalid %s", key)
			}
			*setting.value = n
		}
	}
	if raw := os.Getenv("POLL_PREPARATION_SECONDS"); raw != "" {
		n, err := strconv.Atoi(raw)
		if err != nil || n < 5 || n > 60 {
			return c, errors.New("invalid POLL_PREPARATION_SECONDS")
		}
		c.PreparationLead = time.Duration(n) * time.Second
	}
	if raw := os.Getenv("KAFKA_ALLOW_REMOTE_BROKERS"); raw != "" {
		n, err := strconv.ParseBool(raw)
		if err != nil {
			return c, errors.New("invalid KAFKA_ALLOW_REMOTE_BROKERS")
		}
		c.KafkaAllowRemote = n
	}
	for i := range c.KafkaBrokers {
		c.KafkaBrokers[i] = strings.TrimSpace(c.KafkaBrokers[i])
	}
	return c, nil
}
func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
