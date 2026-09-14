package config

import (
	"testing"
	"time"
)

func kafkaConfigEnv(t *testing.T) {
	t.Helper()
	t.Setenv("DATABASE_URL", "postgres://unit-test-only")
	t.Setenv("ADMIN_PASSWORD", "unit-test-only-password")
	t.Setenv("KAFKA_BROKERS", "")
	for _, key := range []string{"MAX_PARTITION_UNIQUE_VOTERS", "KAFKA_TLS", "KAFKA_TLS_CA_FILE", "KAFKA_TLS_CERT_FILE", "KAFKA_TLS_KEY_FILE", "KAFKA_TLS_SERVER_NAME", "KAFKA_SASL_MECHANISM", "KAFKA_SASL_USERNAME", "KAFKA_SASL_PASSWORD", "MAX_INFLIGHT", "DURABILITY_REQUIRED_STANDBYS", "DURABILITY_STANDBY_NAMES", "KAFKA_PARTITIONS", "KAFKA_BATCH_VOTES", "KAFKA_QUEUE_VOTES", "KAFKA_LINGER_MS", "MAX_UNIQUE_VOTERS", "MAX_STORED_POLLS", "POLL_PREPARATION_SECONDS", "KAFKA_ALLOW_REMOTE_BROKERS"} {
		t.Setenv(key, "")
	}
}

func TestKafkaBrokerConfigurationRejectedBeforeStartup(t *testing.T) {
	for _, test := range []struct {
		brokers, remote string
		valid           bool
	}{
		{"127.0.0.1:19092,[::1]:19093", "false", true},
		{"broker.example:9093", "false", false},
		{"broker.example:9093", "true", true},
		{"127.0.0.1:0", "false", false},
		{"127.0.0.1:65536", "false", false},
		{"127.0.0.1:19092,", "false", false},
		{"https://broker.example:9093", "true", false},
		{"broker.example", "true", false},
	} {
		t.Run(test.brokers+"/"+test.remote, func(t *testing.T) {
			kafkaConfigEnv(t)
			t.Setenv("KAFKA_BROKERS", test.brokers)
			t.Setenv("KAFKA_ALLOW_REMOTE_BROKERS", test.remote)
			_, err := Load()
			if (err == nil) != test.valid {
				t.Fatal("incorrect seed address/policy validation", err)
			}
		})
	}
}

func TestKafkaBatchOptionsDefaultsAndOverrides(t *testing.T) {
	kafkaConfigEnv(t)
	cfg, err := Load()
	if err != nil || cfg.KafkaBatchVotes != 256 || cfg.KafkaQueueVotes != 2048 || cfg.KafkaLinger != 2*time.Millisecond {
		t.Fatalf("existing defaults changed: batch=%d queue=%d linger=%s err=%v", cfg.KafkaBatchVotes, cfg.KafkaQueueVotes, cfg.KafkaLinger, err)
	}
	t.Setenv("KAFKA_BATCH_VOTES", "1024")
	t.Setenv("KAFKA_QUEUE_VOTES", "8192")
	t.Setenv("KAFKA_LINGER_MS", "20")
	cfg, err = Load()
	if err != nil || cfg.KafkaBatchVotes != 1024 || cfg.KafkaQueueVotes != 8192 || cfg.KafkaLinger != 20*time.Millisecond {
		t.Fatalf("explicit Kafka tuning was not applied: batch=%d queue=%d linger=%s err=%v", cfg.KafkaBatchVotes, cfg.KafkaQueueVotes, cfg.KafkaLinger, err)
	}
}

func TestKafkaBatchOptionsRejectUnboundedValues(t *testing.T) {
	for _, key := range []string{"KAFKA_BATCH_VOTES", "KAFKA_QUEUE_VOTES", "KAFKA_LINGER_MS"} {
		for _, value := range []string{"0", "-1", "9999999999999999999999999", "2ms", "4097"} {
			if key == "KAFKA_QUEUE_VOTES" && value == "4097" {
				value = "8193"
			}
			t.Run(key+"/"+value, func(t *testing.T) {
				kafkaConfigEnv(t)
				t.Setenv(key, value)
				if _, err := Load(); err == nil {
					t.Fatal("invalid/unbounded Kafka tuning accepted")
				}
			})
		}
	}
}

func TestPartitionScaleAndLegacyUnusedPoolOption(t *testing.T) {
	kafkaConfigEnv(t)
	t.Setenv("KAFKA_PARTITIONS", "256")
	t.Setenv("MAX_PARTITION_UNIQUE_VOTERS", "2000000")
	t.Setenv("VOTE_DB_CONNECTIONS", "obsolete-value")
	c, err := Load()
	if err != nil || c.KafkaPartitions != 256 || c.MaxPartitionUnique != 2000000 {
		t.Fatal("partition scale/config", err)
	}
	t.Setenv("KAFKA_PARTITIONS", "257")
	if _, err := Load(); err == nil {
		t.Fatal("unbounded partitions")
	}
}
