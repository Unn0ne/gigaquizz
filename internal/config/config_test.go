package config

import (
	"testing"
	"time"
)

func minimalEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"HTTP_ADDR", "PUBLIC_URL", "DATABASE_URL", "KAFKA_BROKERS", "DATA_DIR", "MAX_INFLIGHT", "MAX_UNIQUE_VOTERS", "FILE_BATCH_VOTES", "FILE_QUEUE_VOTES", "FILE_GROUP_LINGER", "FILE_PARTITIONS", "MAX_PARTITION_UNIQUE_VOTERS"} {
		t.Setenv(key, "")
	}
	t.Setenv("ADMIN_PASSWORD", "test-only-long-admin-password")
}

func TestFileServiceNeedsNoDatabaseOrBroker(t *testing.T) {
	minimalEnv(t)
	c, err := Load()
	if err != nil {
		t.Fatal(err)
	}
	if c.DataDir != ".local/files" || c.MaxUnique != 120000000 || c.BatchSize != 4096 || c.Linger != 2*time.Millisecond {
		t.Fatalf("unexpected defaults: %+v", c)
	}
	t.Setenv("DATA_DIR", t.TempDir())
	t.Setenv("MAX_UNIQUE_VOTERS", "100")
	t.Setenv("FILE_GROUP_LINGER", "3ms")
	if c, err := Load(); err != nil || c.MaxUnique != 100 || c.Linger != 3*time.Millisecond {
		t.Fatalf("configuration was not applied: %v", err)
	}
}

func TestFileServiceRejectsUnboundedConfiguration(t *testing.T) {
	for _, tc := range []struct{ key, value string }{
		{"ADMIN_PASSWORD", "short"}, {"MAX_INFLIGHT", "0"}, {"MAX_UNIQUE_VOTERS", "200000001"},
		{"FILE_PARTITIONS", "257"}, {"MAX_PARTITION_UNIQUE_VOTERS", "200000001"}, {"FILE_BATCH_VOTES", "131073"}, {"FILE_QUEUE_VOTES", "1048577"}, {"FILE_GROUP_LINGER", "2s"},
	} {
		t.Run(tc.key, func(t *testing.T) {
			minimalEnv(t)
			t.Setenv(tc.key, tc.value)
			if _, err := Load(); err == nil {
				t.Fatal("invalid configuration accepted")
			}
		})
	}
}

func TestZeroLingerAndPartitionDefaults(t *testing.T) {
	minimalEnv(t)
	t.Setenv("FILE_GROUP_LINGER", "0s")
	t.Setenv("MAX_UNIQUE_VOTERS", "150000000")
	c, err := Load()
	if err != nil || c.Linger != 0 || c.Partitions != 1 || c.MaxPartitionUnique != 150000000 {
		t.Fatalf("explicit zero/default compatibility: %+v %v", c, err)
	}
	t.Setenv("FILE_PARTITIONS", "128")
	t.Setenv("MAX_PARTITION_UNIQUE_VOTERS", "2000000")
	c, err = Load()
	if err != nil || c.Partitions != 128 || c.MaxPartitionUnique != 2000000 {
		t.Fatalf("partition limits: %+v %v", c, err)
	}
}
