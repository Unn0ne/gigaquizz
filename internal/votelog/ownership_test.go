package votelog

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestOwnershipScopePreservesFullRoutingAndPrivateConfig(t *testing.T) {
	c, _ := frameFixture()
	c.Partitions = 8
	base := c.DefinitionHash()
	token := [16]byte{7}
	partition := c.Partition(token)
	c.OwnedPartitions = []int32{partition}
	c.Security = &ClientSecurity{Username: "private-user", Password: "private-secret"}
	if c.Partition(token) != partition || c.DefinitionHash() != base {
		t.Fatal("runtime ownership changed immutable routing")
	}
	data, err := json.Marshal(c)
	if err != nil || strings.Contains(string(data), "private-") || strings.Contains(string(data), "OwnedPartitions") {
		t.Fatal("runtime credentials/ownership persisted")
	}
	m := c.manifest()
	if !m.Partial || m.TotalPartitions != 8 || !reflect.DeepEqual(m.OwnedPartitions, []int32{partition}) {
		t.Fatal("partial manifest omitted ownership scope")
	}
	for _, scope := range [][]int32{{}, {0, 0}, {-1}, {8}} {
		c.OwnedPartitions = scope
		if c.validate() == nil {
			t.Fatal("invalid ownership scope accepted")
		}
	}
	c.OwnedPartitions = nil
	if err := c.validate(); err != nil {
		t.Fatal(err)
	}
	if c.manifest().Partial || len(c.manifest().OwnedPartitions) != 8 {
		t.Fatal("nil did not preserve all-partition compatibility")
	}
}

func TestUnownedSubmitAndFrameNeverEnterQueue(t *testing.T) {
	s, w, _ := batchTestWriter()
	s.cfg.Partitions = 2
	s.cfg.OwnedPartitions = []int32{0}
	s.byPartition = []*writer{w, nil}
	var token [16]byte
	for i := 1; i < 256; i++ {
		token[0] = byte(i)
		if s.cfg.Partition(token) == 1 {
			break
		}
	}
	if s.cfg.Partition(token) != 1 {
		t.Fatal("fixture has no unowned token")
	}
	if _, err := s.Submit(context.Background(), token, 1); !errors.Is(err, ErrNotOwned) {
		t.Fatal(err)
	}
	if _, err := s.SubmitFrame(context.Background(), []Input{{Token: token, Choice: 1}}); !errors.Is(err, ErrNotOwned) {
		t.Fatal(err)
	}
	if len(w.jobs) != 0 || s.admitted.Load() != 0 {
		t.Fatal("unowned vote was admitted")
	}
}

func TestDefinitionHashBindsCheckpointToPollNotTuning(t *testing.T) {
	c, _ := frameFixture()
	base := c.DefinitionHash()
	changed := c
	changed.BatchSize = 1
	changed.Linger = time.Second
	changed.Brokers = []string{"127.0.0.1:1"}
	if changed.DefinitionHash() != base {
		t.Fatal("runtime tuning invalidated immutable definition")
	}
	for _, change := range []func(*Config){func(v *Config) { v.Topic += "_new" }, func(v *Config) { v.Partitions++ }, func(v *Config) { v.AllowedMask = 1 }, func(v *Config) { v.StartsAt = v.StartsAt.Add(time.Second) }, func(v *Config) { v.PollID[1]++ }} {
		next := c
		change(&next)
		if next.DefinitionHash() == base {
			t.Fatal("changed definition retained checkpoint identity")
		}
	}
}
