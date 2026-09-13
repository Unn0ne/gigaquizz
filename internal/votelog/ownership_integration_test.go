package votelog

import (
	"errors"
	"testing"
)

func TestKafkaDisjointPartitionOwnersTransferWithoutFencingPeer(t *testing.T) {
	ctx, full := kafkaTestConfig(t, 4)
	left := full
	left.OwnedPartitions = []int32{0, 2}
	right := full
	right.OwnedPartitions = []int32{1, 3}
	a, clockA := kafkaTestStore(t, ctx, left)
	b, clockB := kafkaTestStore(t, ctx, right) // Existing BOOTs in A must not block fresh B.
	tokenFor := func(p int32) [16]byte {
		for {
			v := kafkaTestToken(t)
			if full.Partition(v) == p {
				return v
			}
		}
	}
	keyA, keyB, keyC := tokenFor(0), tokenFor(1), tokenFor(3)
	if _, err := a.Submit(ctx, keyB, 1); !errors.Is(err, ErrNotOwned) {
		t.Fatal("unowned admission", err)
	}
	if _, err := a.Submit(ctx, keyA, 1); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Submit(ctx, keyB, 2); err != nil {
		t.Fatal(err)
	}
	a.Close()
	a, clockA = kafkaTestStore(t, ctx, left)
	if _, err := a.Submit(ctx, keyA, 4); err != nil {
		t.Fatal("subset recovery", err)
	}
	if _, err := b.Submit(ctx, keyC, 4); err != nil {
		t.Fatal("unrelated owner was fenced", err)
	}
	clockA.Store(full.EndsAt.UnixNano())
	ma, err := a.Seal(ctx)
	if err != nil || !ma.Partial || len(ma.Partitions) != 2 || ma.TotalPartitions != 4 {
		t.Fatal("subset seal", ma, err)
	}
	count := 0
	if _, err := ReplayPartition(ctx, full, 0, func(p Position, v Vote) error {
		if p.Partition != 0 || v.Token != keyA {
			t.Fatal("partition reader escaped scope")
		}
		count++
		return nil
	}); err != nil || count != 2 {
		t.Fatal("partition replay", count, err)
	}
	clockB.Store(full.EndsAt.UnixNano())
	if _, err := b.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	seen := make(map[[16]byte]uint32)
	attempts := 0
	manifest, err := Replay(ctx, left, func(_ Position, v Vote) error {
		attempts++
		if _, ok := seen[v.Token]; !ok {
			seen[v.Token] = v.Choice
		}
		return nil
	})
	if err != nil || len(manifest.Partitions) != 4 || attempts != 4 || len(seen) != 3 || seen[keyA] != 1 {
		t.Fatal("full replay silently narrowed scope or changed first choice", attempts, len(seen), err)
	}
}
