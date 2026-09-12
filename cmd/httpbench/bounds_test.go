package main

import "testing"

func TestWorkerIncreasePreservesOtherWorkloadBounds(t *testing.T) {
	m, _, _, _ := fixture(t)
	for _, workers := range []int{1, 128, 2048, 4096} {
		c := m.Config
		c.Workers = workers
		if err := c.validate(); err != nil {
			t.Fatalf("workers=%d rejected: %v", workers, err)
		}
	}
	for _, workers := range []int{0, -1, 4097} {
		c := m.Config
		c.Workers = workers
		if err := c.validate(); err == nil {
			t.Fatalf("workers=%d accepted", workers)
		}
	}
	c := m.Config
	c.Workers = 4096
	c.Rate = 100000
	if err := c.validate(); err != nil {
		t.Fatalf("existing rate ceiling with bounded repeat workload rejected: %v", err)
	}
	c.Rate = 100001
	if err := c.validate(); err == nil {
		t.Fatal("worker increase changed the 100k unique/s ceiling")
	}
	c.Rate = 100000
	c.RepeatEvery = 1 // 6M originals + 6M repeats exceeds the unchanged 10M cap.
	if err := c.validate(); err == nil {
		t.Fatal("worker increase changed the total-attempt ceiling")
	}
}
