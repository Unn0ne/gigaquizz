package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gigaquizz/internal/filelog"
)

func TestOfflineReaderMatchesRealClosedFileJournal(t *testing.T) {
	m, a, _, res := fixture(t)
	m.Config.Start = time.Now().Add(-59 * time.Second).UTC()
	m.Poll.StartsAt = m.Config.Start
	m.Poll.EndsAt = m.Config.Start.Add(time.Minute)
	id, _ := parseID(m.Config.PollID)
	c := filelog.Config{Directory: filepath.Join(t.TempDir(), "journal"), PollID: id, StartsAt: m.Poll.StartsAt, EndsAt: m.Poll.EndsAt, AllowedMask: 3, Partitions: 1, BatchSize: 16, QueuePerPartition: 16}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	w, err := filelog.New(ctx, c)
	if err != nil {
		t.Fatal(err)
	}
	defer w.Close()
	repeat, _ := sequence(m.Config, 1, 2)
	receipt, err := w.SubmitFrame(ctx, []filelog.Input{{Token: a[0].Token, Choice: 1}, {Token: a[repeat].Token, Choice: 2}, {Token: a[1].Token, Choice: 1}})
	if err != nil {
		t.Fatal(err)
	}
	a[0].AdmittedNS = receipt.AdmittedAt.UnixNano()
	a[repeat].AdmittedNS = receipt.AdmittedAt.UnixNano()
	if _, err = w.Seal(ctx); err != nil {
		t.Fatal(err)
	}
	w.Close()
	path := writeFixture(t, m, a)
	encoded, _ := json.Marshal(map[string]any{"results": res})
	resultPath := filepath.Join(m.Config.Directory, "service-result.json")
	if err = os.WriteFile(resultPath, encoded, 0600); err != nil {
		t.Fatal(err)
	}
	r, err := runAudit(ctx, path, c.Directory, "", resultPath)
	if err != nil || !r.Correct || r.Matched != 2 || r.Resolved != 1 || r.Recorded != 3 {
		t.Fatalf("real-journal reconciliation %+v %v", r, err)
	}
}
