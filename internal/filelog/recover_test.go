package filelog

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRecoverResumesOriginalWindowAndSequence(t *testing.T) {
	c := testConfig(t)
	s, _ := testStore(t, c)
	first, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s.Close()
	r, err := Recover(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	second, err := r.SubmitFrame(context.Background(), []Input{testInput(2, 2)})
	if err != nil || first.Offset != 0 || second.Offset != 1 {
		t.Fatalf("recovered sequence: %+v %+v %v", first, second, err)
	}
	if !r.c.StartsAt.Equal(c.StartsAt) || !r.c.EndsAt.Equal(c.EndsAt) {
		t.Fatal("restart changed the poll window")
	}
	r.now = func() time.Time { return c.EndsAt }
	if _, err := r.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	closed, err := Recover(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer closed.Close()
	closed.now = func() time.Time { return c.StartsAt.Add(time.Second) }
	if _, err := closed.SubmitFrame(context.Background(), []Input{testInput(3, 1)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("recovered CLOSED reopened: %v", err)
	}
	count := 0
	if _, err := Replay(context.Background(), c, func(Position, Vote) error { count++; return nil }); err != nil || count != 2 {
		t.Fatalf("lost recovered votes: %d %v", count, err)
	}
}

func TestRecoverAfterDeadlineSealsWithoutExtendingAdmission(t *testing.T) {
	c := testConfig(t)
	c.StartsAt = time.Now().Add(-2 * time.Minute)
	c.EndsAt = c.StartsAt.Add(time.Minute)
	s, _ := testStore(t, c)
	if _, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1)}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	r, err := Recover(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := r.SubmitFrame(context.Background(), []Input{testInput(2, 1)}); !errors.Is(err, ErrClosed) {
		t.Fatalf("expired journal accepted after restart: %v", err)
	}
	if _, err := r.Seal(context.Background()); err != nil {
		t.Fatal(err)
	}
	audit, err := Scan(context.Background(), c, nil)
	if err != nil || !audit.Closed || audit.Votes != 1 {
		t.Fatalf("expired recovery: %+v %v", audit, err)
	}
}

func TestRecoverRepairsOnlyIncompleteTail(t *testing.T) {
	for _, partial := range []int{17, frameHeaderBytes + 7} {
		t.Run(time.Duration(partial).String(), func(t *testing.T) {
			c := testConfig(t)
			s, _ := testStore(t, c)
			if _, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1)}); err != nil {
				t.Fatal(err)
			}
			s.Close()
			path := filepath.Join(c.Directory, walName)
			frame := encodeInputs([]Input{testInput(9, 2)}, c.StartsAt.Add(time.Second))
			finishFrame(frame, 1, kindVotes, 1)
			f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := f.Write(frame[:partial]); err != nil {
				f.Close()
				t.Fatal(err)
			}
			f.Close()
			r, err := Recover(context.Background(), c)
			if err != nil {
				t.Fatal(err)
			}
			defer r.Close()
			info, err := os.Stat(path)
			if err != nil || info.Size() != fileHeaderBytes+frameHeaderBytes+entryBytes {
				t.Fatalf("tail not repaired: %v %v", info, err)
			}
			receipt, err := r.SubmitFrame(context.Background(), []Input{testInput(2, 1)})
			if err != nil || receipt.Offset != 1 {
				t.Fatalf("replacement frame: %+v %v", receipt, err)
			}
			r.Close()
			var tokens [][16]byte
			audit, err := Scan(context.Background(), c, func(_ Position, v Vote) error { tokens = append(tokens, v.Token); return nil })
			if err != nil || audit.IncompleteTail || len(tokens) != 2 || tokens[1] != testInput(2, 1).Token {
				t.Fatalf("repair exposed incomplete frame: %+v %v %v", audit, tokens, err)
			}
		})
	}
}

func TestRecoverCorruptionNeverTruncatesAndReleasesLock(t *testing.T) {
	c := testConfig(t)
	s, _ := testStore(t, c)
	if _, err := s.SubmitFrame(context.Background(), []Input{testInput(1, 1)}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	path := filepath.Join(c.Directory, walName)
	b, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	b[fileHeaderBytes+frameHeaderBytes] ^= 1
	if err := os.WriteFile(path, b, 0600); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := Recover(context.Background(), c); err == nil {
			t.Fatal("corrupt frame recovered")
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || string(after) != string(b) {
		t.Fatal("corrupt journal was altered")
	}
}

func TestRecoverExcludesConcurrentWriter(t *testing.T) {
	c := testConfig(t)
	s, _ := testStore(t, c)
	if _, err := Recover(context.Background(), c); err == nil {
		t.Fatal("recovery fenced neither live owner nor itself")
	}
	s.Close()
	r, err := Recover(context.Background(), c)
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	if _, err := Recover(context.Background(), c); err == nil {
		t.Fatal("two recovered owners admitted")
	}
}
