package filestore

import (
	"context"
	"fmt"
	"testing"
	"time"

	"gigaquizz/internal/filelog"
	"gigaquizz/internal/poll"
)

func TestRepositoryVotesUsePackedSingleWriterAndPreserveFirstChoice(t *testing.T) {
	c := configForTest(t)
	c.BatchSize, c.Linger = 8, time.Second
	s := openTest(t, c)
	p, err := s.Create(context.Background(), poll.CreateInput{Question: "q", Type: "ab", Options: []string{"A", "B"}})
	if err != nil {
		t.Fatal(err)
	}
	type response struct {
		token   string
		choice  int
		receipt poll.Receipt
		err     error
	}
	responses := make(chan response, 8)
	submit := func(token string, choice int) {
		go func() {
			r, err := s.Vote(context.Background(), p.ID, token, []int{choice})
			responses <- response{token, choice, r, err}
		}()
	}
	submit(tokenA, 1)
	e := s.lookup(p.ID)
	deadline := time.Now().Add(3 * time.Second)
	active := false
	for time.Now().Before(deadline) {
		e.mu.Lock()
		w := e.writer
		e.mu.Unlock()
		if w != nil && w.Metrics()["active_votes"] == 1 {
			active = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	if !active {
		t.Fatal("first HTTP repository attempt never entered writer")
	}
	// Launch a conflicting retry only after the first choice was admitted.
	// The remaining calls fill one sync group without changing its deadline.
	submit(tokenA, 2)
	for i := 2; i < 8; i++ {
		submit(fmt.Sprintf("%032x", i), 2)
	}
	var received []response
	for range 8 {
		select {
		case r := <-responses:
			if r.err != nil || r.receipt.Status != "recorded" || len(r.receipt.Choices) != 1 || r.receipt.Choices[0] != r.choice || r.receipt.AcceptedAt == nil {
				t.Fatalf("durable attempt receipt: %+v", r)
			}
			received = append(received, r)
		case <-time.After(3 * time.Second):
			t.Fatal("packed repository vote did not return")
		}
	}
	s.Close()
	canonical := make(map[[16]byte]uint32)
	matched := make([]bool, len(received))
	scan, err := filelog.Scan(context.Background(), e.logConfig(), func(position filelog.Position, vote filelog.Vote) error {
		if position.Offset != 0 {
			t.Errorf("repository still writes one physical frame per vote: %+v", position)
		}
		found := false
		for i, r := range received {
			key, _ := parseToken(r.token)
			if !matched[i] && vote.Token == key && vote.Choice == 1<<uint(r.choice-1) && vote.AdmittedAt.Equal(*r.receipt.AcceptedAt) {
				matched[i], found = true, true
				break
			}
		}
		if !found {
			t.Errorf("durable entry does not match any exact attempt receipt: %+v", position)
		}
		if _, ok := canonical[vote.Token]; !ok {
			canonical[vote.Token] = vote.Choice
		}
		return nil
	})
	a, _ := parseToken(tokenA)
	if err != nil || scan.Votes != 8 || scan.Frames != 1 || len(canonical) != 7 || canonical[a] != 1 {
		t.Fatalf("packed repository journal: %+v canonical=%v err=%v", scan, canonical, err)
	}
	for _, ok := range matched {
		if !ok {
			t.Fatal("recorded response is absent from durable journal")
		}
	}
}
