package main

import (
	"context"
	"encoding/json"
	"io"
	"mime"
	"net/http"
	"strings"
	"time"

	"gigaquizz/internal/poll"
)

const journeyStages = 5
const maxAssetBody = 256 * 1024
const maxQuestionBody = 64 * 1024

var journeyNames = [journeyStages]string{"html", "css", "common_js", "poll_js", "question"}

type getStats struct{ Sent, Failures, Bytes, LatencyNS, MaxNS uint64 }
type getReport struct {
	Name     string  `json:"stage"`
	Sent     uint64  `json:"sent"`
	Failures uint64  `json:"failures"`
	Bytes    uint64  `json:"response_body_bytes"`
	MeanMS   float64 `json:"mean_latency_ms"`
	MaxMS    float64 `json:"max_latency_ms"`
}

func dispatchAttempt(ctx context.Context, c config, client *http.Client, a *attempt, s *workerStats, start time.Time) {
	if c.Journey && a.Choice == 1 {
		deadline := start.Add(time.Duration(a.ScheduledNS) + c.MaxLag)
		if !runJourney(ctx, c, client, s, deadline) {
			// This original never reached POST. Its planned position is preserved,
			// and independent replay must reject a persisted vote for this attempt.
			a.State = stateJourneyFailed
			return
		}
	}
	sendAttempt(ctx, c, client, a, s, start)
}

func runJourney(parent context.Context, c config, client *http.Client, s *workerStats, deadline time.Time) bool {
	begin := time.Now()
	s.JourneyStarted++
	defer func() {
		elapsed := uint64(time.Since(begin))
		s.JourneyNS += elapsed
		if elapsed > s.JourneyMaxNS {
			s.JourneyMaxNS = elapsed
		}
	}()
	ctx, cancel := context.WithDeadline(parent, deadline)
	defer cancel()
	paths := [journeyStages]string{"/p/" + c.PollID, "/static/app.css", "/static/common.js", "/static/poll.js", "/api/polls/" + c.PollID}
	for i, path := range paths {
		if ctx.Err() != nil || !getResource(ctx, c, client, path, i, &s.GET[i]) {
			return false
		}
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return false
	}
	s.JourneyCompleted++
	return true
}

func getResource(ctx context.Context, c config, client *http.Client, path string, stage int, s *getStats) (valid bool) {
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.URL, "/")+path, nil)
	if err != nil {
		return false
	}
	begin := time.Now()
	s.Sent++
	defer func() {
		elapsed := uint64(time.Since(begin))
		s.LatencyNS += elapsed
		if elapsed > s.MaxNS {
			s.MaxNS = elapsed
		}
		if !valid {
			s.Failures++
		}
	}()
	resp, err := client.Do(r)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	media, _, mediaErr := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	// Static bodies are streamed into a fixed-size discard buffer. Only the
	// bounded question JSON is retained, so concurrency cannot retain five pages.
	var count int64
	var body []byte
	if stage == journeyStages-1 {
		body, err = io.ReadAll(io.LimitReader(resp.Body, maxQuestionBody+1))
		count = int64(len(body))
	} else {
		count, err = io.Copy(io.Discard, io.LimitReader(resp.Body, maxAssetBody+1))
	}
	s.Bytes += uint64(count)
	closeErr := resp.Body.Close()
	if err != nil || closeErr != nil || resp.StatusCode != 200 || count == 0 || mediaErr != nil || (resp.Header.Get("Content-Encoding") != "" && resp.Header.Get("Content-Encoding") != "identity") {
		return false
	}
	switch stage {
	case 0:
		return count <= maxAssetBody && media == "text/html"
	case 1:
		return count <= maxAssetBody && media == "text/css"
	case 2, 3:
		return count <= maxAssetBody && (media == "text/javascript" || media == "application/javascript")
	case 4:
		var p poll.Poll
		return count <= maxQuestionBody && media == "application/json" && json.Unmarshal(body, &p) == nil && validatePoll(c, p) == nil
	default:
		return false
	}
}
