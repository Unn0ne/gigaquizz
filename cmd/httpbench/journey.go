package main

import (
	"bufio"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"mime"
	"net"
	"net/http"
	"strings"
	"time"

	"gigaquizz/internal/poll"
)

const journeyStages = 5
const maxAssetBody = 256 * 1024
const maxQuestionBody = 64 * 1024

var journeyNames = [journeyStages]string{"html", "css", "common_js", "poll_js", "question"}

type getStats struct {
	Sent, Failures, Bytes, DecodedBytes, LatencyNS, MaxNS           uint64
	Timeouts, TransportFailures, StatusFailures, ValidationFailures uint64
}
type getReport struct {
	Timeouts           uint64  `json:"timeouts"`
	TransportFailures  uint64  `json:"transport_failures"`
	StatusFailures     uint64  `json:"status_failures"`
	ValidationFailures uint64  `json:"validation_failures"`
	DecodedBytes       uint64  `json:"decoded_body_bytes"`
	Name               string  `json:"stage"`
	Sent               uint64  `json:"sent"`
	Failures           uint64  `json:"failures"`
	Bytes              uint64  `json:"response_body_bytes"`
	MeanMS             float64 `json:"mean_latency_ms"`
	MaxMS              float64 `json:"max_latency_ms"`
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
	if c.Definition {
		paths[journeyStages-1] += "/definition"
	}
	for i, path := range paths {
		if ctx.Err() != nil || !getResource(ctx, c, client, path, i, &s.GET[i], &s.Decoder) {
			return false
		}
	}
	if ctx.Err() != nil || !time.Now().Before(deadline) {
		return false
	}
	s.JourneyCompleted++
	return true
}

// countedBody measures encoded response bytes; both encoded and expanded bodies
// are bounded independently so malformed compressed responses cannot grow memory.
type countedBody struct {
	r io.Reader
	n uint64
}

func (r *countedBody) Read(b []byte) (int, error) {
	n, err := r.r.Read(b)
	r.n += uint64(n)
	return n, err
}

// One decoder belongs to one sequential HTTP worker. Reusing flate state
// prevents every GET from allocating a new compression dictionary. Reset also
// releases the previous response so cancelled connections are not retained.
type journeyDecoder struct {
	input *bufio.Reader
	gzip  *gzip.Reader
}

func (d *journeyDecoder) open(r io.Reader) (*gzip.Reader, error) {
	if d.input == nil {
		d.input = bufio.NewReaderSize(r, 4096)
	} else {
		d.input.Reset(r)
	}
	var err error
	if d.gzip == nil {
		d.gzip, err = gzip.NewReader(d.input)
	} else {
		err = d.gzip.Reset(d.input)
	}
	if err != nil {
		d.input.Reset(nil)
		return nil, err
	}
	return d.gzip, nil
}
func (d *journeyDecoder) release() { _ = d.gzip.Close(); d.input.Reset(nil) }

func getResource(ctx context.Context, c config, client *http.Client, path string, stage int, s *getStats, decoder *journeyDecoder) (valid bool) {
	r, err := http.NewRequestWithContext(ctx, http.MethodGet, strings.TrimRight(c.URL, "/")+path, nil)
	if err != nil {
		return false
	}
	if c.Definition {
		r.Header.Set("Accept-Encoding", "gzip")
	}
	begin := time.Now()
	s.Sent++
	// Exactly one failure category per failed GET.
	category := "validation"
	defer func() {
		elapsed := uint64(time.Since(begin))
		s.LatencyNS += elapsed
		if elapsed > s.MaxNS {
			s.MaxNS = elapsed
		}
		if !valid {
			s.Failures++
			switch category {
			case "timeout":
				s.Timeouts++
			case "transport":
				s.TransportFailures++
			case "status":
				s.StatusFailures++
			default:
				s.ValidationFailures++
			}
		}
	}()
	classifyIO := func(err error) {
		var ne net.Error
		if errors.Is(err, context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
			category = "timeout"
		} else {
			category = "transport"
		}
	}
	resp, err := client.Do(r)
	if err != nil {
		classifyIO(err)
		return false
	}
	defer resp.Body.Close()
	media, _, mediaErr := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	limit := int64(maxAssetBody)
	if stage == journeyStages-1 {
		limit = maxQuestionBody
	}
	counter := &countedBody{r: io.LimitReader(resp.Body, limit+1)}
	defer func() { s.Bytes += counter.n }()
	var bodyReader io.Reader = counter
	encoding := strings.ToLower(strings.TrimSpace(resp.Header.Get("Content-Encoding")))
	if encoding == "gzip" && c.Definition {
		z, e := decoder.open(counter)
		if e != nil {
			return false
		}
		defer decoder.release()
		bodyReader = z
	} else if encoding != "" && encoding != "identity" {
		return false
	}
	var count int64
	var body []byte
	if stage == journeyStages-1 {
		body, err = io.ReadAll(io.LimitReader(bodyReader, limit+1))
		count = int64(len(body))
	} else {
		count, err = io.Copy(io.Discard, io.LimitReader(bodyReader, limit+1))
	}
	s.DecodedBytes += uint64(count)
	closeErr := resp.Body.Close()
	if err != nil || closeErr != nil {
		// A truncated/invalid gzip is a validation failure; an actual deadline or
		// socket failure is classified separately where the error retains its type.
		var ne net.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.As(err, &ne) {
			classifyIO(err)
		} else if encoding != "gzip" {
			classifyIO(err)
		}
		return false
	}
	if resp.StatusCode != 200 {
		category = "status"
		return false
	}
	if count == 0 || count > limit || counter.n > uint64(limit) || mediaErr != nil {
		return false
	}
	switch stage {
	case 0:
		return media == "text/html"
	case 1:
		return media == "text/css"
	case 2, 3:
		return media == "text/javascript" || media == "application/javascript"
	case 4:
		var p poll.Poll
		return media == "application/json" && json.Unmarshal(body, &p) == nil && validatePoll(c, p) == nil
	default:
		return false
	}
}
