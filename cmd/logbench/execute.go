package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sync"
	"time"

	"gigaquizz/internal/votelog"
)

func execute(parent context.Context, c config) (r report) {
	start := time.Now()
	defer func() { r.WallSeconds = time.Since(start).Seconds() }()
	r.Mode = c.mode
	r.Errors = []string{}
	r.Limitations = []string{
		"202 records an attempt, not a final unique vote; canonical results are derived after CLOSED.",
		"Generator, HTTP handler and Kafka brokers share one computer; this is not a 1.7M/s capacity validation or independent failure zones.",
		"HTTP mode uses the real experimental handler over loopback HTTP, without external routing, TLS, CDN or browser traffic.",
		"Generator queues and admission queues are bounded; skipped slots were never submitted and are separate from unknown and missing confirmed records.",
		"Kafka transaction acknowledgements use replicated logs and do not claim simultaneous all-copy power-loss survival or PostgreSQL-style remote physical flush proofs.",
		"One static writer owner per partition; automatic production ownership assignment is not implemented.",
		fmt.Sprintf("Latency arrays and the private receipt ledger are bounded at %d attempts; raw ledger tokens are synthetic and never included in this report.", maximumAttempts),
	}
	ctx, cancel := context.WithTimeout(parent, 4*time.Minute)
	defer cancel()
	if c.auditOnly != "" || c.recoverAndSeal != "" {
		r.Mode = "audit-only"
		ledgerPath := c.auditOnly
		if c.recoverAndSeal != "" {
			r.Mode = "recover-and-seal"
			ledgerPath = c.recoverAndSeal
		}
		h, entries, err := readLedger(ledgerPath)
		if err != nil {
			r.Errors = append(r.Errors, err.Error())
			return r
		}
		h.Config.Brokers, _ = localBrokers(c.brokers)
		r.Topic = h.Config.Topic
		r.Ledger = ledgerPath
		if c.recoverAndSeal != "" {
			r.Limitations = append(r.Limitations, "Explicit manual ownership transfer after checking original owner PID is absent; no concurrent automatic failover or distributed ownership service.")
			unlock, err := lockRecoveryLedger(ledgerPath)
			if err != nil {
				r.Errors = append(r.Errors, err.Error())
				return r
			}
			defer unlock()
			if err := requireStoppedOwner(h.OwnerPID); err != nil {
				r.Errors = append(r.Errors, err.Error())
				return r
			}
			recoverCtx, recoverCancel := context.WithTimeout(ctx, 90*time.Second)
			store, err := votelog.New(recoverCtx, h.Config)
			recoverCancel()
			if err != nil {
				r.Errors = append(r.Errors, fmt.Sprintf("explicit writer recovery: %v", err))
				return r
			}
			sealCtx, sealCancel := context.WithTimeout(ctx, max(0, time.Until(h.Config.EndsAt))+45*time.Second)
			_, err = store.Seal(sealCtx)
			sealCancel()
			r.StoreMetrics = store.Metrics()
			store.Close()
			if err != nil {
				r.Errors = append(r.Errors, fmt.Sprintf("seal recovered journal: %v", err))
				return r
			}
		}
		auditCtx, auditCancel := context.WithTimeout(ctx, 90*time.Second)
		defer auditCancel()
		a, err := runAudit(auditCtx, h, entries)
		r.Audit = &a
		if err != nil {
			r.Errors = append(r.Errors, err.Error())
		}
		return r
	}
	var id [16]byte
	var prefix [8]byte
	if _, err := rand.Read(id[:]); err != nil {
		r.Errors = append(r.Errors, "cannot generate fixture ID")
		return r
	}
	if _, err := rand.Read(prefix[:]); err != nil {
		r.Errors = append(r.Errors, "cannot generate synthetic key prefix")
		return r
	}
	entries := makeEntries(c, prefix)
	// Check retained ledger and free-space bounds before any broker writes.
	if err := prepareLedgerDirectory(".local/logbench", len(entries)); err != nil {
		r.Errors = append(r.Errors, err.Error())
		return r
	}
	brokers, _ := localBrokers(c.brokers)
	startsAt := time.Now().Add(15 * time.Second).UTC()
	cfg := votelog.Config{Brokers: brokers, Topic: "gigaquizz_logbench_" + hex.EncodeToString(id[:8]), PollID: id, Partitions: c.partitions, StartsAt: startsAt, EndsAt: startsAt.Add(time.Minute), AllowedMask: 3, Multiple: false, BatchSize: c.batchSize, Linger: c.linger, QueuePerPartition: c.partitionQueue, TransactionTimeout: c.transactionTimeout}
	r.Topic = cfg.Topic
	r.Configuration = map[string]any{"mode": c.mode, "unique_key_rate_per_second": c.rate, "duration_seconds": c.duration.Seconds(), "poll_window_seconds": 60, "workers": c.workers, "generator_queue": c.queue, "max_lag_ms": float64(c.maxLag) / 1e6, "attempt_timeout_seconds": c.timeout.Seconds(), "drain_seconds": c.drain.Seconds(), "repeat_every": c.repeatEvery, "different_choice": c.different, "partitions": c.partitions, "batch_size": c.batchSize, "linger_ms": float64(c.linger) / 1e6, "queue_per_partition": c.partitionQueue, "replication_factor": 3, "minimum_in_sync_replicas": 2, "producer_acks": "all", "transaction_commit_required_before_202": true, "starts_at": cfg.StartsAt, "ends_at": cfg.EndsAt, "ledger_encoding": "72-byte fixed entries, JSON configuration header, SHA-256 integrity footer; mode 0600"}
	r.Configuration["transaction_timeout_seconds"] = cfg.TransactionTimeout.Seconds()
	setupCtx, setupCancel := context.WithTimeout(ctx, 90*time.Second)
	if err := votelog.CreateTopic(setupCtx, cfg); err != nil {
		setupCancel()
		r.Errors = append(r.Errors, fmt.Sprintf("create owned topic: %v", err))
		return r
	}
	store, err := votelog.New(setupCtx, cfg)
	setupCancel()
	if err != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("initialize and warm journal writers: %v", err))
		return r
	}
	defer store.Close()
	var submit submitter
	if c.mode == "direct" {
		submit = directSubmitter(store)
	} else {
		server := httptest.NewServer(store.Handler())
		defer server.Close()
		transport := &http.Transport{DialContext: (&net.Dialer{Timeout: c.timeout, KeepAlive: time.Minute}).DialContext, MaxIdleConns: c.workers, MaxIdleConnsPerHost: c.workers, MaxConnsPerHost: c.workers, IdleConnTimeout: time.Minute, ResponseHeaderTimeout: c.timeout}
		defer transport.CloseIdleConnections()
		client := &http.Client{Transport: transport, Timeout: c.timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
		warmHTTP(ctx, client, server.URL, c.workers)
		submit = httpSubmitter(client, server.URL+"/vote")
	}
	if !time.Now().Before(cfg.StartsAt.Add(-100 * time.Millisecond)) {
		r.Errors = append(r.Errors, "writer/HTTP warmup consumed the 15-second preparation lead; no workload was scheduled")
		return r
	}
	r.BoundaryChecks = map[string]string{}
	var probe [16]byte
	copy(probe[:8], prefix[:]) // sequence zero is reserved for boundary probes.
	probeCtx, probeCancel := context.WithTimeout(ctx, c.timeout)
	before := submit(probeCtx, probe, 1)
	probeCancel()
	r.BoundaryChecks["before_start"] = before.Outcome.String()
	if before.Outcome != notOpen {
		r.Errors = append(r.Errors, "before-start probe was not rejected as not_open")
	}
	finishDiagnostics, err := startWorkloadDiagnostics()
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
		return r
	}
	work := runWorkload(ctx, c, cfg.StartsAt, entries, submit)
	diagnostics := finishDiagnostics()
	r.Diagnostics = &diagnostics
	r.Workload = &work
	h := ledgerHeader{Version: 1, Config: cfg, Mode: c.mode, Entries: len(entries), Keys: c.keys(), Rate: c.rate, OwnerPID: os.Getpid()}
	path, ledgerErr := writeLedger(".local/logbench", h, entries)
	r.Ledger = path
	if ledgerErr != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("write private receipt ledger: %v", ledgerErr))
	}
	// Seal uses the actual 60-second poll deadline even when arrivals were shorter.
	sealCtx, sealCancel := context.WithTimeout(ctx, time.Until(cfg.EndsAt)+45*time.Second)
	_, sealErr := store.Seal(sealCtx)
	sealCancel()
	r.StoreMetrics = store.Metrics()
	if sealErr != nil {
		r.Errors = append(r.Errors, fmt.Sprintf("seal committed journal: %v", sealErr))
		return r
	}
	probeCtx, probeCancel = context.WithTimeout(ctx, c.timeout)
	after := submit(probeCtx, probe, 1)
	probeCancel()
	r.BoundaryChecks["after_closed_new_token"] = after.Outcome.String()
	if after.Outcome != closed {
		r.Errors = append(r.Errors, "after-close new-token probe was not rejected as closed")
	}
	if len(entries) > 0 {
		probeCtx, probeCancel = context.WithTimeout(ctx, c.timeout)
		repeat := submit(probeCtx, entries[0].Token, entries[0].Choice)
		probeCancel()
		r.BoundaryChecks["after_closed_existing_token"] = repeat.Outcome.String()
		if repeat.Outcome != closed {
			r.Errors = append(r.Errors, "after-close existing-token probe was not rejected as closed")
		}
	}
	auditCtx, auditCancel := context.WithTimeout(ctx, 90*time.Second)
	defer auditCancel()
	a, err := runAudit(auditCtx, h, entries)
	r.Audit = &a
	if err != nil {
		r.Errors = append(r.Errors, err.Error())
	}
	return r
}

func directSubmitter(store *votelog.Store) submitter {
	return func(ctx context.Context, token [16]byte, choice uint32) attemptResult {
		receipt, err := store.Submit(ctx, token, choice)
		r := attemptResult{Outcome: classify(err), Partition: receipt.Partition, Offset: receipt.Offset, AdmittedAt: receipt.AdmittedAt}
		return r
	}
}

func classify(err error) outcome {
	switch {
	case err == nil:
		return recorded
	case errors.Is(err, votelog.ErrNotOpen):
		return notOpen
	case errors.Is(err, votelog.ErrClosed):
		return closed
	case errors.Is(err, votelog.ErrBusy):
		return busy
	case errors.Is(err, votelog.ErrInvalid):
		return rejected
	default:
		return unknown
	}
}

func httpSubmitter(client *http.Client, url string) submitter {
	return func(ctx context.Context, token [16]byte, choice uint32) attemptResult {
		body, _ := json.Marshal(struct {
			Token  string `json:"token"`
			Choice uint32 `json:"choice"`
		}{hex.EncodeToString(token[:]), choice})
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			return attemptResult{Outcome: unknown}
		}
		req.Header.Set("Content-Type", "application/json")
		response, err := client.Do(req)
		if err != nil {
			return attemptResult{Outcome: unknown}
		}
		defer response.Body.Close()
		var r attemptResult
		switch response.StatusCode {
		case http.StatusAccepted:
			var receipt votelog.Receipt
			if err = json.NewDecoder(io.LimitReader(response.Body, 4096)).Decode(&receipt); err != nil || receipt.Partition < 0 || receipt.Offset < 0 || receipt.AdmittedAt.IsZero() {
				return attemptResult{Outcome: unknown}
			}
			r = attemptResult{Outcome: recorded, Partition: receipt.Partition, Offset: receipt.Offset, AdmittedAt: receipt.AdmittedAt}
		case http.StatusTooEarly:
			r.Outcome = notOpen
		case http.StatusGone:
			r.Outcome = closed
		case http.StatusTooManyRequests:
			r.Outcome = busy
		case http.StatusBadRequest:
			r.Outcome = rejected
		default:
			r.Outcome = unknown
		}
		_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
		return r
	}
}

func warmHTTP(ctx context.Context, client *http.Client, url string, workers int) {
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			req, err := http.NewRequestWithContext(ctx, http.MethodGet, url+"/healthz", nil)
			if err != nil {
				return
			}
			response, err := client.Do(req)
			if err == nil {
				_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 4096))
				_ = response.Body.Close()
			}
		})
	}
	wg.Wait()
}
