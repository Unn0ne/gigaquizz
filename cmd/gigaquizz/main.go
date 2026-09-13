package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"gigaquizz/internal/config"
	"gigaquizz/internal/filestore"
	"gigaquizz/internal/httpapi"
	"gigaquizz/internal/poll"
	"gigaquizz/internal/web"
)

func main() {
	if err := run(); err != nil {
		slog.Error("startup or shutdown failed", "error", err.Error())
		os.Exit(1)
	}
}

func run() (runErr error) {
	envFile := flag.String("env", ".env", "configuration file")
	cpuProfile := flag.String("cpu-profile", "", "write CPU profile to a new private local file (includes startup and shutdown)")
	flag.Parse()
	stopProfile, err := startCPUProfile(*cpuProfile)
	if err != nil {
		return err
	}
	defer func() {
		if err := stopProfile(); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}()
	if err := config.LoadEnv(*envFile); err != nil {
		return errors.New("cannot load environment file")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	store, err := filestore.New(startup, filestore.Config{Directory: cfg.DataDir, MaxUnique: cfg.MaxUnique, MaxPartitionUnique: cfg.MaxPartitionUnique, Partitions: cfg.Partitions, BatchSize: cfg.BatchSize, QueueVotes: cfg.QueueVotes, Linger: cfg.Linger})
	if err != nil {
		return fmt.Errorf("cannot open file storage: %w", err)
	}
	defer store.Close()
	api, err := httpapi.New(store, store, web.Files, httpapi.Config{AdminPassword: cfg.AdminPassword, PublicURL: cfg.PublicURL, MaxInflight: cfg.MaxInflight, OperationTimeout: 10 * time.Second})
	if err != nil {
		return err
	}
	api.SetReady(true)
	server := &http.Server{Addr: cfg.Addr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 8 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192, ErrorLog: log.New(io.Discard, "", 0)}
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("cannot listen on %s: %w", cfg.Addr, err)
	}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	healthGate := &readinessGate{target: api}
	go func() {
		defer close(workerDone)
		readyDone := make(chan struct{})
		go func() { defer close(readyDone); readiness(workerCtx, healthGate, store, 5*time.Second) }()
		maintenance(workerCtx, api, store)
		<-readyDone
	}()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	slog.Info("gigaquizz started", "url", cfg.PublicURL, "admin", cfg.PublicURL+"/admin")
	select {
	case <-signals.Done():
	case serveErr := <-serveDone:
		stopWorker()
		<-workerDone
		if !errors.Is(serveErr, http.ErrServerClosed) {
			return errors.New("HTTP listener failed")
		}
		return nil
	}
	healthGate.stop()
	shutdown, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutdownCancel()
	err = server.Shutdown(shutdown)
	if err != nil {
		_ = server.Close()
	}
	stopWorker()
	<-workerDone
	slog.Info("gigaquizz stopped", "counters", api.Metrics())
	return err
}

func maintenance(ctx context.Context, api *httpapi.Server, store poll.Repository) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var ticks int
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		_, err := store.FinalizeDue(ctx)
		ticks++

		if ticks%30 == 0 {
			slog.Info("service counters", "counters", api.Metrics(), "finalization_healthy", err == nil)
		}
	}
}

// Health checks have their own goroutine; a large historical replay cannot
// suspend readiness updates for the currently admitting writer.
func readiness(ctx context.Context, api interface{ SetReady(bool) }, store interface{ Ping(context.Context) error }, interval time.Duration) {
	tick := time.NewTicker(interval)
	defer tick.Stop()
	for {
		ping, cancel := context.WithTimeout(ctx, time.Second)
		err := store.Ping(ping)
		cancel()
		if ctx.Err() != nil {
			return
		}
		api.SetReady(err == nil)
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

// An in-flight health check cannot re-enable readiness after shutdown starts.
// This gate does not wait for a possibly blocked Ping or finalization syscall.
type readinessGate struct {
	mu      sync.Mutex
	stopped bool
	target  interface{ SetReady(bool) }
}

func (g *readinessGate) SetReady(ready bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if !g.stopped {
		g.target.SetReady(ready)
	}
}
func (g *readinessGate) stop() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.stopped = true
	g.target.SetReady(false)
}
