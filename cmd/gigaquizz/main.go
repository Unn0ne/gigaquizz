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
	"syscall"
	"time"

	"gigaquizz/internal/config"
	"gigaquizz/internal/httpapi"
	"gigaquizz/internal/postgres"
	"gigaquizz/internal/web"
)

func main() {
	if err := run(); err != nil {
		slog.Error("startup or shutdown failed", "error", err.Error())
		os.Exit(1)
	}
}

func run() error {
	envFile := flag.String("env", ".env", "configuration file")
	migrateOnly := flag.Bool("migrate-only", false, "apply migrations and exit")
	flag.Parse()
	if err := config.LoadEnv(*envFile); err != nil {
		return errors.New("cannot load environment file")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	startup, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	durability := postgres.DurabilityOptions{RequiredStandbys: cfg.RequiredStandbys, StandbyNames: cfg.StandbyNames}
	admin, err := postgres.NewWithOptions(startup, cfg.DatabaseURL, 8, durability)
	if err != nil {
		return errors.New("cannot connect to PostgreSQL (connection details suppressed)")
	}
	defer admin.Close()
	if err := admin.Migrate(startup); err != nil {
		return errors.New("database migration failed (query details suppressed)")
	}
	if *migrateOnly {
		slog.Info("migrations applied")
		return nil
	}
	votes, err := postgres.NewWithOptions(startup, cfg.DatabaseURL, cfg.VoteConnections, durability)
	if err != nil {
		return errors.New("cannot open vote connection pool")
	}
	defer votes.Close()
	if err := votes.Warm(startup); err != nil {
		return errors.New("cannot warm vote connection pool before serving traffic")
	}
	if err := admin.Warm(startup); err != nil {
		return errors.New("cannot warm administrative connection pool before serving traffic")
	}
	api, err := httpapi.New(votes, admin, web.Files, httpapi.Config{AdminPassword: cfg.AdminPassword, PublicURL: cfg.PublicURL, MaxInflight: cfg.MaxInflight, OperationTimeout: 10 * time.Second})
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
	go func() { defer close(workerDone); maintenance(workerCtx, api, admin) }()
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
	api.SetReady(false)
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

func maintenance(ctx context.Context, api *httpapi.Server, store *postgres.Store) {
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var ticks int
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		work, cancel := context.WithTimeout(ctx, 15*time.Second)
		_, err := store.FinalizeDue(work)
		cancel()
		ticks++
		if ticks%5 == 0 {
			ping, pingCancel := context.WithTimeout(ctx, time.Second)
			api.SetReady(store.Ping(ping) == nil)
			pingCancel()
		}
		if ticks%30 == 0 {
			slog.Info("service counters", "counters", api.Metrics(), "finalization_healthy", err == nil)
		}
	}
}
