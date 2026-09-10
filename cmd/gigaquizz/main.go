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
	"gigaquizz/internal/kafkapoll"
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
	startup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	durability := postgres.DurabilityOptions{RequiredStandbys: cfg.RequiredStandbys, StandbyNames: cfg.StandbyNames}
	if *migrateOnly {
		if err := kafkapoll.Migrate(startup, cfg.DatabaseURL, cfg.DatabaseSchema); err != nil {
			return errors.New("metadata migration failed (connection details suppressed)")
		}
		slog.Info("migrations applied")
		return nil
	}
	store, err := kafkapoll.New(startup, kafkapoll.Options{DatabaseURL: cfg.DatabaseURL, Schema: cfg.DatabaseSchema, Brokers: cfg.KafkaBrokers, AllowRemoteBrokers: cfg.KafkaAllowRemote, Partitions: cfg.KafkaPartitions, MaxUnique: cfg.MaxUnique, MaxPolls: cfg.MaxPolls, PreparationLead: cfg.PreparationLead, Durability: durability})
	if err != nil {
		return errors.New("cannot initialize PostgreSQL/Kafka controller (check broker readiness and exclusive schema ownership)")
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
	go func() { defer close(workerDone); maintenance(workerCtx, api, store) }()
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

func maintenance(ctx context.Context, api *httpapi.Server, store *kafkapoll.Store) {
	healthDone := make(chan struct{})
	go func() {
		defer close(healthDone)
		tick := time.NewTicker(time.Second)
		defer tick.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-tick.C:
			}
			ping, cancel := context.WithTimeout(ctx, time.Second)
			api.SetReady(store.Ping(ping) == nil)
			cancel()
		}
	}()
	defer func() { <-healthDone }()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	var ticks int
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
		work, cancel := context.WithTimeout(ctx, 5*time.Minute)
		_, err := store.FinalizeDue(work)
		cancel()
		ticks++
		if ticks%30 == 0 {
			slog.Info("service counters", "counters", api.Metrics(), "finalization_healthy", err == nil)
		}
	}
}
