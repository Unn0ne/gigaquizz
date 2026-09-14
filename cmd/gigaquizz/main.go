package main

import (
	"context"
	"encoding/json"
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
	"gigaquizz/internal/runtimeconfig"
	"gigaquizz/internal/votelog"
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
	migrateOnly := flag.Bool("migrate-only", false, "apply migrations and exit")
	checkConfig := flag.Bool("check-config", false, "validate configuration and report effective runtime settings without opening storage or listeners")
	flag.Parse()
	if *checkConfig && *cpuProfile != "" {
		return errors.New("check-config cannot create a CPU profile")
	}
	stopProfile, err := startCPUProfile(*cpuProfile)
	if err != nil {
		return err
	}
	defer func() {
		if err := stopProfile(); err != nil {
			runErr = errors.Join(runErr, err)
		}
	}()
	explicitEnv := false
	flag.Visit(func(f *flag.Flag) {
		if f.Name == "env" {
			explicitEnv = true
		}
	})
	if err := config.LoadEnv(*envFile); err != nil && (explicitEnv || !errors.Is(err, os.ErrNotExist)) {
		return errors.New("cannot load environment file")
	}
	cfg, err := config.Load()
	if err != nil {
		return err
	}
	apiConfig, err := httpapi.ValidateConfig(httpapi.Config{AdminPassword: cfg.AdminPassword, PublicURL: cfg.PublicURL, MaxInflight: cfg.MaxInflight, OperationTimeout: 10 * time.Second})
	if err != nil {
		return err
	}
	cfg.PublicURL = apiConfig.PublicURL
	runtimeSettings, err := runtimeconfig.Load()
	if err != nil {
		return err
	}
	if *checkConfig && *migrateOnly {
		return errors.New("check-config cannot apply migrations")
	}
	var security *votelog.ClientSecurity
	if !*migrateOnly {
		security, err = votelog.BuildSecurity(votelog.SecurityOptions{TLS: cfg.KafkaTLS, CAFile: cfg.KafkaTLSCAFile, CertFile: cfg.KafkaTLSCertFile, KeyFile: cfg.KafkaTLSKeyFile, ServerName: cfg.KafkaTLSServerName, SASLMechanism: cfg.KafkaSASLMechanism, Username: cfg.KafkaSASLUsername, Password: cfg.KafkaSASLPassword})
		if err != nil {
			return err
		}
	}
	durability := postgres.DurabilityOptions{RequiredStandbys: cfg.RequiredStandbys, StandbyNames: cfg.StandbyNames}
	storeOptions, err := kafkapoll.ValidateOptions(kafkapoll.Options{DatabaseURL: cfg.DatabaseURL, Schema: cfg.DatabaseSchema, Brokers: cfg.KafkaBrokers, AllowRemoteBrokers: cfg.KafkaAllowRemote, Partitions: cfg.KafkaPartitions, BatchSize: cfg.KafkaBatchVotes, QueuePerPartition: cfg.KafkaQueueVotes, Linger: cfg.KafkaLinger, Security: security, MaxPartitionUnique: cfg.MaxPartitionUnique, MaxUnique: cfg.MaxUnique, MaxPolls: cfg.MaxPolls, PreparationLead: cfg.PreparationLead, Durability: durability})
	if err != nil {
		return err
	}
	runtimeSettings.Apply()
	if *checkConfig {
		return json.NewEncoder(os.Stdout).Encode(map[string]any{
			"mode": "configuration-check", "backend": "postgres-kafka",
			"scope":   "configuration and runtime only; no listener, storage, network or capacity verification",
			"runtime": runtimeconfig.Current(), "max_inflight": cfg.MaxInflight,
			"new_poll_partitions": cfg.KafkaPartitions, "new_poll_batch_votes": cfg.KafkaBatchVotes, "new_poll_queue_per_partition": cfg.KafkaQueueVotes,
			"max_unique_voters": cfg.MaxUnique, "max_partition_unique_voters": cfg.MaxPartitionUnique,
		})
	}
	startup, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	if *migrateOnly {
		if err := kafkapoll.Migrate(startup, cfg.DatabaseURL, cfg.DatabaseSchema); err != nil {
			return errors.New("metadata migration failed (connection details suppressed)")
		}
		slog.Info("migrations applied")
		return nil
	}
	listener, err := net.Listen("tcp", cfg.Addr)
	if err != nil {
		return fmt.Errorf("cannot listen on %s: %w", cfg.Addr, err)
	}
	defer listener.Close()
	store, err := kafkapoll.New(startup, storeOptions)
	if err != nil {
		return errors.New("cannot initialize PostgreSQL/Kafka controller (check broker readiness and exclusive schema ownership)")
	}
	defer store.Close()
	api, err := httpapi.New(store, store, web.Files, apiConfig)
	if err != nil {
		return err
	}
	api.SetReady(true)
	server := &http.Server{Addr: cfg.Addr, Handler: api.Handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 8 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8192, ErrorLog: log.New(io.Discard, "", 0)}
	signals, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	workerCtx, stopWorker := context.WithCancel(context.Background())
	workerDone := make(chan struct{})
	healthGate := &readinessGate{target: api}
	go func() {
		defer close(workerDone)
		healthDone := make(chan struct{})
		go func() { defer close(healthDone); readiness(workerCtx, healthGate, store, time.Second) }()
		maintenance(workerCtx, api, store)
		<-healthDone
	}()
	serveDone := make(chan error, 1)
	go func() { serveDone <- server.Serve(listener) }()
	slog.Info("gigaquizz started", "url", cfg.PublicURL, "admin", cfg.PublicURL+"/admin", "runtime", runtimeconfig.Current())
	select {
	case <-signals.Done():
	case serveErr := <-serveDone:
		healthGate.stop()
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

func maintenance(ctx context.Context, api *httpapi.Server, store *kafkapoll.Store) {
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
