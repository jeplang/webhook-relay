// Command relay is the webhook-relay entry point: it loads config, opens the
// store, and serves the HTTP API (the delivery worker is wired on Day 2).
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"webhook-relay/internal/api"
	"webhook-relay/internal/config"
	"webhook-relay/internal/logging"
	"webhook-relay/internal/metrics"
	"webhook-relay/internal/store"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "relay:", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	log, err := logging.New(cfg.LogLevel, os.Stdout)
	if err != nil {
		return err
	}

	st, err := store.NewSQLStore(cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("store: %w", err)
	}
	defer func() { _ = st.Close() }()

	if err := st.Ping(context.Background()); err != nil {
		return fmt.Errorf("db ping: %w", err)
	}

	m := metrics.New()
	h := &api.Handler{Store: st, Log: log, Metrics: m}

	mux := http.NewServeMux()
	mux.Handle("GET /metrics", m.Handler)
	mux.Handle("/", api.NewRouter(h))

	srv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	// SIGTERM/SIGINT → stop accepting, drain, exit (≤ 30s per §10 Day 3).
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() {
		log.Info("listening", slog.String("component", "api"), slog.String("addr", cfg.HTTPAddr))
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutdown signal received")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("shutdown: %w", err)
		}
	}
	log.Info("stopped")
	return nil
}
