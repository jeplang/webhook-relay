// Package config loads webhook-relay configuration from environment variables.
// No config file in MVP; env is enough (see PROJECT_BRIEF.md §8.7).
package config

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all runtime configuration for the relay.
type Config struct {
	DatabaseURL    string
	HTTPAddr       string
	WorkerBatch    int
	WorkerInterval time.Duration
	StaleInFlight  time.Duration
	HTTPTimeout    time.Duration
	LogLevel       string
}

// Load reads RELAY_* environment variables. RELAY_DATABASE_URL is required;
// everything else has a documented default.
func Load() (Config, error) {
	var c Config

	c.DatabaseURL = os.Getenv("RELAY_DATABASE_URL")
	if c.DatabaseURL == "" {
		return c, errors.New("config: RELAY_DATABASE_URL is required")
	}

	var err error
	if c.HTTPAddr, err = envStr("RELAY_HTTP_ADDR", ":8080"); err != nil {
		return c, err
	}
	if c.WorkerBatch, err = envInt("RELAY_WORKER_BATCH", 50); err != nil {
		return c, err
	}
	if c.WorkerInterval, err = envDur("RELAY_WORKER_INTERVAL", time.Second); err != nil {
		return c, err
	}
	if c.StaleInFlight, err = envDur("RELAY_STALE_IN_FLIGHT", 5*time.Minute); err != nil {
		return c, err
	}
	if c.HTTPTimeout, err = envDur("RELAY_HTTP_TIMEOUT", 10*time.Second); err != nil {
		return c, err
	}
	if c.LogLevel, err = envStr("RELAY_LOG_LEVEL", "info"); err != nil {
		return c, err
	}

	if c.WorkerBatch <= 0 {
		return c, errors.New("config: RELAY_WORKER_BATCH must be > 0")
	}
	if c.WorkerInterval <= 0 {
		return c, errors.New("config: RELAY_WORKER_INTERVAL must be > 0")
	}
	if c.HTTPTimeout <= 0 {
		return c, errors.New("config: RELAY_HTTP_TIMEOUT must be > 0")
	}
	return c, nil
}

func envStr(key, def string) (string, error) {
	if v := os.Getenv(key); v != "" {
		return v, nil
	}
	return def, nil
}

func envInt(key string, def int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s: %w", key, err)
	}
	return n, nil
}

func envDur(key string, def time.Duration) (time.Duration, error) {
	v := os.Getenv(key)
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("config: %s: %w", key, err)
	}
	return d, nil
}
