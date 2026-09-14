// Package config loads worker configuration from environment variables.
package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds everything the worker and queue need to run. Values come from
// the environment via Load; every field has a default except DatabaseURL.
type Config struct {
	DatabaseURL      string        // DATABASE_URL (required)
	WorkerCount      int           // WORKER_COUNT
	PollInterval     time.Duration // POLL_INTERVAL
	BatchSize        int           // BATCH_SIZE
	MaxAttempts      int           // MAX_ATTEMPTS
	BaseBackoff      time.Duration // BASE_BACKOFF
	StaleLockTimeout time.Duration // STALE_LOCK_TIMEOUT
}

// Load reads configuration from the environment, applies defaults for anything
// unset, and validates the result. DATABASE_URL is the only required variable.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL:      os.Getenv("DATABASE_URL"),
		WorkerCount:      4,
		PollInterval:     500 * time.Millisecond,
		BatchSize:        10,
		MaxAttempts:      5,
		BaseBackoff:      1 * time.Second,
		StaleLockTimeout: 5 * time.Minute,
	}

	var err error
	if cfg.WorkerCount, err = intEnv("WORKER_COUNT", cfg.WorkerCount); err != nil {
		return Config{}, err
	}
	if cfg.PollInterval, err = durationEnv("POLL_INTERVAL", cfg.PollInterval); err != nil {
		return Config{}, err
	}
	if cfg.BatchSize, err = intEnv("BATCH_SIZE", cfg.BatchSize); err != nil {
		return Config{}, err
	}
	if cfg.MaxAttempts, err = intEnv("MAX_ATTEMPTS", cfg.MaxAttempts); err != nil {
		return Config{}, err
	}
	if cfg.BaseBackoff, err = durationEnv("BASE_BACKOFF", cfg.BaseBackoff); err != nil {
		return Config{}, err
	}
	if cfg.StaleLockTimeout, err = durationEnv("STALE_LOCK_TIMEOUT", cfg.StaleLockTimeout); err != nil {
		return Config{}, err
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

func (c Config) validate() error {
	if c.DatabaseURL == "" {
		return fmt.Errorf("config: DATABASE_URL is required")
	}
	if c.WorkerCount < 1 {
		return fmt.Errorf("config: WORKER_COUNT must be >= 1, got %d", c.WorkerCount)
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("config: POLL_INTERVAL must be > 0, got %s", c.PollInterval)
	}
	if c.BatchSize < 1 {
		return fmt.Errorf("config: BATCH_SIZE must be >= 1, got %d", c.BatchSize)
	}
	if c.MaxAttempts < 1 {
		return fmt.Errorf("config: MAX_ATTEMPTS must be >= 1, got %d", c.MaxAttempts)
	}
	if c.BaseBackoff <= 0 {
		return fmt.Errorf("config: BASE_BACKOFF must be > 0, got %s", c.BaseBackoff)
	}
	if c.StaleLockTimeout <= 0 {
		return fmt.Errorf("config: STALE_LOCK_TIMEOUT must be > 0, got %s", c.StaleLockTimeout)
	}
	return nil
}

func intEnv(key string, def int) (int, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return def, nil
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s: invalid integer %q: %w", key, raw, err)
	}
	return v, nil
}

func durationEnv(key string, def time.Duration) (time.Duration, error) {
	raw := os.Getenv(key)
	if raw == "" {
		return def, nil
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("config: %s: invalid duration %q: %w", key, raw, err)
	}
	return v, nil
}
