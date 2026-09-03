// Command worker runs the job-processing worker pool until it receives
// SIGINT or SIGTERM, then drains in-flight jobs and exits.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/josiahcrossman/pgqueue/internal/config"
	"github.com/josiahcrossman/pgqueue/internal/example"
	"github.com/josiahcrossman/pgqueue/internal/queue"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(log)

	if err := run(log); err != nil {
		log.Error("worker exited with error", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	cfg, err := config.Load()
	if err != nil {
		return err
	}

	// signal.NotifyContext cancels ctx on SIGINT/SIGTERM. The worker pool
	// observes the cancellation, stops claiming new work, finishes in-flight
	// jobs, and returns.
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	pool, err := queue.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	q := queue.New(pool, cfg, log)

	registry := queue.NewRegistry()
	registry.Register(example.EmailJobType, example.NewEmailHandler(log))

	worker := queue.NewWorker(q, registry, cfg, log)

	log.Info("worker started; send SIGINT/SIGTERM to drain and exit")
	if err := worker.Run(ctx); err != nil {
		return err
	}
	log.Info("worker drained cleanly")
	return nil
}
