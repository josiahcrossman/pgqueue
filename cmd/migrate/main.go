// Command migrate applies or rolls back database migrations.
//
//	go run ./cmd/migrate up     # apply all pending migrations
//	go run ./cmd/migrate down   # roll back the most recent migration
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"

	"github.com/josiahcrossman/pgqueue/internal/config"
	"github.com/josiahcrossman/pgqueue/internal/queue"
)

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	if err := run(log); err != nil {
		log.Error("migrate failed", "error", err)
		os.Exit(1)
	}
}

func run(log *slog.Logger) error {
	direction := "up"
	if len(os.Args) > 1 {
		direction = os.Args[1]
	}

	cfg, err := config.Load()
	if err != nil {
		return err
	}

	ctx := context.Background()
	pool, err := queue.Connect(ctx, cfg)
	if err != nil {
		return err
	}
	defer pool.Close()

	switch direction {
	case "up":
		return queue.Migrate(ctx, pool, log)
	case "down":
		return queue.Rollback(ctx, pool, log)
	default:
		return fmt.Errorf("unknown direction %q: use 'up' or 'down'", direction)
	}
}
