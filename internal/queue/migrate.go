package queue

import (
	"context"
	"fmt"
	"io/fs"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/josiahcrossman/pgqueue/migrations"
)

// migration is one versioned pair of up/down SQL files.
type migration struct {
	version int
	name    string
	up      string
	down    string
}

// Migrate applies every up migration that has not yet been recorded, in
// version order, inside a transaction per migration. It is safe to run
// repeatedly. This runner is fully implemented; it does not read the contents
// of the SQL you write in 0001_init.up.sql beyond executing it.
func Migrate(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	if err := ensureMigrationsTable(ctx, pool); err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return err
	}
	migs, err := loadMigrations()
	if err != nil {
		return err
	}

	for _, m := range migs {
		if applied[m.version] {
			continue
		}
		if strings.TrimSpace(stripSQLComments(m.up)) == "" {
			log.Warn("migration has no executable statements (still recorded as applied)",
				"version", m.version, "name", m.name)
		}
		if err := applyOne(ctx, pool, m); err != nil {
			return fmt.Errorf("migrate up %04d_%s: %w", m.version, m.name, err)
		}
		log.Info("applied migration", "version", m.version, "name", m.name)
	}
	return nil
}

// Rollback reverts the single most recently applied migration.
func Rollback(ctx context.Context, pool *pgxpool.Pool, log *slog.Logger) error {
	if err := ensureMigrationsTable(ctx, pool); err != nil {
		return err
	}
	applied, err := appliedVersions(ctx, pool)
	if err != nil {
		return err
	}
	migs, err := loadMigrations()
	if err != nil {
		return err
	}

	// Find the highest applied version.
	latest := -1
	var target migration
	for _, m := range migs {
		if applied[m.version] && m.version > latest {
			latest = m.version
			target = m
		}
	}
	if latest == -1 {
		log.Info("no migrations to roll back")
		return nil
	}

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, target.down); err != nil {
		return fmt.Errorf("migrate down %04d_%s: %w", target.version, target.name, err)
	}
	if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, target.version); err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	log.Info("rolled back migration", "version", target.version, "name", target.name)
	return nil
}

func ensureMigrationsTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    integer PRIMARY KEY,
			name       text NOT NULL,
			applied_at timestamptz NOT NULL DEFAULT now()
		)`)
	return err
}

func appliedVersions(ctx context.Context, pool *pgxpool.Pool) (map[int]bool, error) {
	rows, err := pool.Query(ctx, `SELECT version FROM schema_migrations`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	applied := make(map[int]bool)
	for rows.Next() {
		var v int
		if err := rows.Scan(&v); err != nil {
			return nil, err
		}
		applied[v] = true
	}
	return applied, rows.Err()
}

func applyOne(ctx context.Context, pool *pgxpool.Pool, m migration) error {
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if strings.TrimSpace(m.up) != "" {
		if _, err := tx.Exec(ctx, m.up); err != nil {
			return err
		}
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations (version, name) VALUES ($1, $2)`,
		m.version, m.name); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

// loadMigrations reads the embedded migrations directory and pairs up/down
// files by version. File names must look like NNNN_name.up.sql / .down.sql.
func loadMigrations() ([]migration, error) {
	entries, err := fs.ReadDir(migrations.FS, ".")
	if err != nil {
		return nil, err
	}

	byVersion := make(map[int]*migration)
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		var direction string
		switch {
		case strings.HasSuffix(name, ".up.sql"):
			direction = "up"
		case strings.HasSuffix(name, ".down.sql"):
			direction = "down"
		default:
			continue
		}

		version, label, err := parseVersion(name)
		if err != nil {
			return nil, err
		}
		body, err := fs.ReadFile(migrations.FS, name)
		if err != nil {
			return nil, err
		}

		m := byVersion[version]
		if m == nil {
			m = &migration{version: version, name: label}
			byVersion[version] = m
		}
		if direction == "up" {
			m.up = string(body)
		} else {
			m.down = string(body)
		}
	}

	migs := make([]migration, 0, len(byVersion))
	for _, m := range byVersion {
		migs = append(migs, *m)
	}
	sort.Slice(migs, func(i, j int) bool { return migs[i].version < migs[j].version })
	return migs, nil
}

// parseVersion turns "0001_init.up.sql" into (1, "init").
func parseVersion(filename string) (int, string, error) {
	base := strings.TrimSuffix(strings.TrimSuffix(filename, ".up.sql"), ".down.sql")
	parts := strings.SplitN(base, "_", 2)
	if len(parts) != 2 {
		return 0, "", fmt.Errorf("migration filename %q must be NNNN_name.up|down.sql", filename)
	}
	var version int
	if _, err := fmt.Sscanf(parts[0], "%d", &version); err != nil {
		return 0, "", fmt.Errorf("migration filename %q has non-numeric version: %w", filename, err)
	}
	return version, parts[1], nil
}

// stripSQLComments removes -- line comments so we can detect an all-comment
// (i.e. stub) migration file.
func stripSQLComments(sql string) string {
	var b strings.Builder
	for _, line := range strings.Split(sql, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--") {
			continue
		}
		b.WriteString(line)
		b.WriteString("\n")
	}
	return b.String()
}
