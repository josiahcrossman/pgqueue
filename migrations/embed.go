// Package migrations embeds the SQL migration files so the runner can read them
// without depending on the working directory. The .go file lives alongside the
// .sql files because //go:embed patterns cannot reference parent directories.
package migrations

import "embed"

// FS holds every *.sql migration file in this directory.
//
//go:embed *.sql
var FS embed.FS
