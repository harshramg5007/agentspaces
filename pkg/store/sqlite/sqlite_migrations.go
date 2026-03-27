package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/urobora-ai/agentspaces/pkg/store/migrations"
)

const (
	schemaActionValidate = "validate"
	schemaActionMigrate  = "migrate"
	schemaActionRollback = "rollback"
)

// ExecuteSchemaAction runs validate/migrate/rollback against SQLite and returns structured metadata.
func ExecuteSchemaAction(ctx context.Context, cfg *SQLiteConfig, action string, requestedVersion int64) (*migrations.Result, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sqlite config is nil")
	}
	if ctx == nil {
		ctx = context.Background()
	}

	dsn, path, err := buildSQLiteDSN(cfg)
	if err != nil {
		return nil, err
	}
	if path != "" {
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return nil, fmt.Errorf("failed to create sqlite dir: %w", err)
		}
	}

	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to open sqlite db: %w", err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	if err := applySQLitePragmas(db, cfg); err != nil {
		return nil, err
	}

	return runSchemaActionWithDB(ctx, db, action, requestedVersion)
}

func runSchemaActionWithDB(ctx context.Context, db *sql.DB, action string, requestedVersion int64) (*migrations.Result, error) {
	action = strings.ToLower(strings.TrimSpace(action))
	if action == "" {
		action = schemaActionValidate
	}
	if action != schemaActionValidate && action != schemaActionMigrate && action != schemaActionRollback {
		return nil, fmt.Errorf("unsupported sqlite schema action: %s", action)
	}

	if err := ensureMigrationTable(ctx, db); err != nil {
		return nil, err
	}

	upSpecs, err := migrations.Load(migrations.BackendSQLite, migrations.DirectionUp)
	if err != nil {
		return nil, err
	}
	downSpecs, err := migrations.Load(migrations.BackendSQLite, migrations.DirectionDown)
	if err != nil {
		return nil, err
	}

	upByVersion := make(map[int64]migrations.Spec, len(upSpecs))
	for _, spec := range upSpecs {
		upByVersion[spec.Version] = spec
	}
	downByVersion := make(map[int64]migrations.Spec, len(downSpecs))
	for _, spec := range downSpecs {
		downByVersion[spec.Version] = spec
	}

	appliedRows, err := loadAppliedRows(ctx, db)
	if err != nil {
		return nil, err
	}
	if err := reconcileAppliedChecksums(ctx, db, upByVersion, appliedRows); err != nil {
		return nil, err
	}

	previousVersion, err := currentVersion(ctx, db)
	if err != nil {
		return nil, err
	}

	result := &migrations.Result{
		Backend:            migrations.BackendSQLite,
		Action:             action,
		RequestedVersion:   requestedVersion,
		PreviousVersion:    previousVersion,
		CurrentVersion:     previousVersion,
		TargetVersion:      migrations.SchemaTargetVersion,
		MinCompatVersion:   migrations.SchemaMinCompatVersion,
		CompatibilityRange: [2]int64{migrations.SchemaMinCompatVersion, migrations.SchemaTargetVersion},
		Compatible:         false,
		Applied:            []migrations.AppliedStep{},
		RolledBack:         []migrations.AppliedStep{},
	}

	switch action {
	case schemaActionValidate:
		// no-op; compatibility is evaluated after checksum reconciliation.
	case schemaActionMigrate:
		target := requestedVersion
		if target <= 0 {
			target = migrations.SchemaTargetVersion
		}
		if target > migrations.SchemaTargetVersion {
			return nil, fmt.Errorf("requested migration target %d exceeds schema target %d", target, migrations.SchemaTargetVersion)
		}
		if target < previousVersion {
			return nil, fmt.Errorf("requested migration target %d is below current version %d; use rollback", target, previousVersion)
		}
		result.RequestedVersion = target

		for _, spec := range upSpecs {
			if spec.Version <= previousVersion || spec.Version > target {
				continue
			}
			if err := applyUpSpec(ctx, db, spec); err != nil {
				return nil, fmt.Errorf("apply sqlite migration %d (%s): %w", spec.Version, spec.Name, err)
			}
			result.Applied = append(result.Applied, migrations.AppliedStep{
				Version:   spec.Version,
				Name:      spec.Name,
				Direction: migrations.DirectionUp,
				Checksum:  spec.Checksum,
			})
		}
	case schemaActionRollback:
		if requestedVersion < 0 {
			return nil, fmt.Errorf("rollback requires explicit target version >= 0")
		}
		if requestedVersion > previousVersion {
			return nil, fmt.Errorf("rollback target %d exceeds current version %d", requestedVersion, previousVersion)
		}
		result.RequestedVersion = requestedVersion
		for v := previousVersion; v > requestedVersion; v-- {
			spec, ok := downByVersion[v]
			if !ok {
				return nil, fmt.Errorf("missing down migration for version %d", v)
			}
			if err := applyDownSpec(ctx, db, spec); err != nil {
				return nil, fmt.Errorf("apply sqlite rollback %d (%s): %w", spec.Version, spec.Name, err)
			}
			result.RolledBack = append(result.RolledBack, migrations.AppliedStep{
				Version:   spec.Version,
				Name:      spec.Name,
				Direction: migrations.DirectionDown,
				Checksum:  spec.Checksum,
			})
		}
	}

	curr, err := currentVersion(ctx, db)
	if err != nil {
		return nil, err
	}
	result.CurrentVersion = curr
	result.Compatible = curr >= migrations.SchemaMinCompatVersion && curr <= migrations.SchemaTargetVersion
	if action == schemaActionValidate && !result.Compatible {
		return nil, fmt.Errorf("schema version %d outside compatible window [%d, %d]", curr, migrations.SchemaMinCompatVersion, migrations.SchemaTargetVersion)
	}
	return result, nil
}

func ensureMigrationTable(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version INTEGER PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NOT NULL DEFAULT '',
			applied_at INTEGER NOT NULL
		);
	`); err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	if _, err := db.ExecContext(ctx, `ALTER TABLE schema_migrations ADD COLUMN checksum TEXT NOT NULL DEFAULT ''`); err != nil && !sqliteIgnoreColumnErr(err) {
		return fmt.Errorf("ensure schema_migrations.checksum column: %w", err)
	}
	return nil
}

type appliedRow struct {
	Version  int64
	Name     string
	Checksum string
}

func loadAppliedRows(ctx context.Context, db *sql.DB) ([]appliedRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT version, name, checksum FROM schema_migrations ORDER BY version ASC`)
	if err != nil {
		return nil, fmt.Errorf("load applied migrations: %w", err)
	}
	defer rows.Close()

	result := make([]appliedRow, 0)
	for rows.Next() {
		var row appliedRow
		if err := rows.Scan(&row.Version, &row.Name, &row.Checksum); err != nil {
			return nil, fmt.Errorf("scan applied migration: %w", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate applied migrations: %w", err)
	}
	return result, nil
}

func reconcileAppliedChecksums(ctx context.Context, db *sql.DB, upByVersion map[int64]migrations.Spec, rows []appliedRow) error {
	for _, row := range rows {
		spec, ok := upByVersion[row.Version]
		if !ok {
			return fmt.Errorf("database has migration version %d with no corresponding up migration file", row.Version)
		}
		existing := strings.TrimSpace(row.Checksum)
		if existing == "" {
			if _, err := db.ExecContext(ctx, `UPDATE schema_migrations SET checksum = ? WHERE version = ?`, spec.Checksum, row.Version); err != nil {
				return fmt.Errorf("backfill migration checksum for version %d: %w", row.Version, err)
			}
			continue
		}
		if existing != spec.Checksum {
			return fmt.Errorf("checksum mismatch for migration version %d (db=%s file=%s)", row.Version, existing, spec.Checksum)
		}
	}
	return nil
}

func currentVersion(ctx context.Context, db *sql.DB) (int64, error) {
	var version int64
	if err := db.QueryRowContext(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		return 0, fmt.Errorf("query current migration version: %w", err)
	}
	return version, nil
}

func applyUpSpec(ctx context.Context, db *sql.DB, spec migrations.Spec) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, spec.SQL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO schema_migrations(version, name, checksum, applied_at)
		VALUES (?, ?, ?, CAST(unixepoch('now') * 1000 AS INTEGER))
		ON CONFLICT(version) DO UPDATE SET
			name = excluded.name,
			checksum = excluded.checksum,
			applied_at = CAST(unixepoch('now') * 1000 AS INTEGER)
	`, spec.Version, spec.Name, spec.Checksum); err != nil {
		return err
	}
	return tx.Commit()
}

func applyDownSpec(ctx context.Context, db *sql.DB, spec migrations.Spec) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, spec.SQL); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM schema_migrations WHERE version = ?`, spec.Version); err != nil {
		return err
	}
	return tx.Commit()
}

func runSchemaMode(ctx context.Context, db *sql.DB, mode string) error {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = schemaActionMigrate
	}
	if mode != schemaActionMigrate && mode != schemaActionValidate {
		return fmt.Errorf("unsupported sqlite schema mode: %s", mode)
	}
	_, err := runSchemaActionWithDB(ctx, db, mode, 0)
	return err
}

func sqliteIgnoreColumnErr(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "duplicate column") || strings.Contains(msg, "already exists")
}
