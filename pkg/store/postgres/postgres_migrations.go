package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/urobora-ai/agentspaces/pkg/store/migrations"
)

var legacyPostgresMigrationChecksums = map[int64]map[string]struct{}{
	6: {
		"5776530a74c7b9f65d49b207bd1555c6f89927245dd1e3f2651b6ef77f38646c": {},
	},
	7: {
		"d01aa52ff1458434284c74fc4c8ba6cb78f9484acfc4f65fab74587944b29aa8": {},
	},
}

const (
	schemaActionValidate = "validate"
	schemaActionMigrate  = "migrate"
	schemaActionRollback = "rollback"
)

// ExecuteSchemaAction runs validate/migrate/rollback against Postgres and returns structured metadata.
func ExecuteSchemaAction(ctx context.Context, cfg *PostgresConfig, action string, requestedVersion int64) (*migrations.Result, error) {
	pool, err := openSchemaPool(ctx, cfg)
	if err != nil {
		return nil, err
	}
	defer pool.Close()
	return runSchemaActionWithPool(ctx, pool, action, requestedVersion)
}

func runSchemaActionWithPool(ctx context.Context, pool *pgxpool.Pool, action string, requestedVersion int64) (*migrations.Result, error) {
	action = strings.ToLower(strings.TrimSpace(action))
	if action == "" {
		action = schemaActionValidate
	}
	if action != schemaActionValidate && action != schemaActionMigrate && action != schemaActionRollback {
		return nil, fmt.Errorf("unsupported postgres schema action: %s", action)
	}

	if err := ensureMigrationTable(ctx, pool); err != nil {
		return nil, err
	}

	upSpecs, err := migrations.Load(migrations.BackendPostgres, migrations.DirectionUp)
	if err != nil {
		return nil, err
	}
	downSpecs, err := migrations.Load(migrations.BackendPostgres, migrations.DirectionDown)
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

	appliedRows, err := loadAppliedRows(ctx, pool)
	if err != nil {
		return nil, err
	}
	if err := reconcileAppliedChecksums(ctx, pool, upByVersion, appliedRows); err != nil {
		return nil, err
	}

	previousVersion, err := currentVersion(ctx, pool)
	if err != nil {
		return nil, err
	}

	result := &migrations.Result{
		Backend:            migrations.BackendPostgres,
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
		// No-op, just compute compatibility after checksum verification.
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
			if err := applyUpSpec(ctx, pool, spec); err != nil {
				return nil, fmt.Errorf("apply postgres migration %d (%s): %w", spec.Version, spec.Name, err)
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
			if err := applyDownSpec(ctx, pool, spec); err != nil {
				return nil, fmt.Errorf("apply postgres rollback %d (%s): %w", spec.Version, spec.Name, err)
			}
			result.RolledBack = append(result.RolledBack, migrations.AppliedStep{
				Version:   spec.Version,
				Name:      spec.Name,
				Direction: migrations.DirectionDown,
				Checksum:  spec.Checksum,
			})
		}
	}

	curr, err := currentVersion(ctx, pool)
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

func openSchemaPool(ctx context.Context, cfg *PostgresConfig) (*pgxpool.Pool, error) {
	if cfg == nil {
		return nil, fmt.Errorf("postgres config is nil")
	}
	connStr, err := buildPostgresConnString(cfg)
	if err != nil {
		return nil, err
	}
	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = int32(cfg.MaxConns)
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = int32(cfg.MinConns)
	}
	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}
	return pool, nil
}

func ensureMigrationTable(ctx context.Context, pool *pgxpool.Pool) error {
	_, err := pool.Exec(ctx, `
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version BIGINT PRIMARY KEY,
			name TEXT NOT NULL,
			checksum TEXT NOT NULL DEFAULT '',
			applied_at TIMESTAMPTZ NOT NULL DEFAULT now()
		);
	`)
	if err != nil {
		return fmt.Errorf("ensure schema_migrations table: %w", err)
	}
	if _, err := pool.Exec(ctx, `ALTER TABLE schema_migrations ADD COLUMN IF NOT EXISTS checksum TEXT NOT NULL DEFAULT ''`); err != nil {
		return fmt.Errorf("ensure schema_migrations.checksum column: %w", err)
	}
	return nil
}

type appliedRow struct {
	Version  int64
	Name     string
	Checksum string
}

func loadAppliedRows(ctx context.Context, pool *pgxpool.Pool) ([]appliedRow, error) {
	rows, err := pool.Query(ctx, `SELECT version, name, checksum FROM schema_migrations ORDER BY version ASC`)
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

func reconcileAppliedChecksums(ctx context.Context, pool *pgxpool.Pool, upByVersion map[int64]migrations.Spec, rows []appliedRow) error {
	for _, row := range rows {
		spec, ok := upByVersion[row.Version]
		if !ok {
			return fmt.Errorf("database has migration version %d with no corresponding up migration file", row.Version)
		}
		existing := strings.TrimSpace(row.Checksum)
		if existing == "" {
			if _, err := pool.Exec(ctx, `UPDATE schema_migrations SET checksum = $1 WHERE version = $2`, spec.Checksum, row.Version); err != nil {
				return fmt.Errorf("backfill migration checksum for version %d: %w", row.Version, err)
			}
			continue
		}
		if !migrationChecksumAccepted(row.Version, existing, spec.Checksum) {
			return fmt.Errorf("checksum mismatch for migration version %d (db=%s file=%s)", row.Version, existing, spec.Checksum)
		}
	}
	return nil
}

func migrationChecksumAccepted(version int64, existing, current string) bool {
	if existing == current {
		return true
	}
	accepted, ok := legacyPostgresMigrationChecksums[version]
	if !ok {
		return false
	}
	_, ok = accepted[existing]
	return ok
}

func currentVersion(ctx context.Context, pool *pgxpool.Pool) (int64, error) {
	var version int64
	if err := pool.QueryRow(ctx, `SELECT COALESCE(MAX(version), 0) FROM schema_migrations`).Scan(&version); err != nil {
		return 0, fmt.Errorf("query current migration version: %w", err)
	}
	return version, nil
}

func applyUpSpec(ctx context.Context, pool *pgxpool.Pool, spec migrations.Spec) error {
	if !spec.Transactional {
		if _, err := pool.Exec(ctx, spec.SQL); err != nil {
			return err
		}
		if _, err := pool.Exec(ctx,
			`INSERT INTO schema_migrations(version, name, checksum) VALUES ($1, $2, $3)
			 ON CONFLICT (version) DO UPDATE SET name = EXCLUDED.name, checksum = EXCLUDED.checksum, applied_at = now()`,
			spec.Version, spec.Name, spec.Checksum,
		); err != nil {
			return err
		}
		return nil
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, spec.SQL); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx,
		`INSERT INTO schema_migrations(version, name, checksum) VALUES ($1, $2, $3)
		 ON CONFLICT (version) DO UPDATE SET name = EXCLUDED.name, checksum = EXCLUDED.checksum, applied_at = now()`,
		spec.Version, spec.Name, spec.Checksum,
	); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func applyDownSpec(ctx context.Context, pool *pgxpool.Pool, spec migrations.Spec) error {
	if !spec.Transactional {
		if _, err := pool.Exec(ctx, spec.SQL); err != nil {
			return err
		}
		if _, err := pool.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, spec.Version); err != nil {
			return err
		}
		return nil
	}
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	if _, err := tx.Exec(ctx, spec.SQL); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `DELETE FROM schema_migrations WHERE version = $1`, spec.Version); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func runSchemaMode(ctx context.Context, pool *pgxpool.Pool, mode string) error {
	mode = strings.ToLower(strings.TrimSpace(mode))
	if mode == "" {
		mode = schemaActionMigrate
	}
	if mode != schemaActionMigrate && mode != schemaActionValidate {
		return fmt.Errorf("unsupported postgres schema mode: %s", mode)
	}
	_, err := runSchemaActionWithPool(ctx, pool, mode, 0)
	return err
}
