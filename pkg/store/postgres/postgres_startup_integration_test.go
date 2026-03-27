package postgres

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/store/migrations"
)

func TestIntegrationPostgresStartupPreflight(t *testing.T) {
	if os.Getenv("RUN_INTEGRATION_TESTS") != "1" {
		t.Skip("Skipping integration test. Set RUN_INTEGRATION_TESTS=1 to run.")
	}

	ctx := context.Background()
	cfg := integrationPostgresConfig(t, "postgres")
	adminConnStr, err := buildPostgresConnString(cfg)
	if err != nil {
		t.Fatalf("build admin connection string: %v", err)
	}
	admin, err := pgx.Connect(ctx, adminConnStr)
	if err != nil {
		t.Fatalf("connect admin postgres: %v", err)
	}
	defer func() {
		_ = admin.Close(ctx)
	}()

	testDB := fmt.Sprintf("agent_startup_%d", time.Now().UTC().UnixNano())
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+pgQuoteIdentifier(testDB)); err != nil {
		t.Fatalf("create test database %s: %v", testDB, err)
	}
	defer func() {
		dropCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, _ = admin.Exec(dropCtx, `SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = $1 AND pid <> pg_backend_pid()`, testDB)
		_, _ = admin.Exec(dropCtx, "DROP DATABASE IF EXISTS "+pgQuoteIdentifier(testDB))
	}()

	testCfg := integrationPostgresConfig(t, testDB)
	testCfg.SchemaMode = schemaActionValidate

	pool, err := openSchemaPool(ctx, testCfg)
	if err != nil {
		t.Fatalf("open schema pool: %v", err)
	}
	result, err := runSchemaActionWithPool(ctx, pool, schemaActionMigrate, 0)
	pool.Close()
	if err != nil {
		t.Fatalf("migrate fresh database: %v", err)
	}
	if result.CurrentVersion != migrations.SchemaTargetVersion {
		t.Fatalf("current schema version = %d, want %d", result.CurrentVersion, migrations.SchemaTargetVersion)
	}

	store, err := NewPostgresStore(ctx, testCfg, zap.NewNop())
	if err != nil {
		t.Fatalf("start postgres store: %v", err)
	}
	defer func() {
		_ = store.Close()
	}()

	assertDailyPartitionsExist(t, ctx, store.pool, "agent_events", 8)
	assertDailyPartitionsExist(t, ctx, store.pool, "telemetry_events", 8)
}

func integrationPostgresConfig(t *testing.T, database string) *PostgresConfig {
	t.Helper()

	port := 5432
	if raw := os.Getenv("POSTGRES_PORT"); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil {
			t.Fatalf("parse POSTGRES_PORT: %v", err)
		}
		port = parsed
	}
	if database == "" {
		database = os.Getenv("POSTGRES_DATABASE")
	}
	return &PostgresConfig{
		Host:           getenvOrDefault("POSTGRES_HOST", "localhost"),
		Port:           port,
		User:           getenvOrDefault("POSTGRES_USER", "postgres"),
		Password:       os.Getenv("POSTGRES_PASSWORD"),
		Database:       database,
		SSLMode:        "disable",
		SchemaMode:     schemaActionValidate,
		MaxConns:       10,
		MinConns:       1,
		QueueClaimMode: "strict",
	}
}

func assertDailyPartitionsExist(t *testing.T, ctx context.Context, pool queryer, parentTable string, totalDays int) {
	t.Helper()

	rows, err := pool.Query(ctx, `
		SELECT child.relname
		FROM pg_inherits
		JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
		JOIN pg_class child ON pg_inherits.inhrelid = child.oid
		JOIN pg_namespace parent_ns ON parent.relnamespace = parent_ns.oid
		JOIN pg_namespace child_ns ON child.relnamespace = child_ns.oid
		WHERE parent_ns.nspname = current_schema()
		  AND child_ns.nspname = current_schema()
		  AND parent.relname = $1
	`, parentTable)
	if err != nil {
		t.Fatalf("query partitions for %s: %v", parentTable, err)
	}
	defer rows.Close()

	seen := make(map[string]struct{})
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatalf("scan partition name: %v", err)
		}
		seen[name] = struct{}{}
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate partition names: %v", err)
	}

	start := truncateToUTCDate(time.Now().UTC())
	for day := 0; day < totalDays; day++ {
		name := fmt.Sprintf("%s_%s", parentTable, start.AddDate(0, 0, day).Format("20060102"))
		if _, ok := seen[name]; !ok {
			t.Fatalf("missing partition %s for parent %s", name, parentTable)
		}
	}
}

type queryer interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

func getenvOrDefault(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
