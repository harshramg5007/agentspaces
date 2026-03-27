package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/internal/api"
	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/metrics"
	"github.com/urobora-ai/agentspaces/pkg/store"
	postgresbackend "github.com/urobora-ai/agentspaces/pkg/store/postgres"
	postgresshardedbackend "github.com/urobora-ai/agentspaces/pkg/store/postgressharded"
	sqlitebackend "github.com/urobora-ai/agentspaces/pkg/store/sqlite"
	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

var (
	cfgFile string
	logger  *zap.Logger
)

type httpRuntimeOptions struct {
	enableSecurityHeaders bool
	enableCORS            bool
	enableRequestTimeout  bool
	enableMaxBody         bool
}

type telemetryCapability interface {
	RecordSpans(ctx context.Context, spans []telemetry.Span) error
	ListSpans(ctx context.Context, traceID string, limit, offset int) ([]telemetry.Span, error)
}

type telemetryRetentionStore interface {
	CleanupSpansBefore(ctx context.Context, cutoff time.Time) (int64, error)
}

var rootCmd = &cobra.Command{
	Use:   "agent-server",
	Short: "Agent Space Server for AI Agent Coordination",
	Long: `A distributed agent space server that enables AI agents to coordinate
through Linda-style primitives with modern features like event sourcing,
DAG tracking, and strong consistency.`,
	Run: runServer,
}

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is ./config.yaml)")
	rootCmd.PersistentFlags().String("runtime-profile", "dev", "Runtime profile (dev, production, benchmark)")
	rootCmd.PersistentFlags().String("store-type", "postgres", "Storage backend type (postgres, postgres-sharded, sqlite-file, sqlite-memory)")
	rootCmd.PersistentFlags().String("postgres-host", "localhost", "PostgreSQL host")
	rootCmd.PersistentFlags().Int("postgres-port", 5432, "PostgreSQL port")
	rootCmd.PersistentFlags().String("postgres-user", "postgres", "PostgreSQL user")
	rootCmd.PersistentFlags().String("postgres-password", "", "PostgreSQL password")
	rootCmd.PersistentFlags().String("postgres-database", "agent_space", "PostgreSQL database")
	rootCmd.PersistentFlags().Int("postgres-max-conns", 25, "PostgreSQL max pool connections")
	rootCmd.PersistentFlags().Int("postgres-min-conns", 5, "PostgreSQL min pool connections")
	rootCmd.PersistentFlags().String("postgres-ssl-mode", "disable", "PostgreSQL SSL mode (disable, require, verify-ca, verify-full)")
	rootCmd.PersistentFlags().String("shard-map-file", "./config/shard_map.json", "Static shard map file for postgres-sharded runtime")
	rootCmd.PersistentFlags().String("node-id", "", "Node identity for postgres-sharded runtime")
	rootCmd.PersistentFlags().String("advertise-addr", "", "Advertised HTTP address for postgres-sharded runtime")
	rootCmd.PersistentFlags().String("shard-map-refresh-interval", "0s", "Optional shard-map refresh interval for postgres-sharded runtime")
	rootCmd.Flags().String("postgres-queue-claim-mode", "strict", "PostgreSQL queue claim mode (strict, parallel)")
	rootCmd.Flags().Bool("slim-events", false, "Store slim CREATED event payloads (omit full agent body)")
	rootCmd.PersistentFlags().String("sqlite-path", "./data/sqlite/agent_space.db", "SQLite database file path")
	rootCmd.PersistentFlags().Bool("sqlite-inmemory", false, "Run SQLite in memory mode (no persistence)")
	rootCmd.PersistentFlags().String("sqlite-busy-timeout", "5s", "SQLite busy timeout (e.g. 5s)")
	rootCmd.PersistentFlags().String("sqlite-journal-mode", "WAL", "SQLite journal mode (WAL, DELETE, TRUNCATE, etc.)")
	rootCmd.PersistentFlags().String("sqlite-synchronous", "NORMAL", "SQLite synchronous mode (OFF, NORMAL, FULL)")
	rootCmd.PersistentFlags().Int("sqlite-cache-kb", 16384, "SQLite cache size in KB")
	rootCmd.Flags().String("http-addr", "127.0.0.1:8080", "HTTP server address")
	rootCmd.Flags().String("api-key", "", "API key for authentication (empty for no auth)")
	rootCmd.Flags().String("auth-keys-file", "", "Path to JSON file containing namespace-bound API keys")
	rootCmd.Flags().String("legacy-namespace-id", "", "Namespace ID for legacy single API key mode")
	rootCmd.Flags().String("auth-mode", "optional", "Auth mode (optional, required)")
	rootCmd.PersistentFlags().String("schema-mode", "", "Schema startup mode (migrate, validate)")
	rootCmd.Flags().String("cors-allow-origins", "", "Comma-separated CORS allowlist origins (empty uses profile defaults)")
	rootCmd.Flags().String("cors-allow-methods", "GET,POST,PUT,DELETE,OPTIONS", "Comma-separated CORS allowed methods")
	rootCmd.Flags().String("cors-allow-headers", "Content-Type,Authorization,X-API-Key", "Comma-separated CORS allowed headers")
	rootCmd.Flags().Int("cors-max-age-seconds", 86400, "CORS preflight max-age in seconds")
	rootCmd.Flags().Int("rate-limit-requests", 0, "Mutating request limit per client per window (0 disables)")
	rootCmd.Flags().String("rate-limit-window", "1s", "Rate limit window duration")
	rootCmd.Flags().Int64("http-max-body-bytes", 1048576, "Maximum request body size in bytes for mutating endpoints")
	rootCmd.Flags().String("http-request-timeout", "30s", "Per-request timeout for mutating endpoints")
	rootCmd.Flags().String("http-read-timeout", "30s", "HTTP server read timeout")
	rootCmd.Flags().String("http-read-header-timeout", "10s", "HTTP server read header timeout")
	rootCmd.Flags().String("http-write-timeout", "30s", "HTTP server write timeout")
	rootCmd.Flags().String("http-idle-timeout", "120s", "HTTP server idle timeout")
	rootCmd.Flags().String("telemetry-mode", "optional", "Telemetry mode (disabled, optional, required)")
	rootCmd.Flags().String("telemetry-retention", "168h", "Telemetry retention duration for supported backends (0 disables cleanup)")
	rootCmd.Flags().String("telemetry-cleanup-interval", "1h", "Telemetry cleanup interval for supported backends (0 disables cleanup)")
	rootCmd.Flags().String("metrics-agent-refresh-interval", "5s", "Agent metrics refresh interval (0 disables periodic agent metrics)")
	rootCmd.Flags().String("metrics-agent-kinds", "fault_task", "Comma-separated agent kinds to track backlog metrics for")
	rootCmd.Flags().String("trusted-proxies", "", "Comma-separated trusted proxy CIDRs for X-Forwarded-For (empty trusts none)")
	rootCmd.Flags().String("log-level", "info", "Log level (debug, info, warn, error)")
	rootCmd.Flags().Bool("log-events", false, "Log all agent events (debug-level)")
	rootCmd.Flags().Bool("http-log-requests", true, "Enable HTTP request logging middleware")
	rootCmd.Flags().Bool("http-metrics", true, "Enable HTTP metrics middleware")
	rootCmd.Flags().Bool("defer-events", true, "Defer event writes to async batch writer for higher throughput")
	rootCmd.Flags().Int("event-batch-size", 100, "Batch size for deferred event writer")
	rootCmd.Flags().Bool("prefetch-enabled", false, "Enable queue-head prefetch for parallel claim mode")
	rootCmd.Flags().Int("prefetch-buffer-size", 64, "Per-queue prefetch candidate buffer size")
	rootCmd.Flags().Int("prefetch-batch-size", 32, "Number of candidate IDs to pre-select per cycle")

	viper.BindPFlag("runtime.profile", rootCmd.PersistentFlags().Lookup("runtime-profile"))
	viper.BindPFlag("store.type", rootCmd.PersistentFlags().Lookup("store-type"))
	viper.BindPFlag("postgres.host", rootCmd.PersistentFlags().Lookup("postgres-host"))
	viper.BindPFlag("postgres.port", rootCmd.PersistentFlags().Lookup("postgres-port"))
	viper.BindPFlag("postgres.user", rootCmd.PersistentFlags().Lookup("postgres-user"))
	viper.BindPFlag("postgres.password", rootCmd.PersistentFlags().Lookup("postgres-password"))
	viper.BindPFlag("postgres.database", rootCmd.PersistentFlags().Lookup("postgres-database"))
	viper.BindPFlag("postgres.max_conns", rootCmd.PersistentFlags().Lookup("postgres-max-conns"))
	viper.BindPFlag("postgres.min_conns", rootCmd.PersistentFlags().Lookup("postgres-min-conns"))
	viper.BindPFlag("postgres.ssl_mode", rootCmd.PersistentFlags().Lookup("postgres-ssl-mode"))
	viper.BindPFlag("postgres_sharded.shard_map_file", rootCmd.PersistentFlags().Lookup("shard-map-file"))
	viper.BindPFlag("postgres_sharded.node_id", rootCmd.PersistentFlags().Lookup("node-id"))
	viper.BindPFlag("postgres_sharded.advertise_addr", rootCmd.PersistentFlags().Lookup("advertise-addr"))
	viper.BindPFlag("postgres_sharded.shard_map_refresh_interval", rootCmd.PersistentFlags().Lookup("shard-map-refresh-interval"))
	viper.BindPFlag("postgres.queue_claim_mode", rootCmd.Flags().Lookup("postgres-queue-claim-mode"))
	viper.BindPFlag("events.slim", rootCmd.Flags().Lookup("slim-events"))
	viper.BindPFlag("events.defer", rootCmd.Flags().Lookup("defer-events"))
	viper.BindPFlag("events.batch_size", rootCmd.Flags().Lookup("event-batch-size"))
	viper.BindPFlag("prefetch.enabled", rootCmd.Flags().Lookup("prefetch-enabled"))
	viper.BindPFlag("prefetch.buffer_size", rootCmd.Flags().Lookup("prefetch-buffer-size"))
	viper.BindPFlag("prefetch.batch_size", rootCmd.Flags().Lookup("prefetch-batch-size"))
	viper.BindPFlag("sqlite.path", rootCmd.PersistentFlags().Lookup("sqlite-path"))
	viper.BindPFlag("sqlite.inmemory", rootCmd.PersistentFlags().Lookup("sqlite-inmemory"))
	viper.BindPFlag("sqlite.busy_timeout", rootCmd.PersistentFlags().Lookup("sqlite-busy-timeout"))
	viper.BindPFlag("sqlite.journal_mode", rootCmd.PersistentFlags().Lookup("sqlite-journal-mode"))
	viper.BindPFlag("sqlite.synchronous", rootCmd.PersistentFlags().Lookup("sqlite-synchronous"))
	viper.BindPFlag("sqlite.cache_kb", rootCmd.PersistentFlags().Lookup("sqlite-cache-kb"))
	viper.BindPFlag("http.addr", rootCmd.Flags().Lookup("http-addr"))
	viper.BindPFlag("api.key", rootCmd.Flags().Lookup("api-key"))
	viper.BindPFlag("auth.keys_file", rootCmd.Flags().Lookup("auth-keys-file"))
	viper.BindPFlag("auth.legacy_namespace_id", rootCmd.Flags().Lookup("legacy-namespace-id"))
	viper.BindPFlag("auth.mode", rootCmd.Flags().Lookup("auth-mode"))
	viper.BindPFlag("schema.mode", rootCmd.PersistentFlags().Lookup("schema-mode"))
	viper.BindPFlag("cors.allow_origins", rootCmd.Flags().Lookup("cors-allow-origins"))
	viper.BindPFlag("cors.allow_methods", rootCmd.Flags().Lookup("cors-allow-methods"))
	viper.BindPFlag("cors.allow_headers", rootCmd.Flags().Lookup("cors-allow-headers"))
	viper.BindPFlag("cors.max_age_seconds", rootCmd.Flags().Lookup("cors-max-age-seconds"))
	viper.BindPFlag("rate_limit.requests", rootCmd.Flags().Lookup("rate-limit-requests"))
	viper.BindPFlag("rate_limit.window", rootCmd.Flags().Lookup("rate-limit-window"))
	viper.BindPFlag("http.max_body_bytes", rootCmd.Flags().Lookup("http-max-body-bytes"))
	viper.BindPFlag("http.request_timeout", rootCmd.Flags().Lookup("http-request-timeout"))
	viper.BindPFlag("http.read_timeout", rootCmd.Flags().Lookup("http-read-timeout"))
	viper.BindPFlag("http.read_header_timeout", rootCmd.Flags().Lookup("http-read-header-timeout"))
	viper.BindPFlag("http.write_timeout", rootCmd.Flags().Lookup("http-write-timeout"))
	viper.BindPFlag("http.idle_timeout", rootCmd.Flags().Lookup("http-idle-timeout"))
	viper.BindPFlag("telemetry.mode", rootCmd.Flags().Lookup("telemetry-mode"))
	viper.BindPFlag("telemetry.retention", rootCmd.Flags().Lookup("telemetry-retention"))
	viper.BindPFlag("telemetry.cleanup_interval", rootCmd.Flags().Lookup("telemetry-cleanup-interval"))
	viper.BindPFlag("metrics.agent_refresh_interval", rootCmd.Flags().Lookup("metrics-agent-refresh-interval"))
	viper.BindPFlag("metrics.agent_kinds", rootCmd.Flags().Lookup("metrics-agent-kinds"))
	viper.BindPFlag("security.trusted_proxies", rootCmd.Flags().Lookup("trusted-proxies"))
	viper.BindPFlag("log.level", rootCmd.Flags().Lookup("log-level"))
	viper.BindPFlag("log.events", rootCmd.Flags().Lookup("log-events"))
	viper.BindPFlag("log.http_requests", rootCmd.Flags().Lookup("http-log-requests"))
	viper.BindPFlag("http.metrics", rootCmd.Flags().Lookup("http-metrics"))

	addSchemaCommands()
}

func isValidRuntimeProfile(profile string) bool {
	switch profile {
	case "dev", "production", "benchmark":
		return true
	default:
		return false
	}
}

func resolveHTTPRuntimeOptions(profile string) httpRuntimeOptions {
	if profile == "benchmark" {
		return httpRuntimeOptions{
			enableSecurityHeaders: false,
			enableCORS:            false,
			enableRequestTimeout:  false,
			enableMaxBody:         false,
		}
	}
	return httpRuntimeOptions{
		enableSecurityHeaders: true,
		enableCORS:            true,
		enableRequestTimeout:  true,
		enableMaxBody:         true,
	}
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName("config")
	}

	viper.SetEnvPrefix("") // No prefix
	viper.AutomaticEnv()
	viper.SetEnvKeyReplacer(strings.NewReplacer(".", "_"))

	// Manual environment variable bindings
	viper.BindEnv("runtime.profile", "RUNTIME_PROFILE")
	viper.BindEnv("store.type", "STORE_TYPE")
	viper.BindEnv("postgres.host", "POSTGRES_HOST")
	viper.BindEnv("postgres.port", "POSTGRES_PORT")
	viper.BindEnv("postgres.user", "POSTGRES_USER")
	viper.BindEnv("postgres.password", "POSTGRES_PASSWORD")
	viper.BindEnv("postgres.database", "POSTGRES_DATABASE")
	viper.BindEnv("postgres.max_conns", "POSTGRES_MAX_CONNS")
	viper.BindEnv("postgres.min_conns", "POSTGRES_MIN_CONNS")
	viper.BindEnv("postgres.ssl_mode", "POSTGRES_SSL_MODE")
	viper.BindEnv("postgres.queue_claim_mode", "POSTGRES_QUEUE_CLAIM_MODE")
	viper.BindEnv("events.slim", "SLIM_EVENTS")
	viper.BindEnv("events.defer", "DEFER_EVENTS")
	viper.BindEnv("events.batch_size", "EVENT_BATCH_SIZE")
	viper.BindEnv("prefetch.enabled", "PREFETCH_ENABLED")
	viper.BindEnv("prefetch.buffer_size", "PREFETCH_BUFFER_SIZE")
	viper.BindEnv("prefetch.batch_size", "PREFETCH_BATCH_SIZE")
	viper.BindEnv("sqlite.path", "SQLITE_PATH")
	viper.BindEnv("sqlite.inmemory", "SQLITE_INMEMORY")
	viper.BindEnv("sqlite.busy_timeout", "SQLITE_BUSY_TIMEOUT")
	viper.BindEnv("sqlite.journal_mode", "SQLITE_JOURNAL_MODE")
	viper.BindEnv("sqlite.synchronous", "SQLITE_SYNCHRONOUS")
	viper.BindEnv("sqlite.cache_kb", "SQLITE_CACHE_KB")
	viper.BindEnv("http.addr", "HTTP_ADDR")
	viper.BindEnv("api.key", "API_KEY")
	viper.BindEnv("auth.keys_file", "AUTH_KEYS_FILE")
	viper.BindEnv("auth.legacy_namespace_id", "AUTH_LEGACY_NAMESPACE_ID")
	viper.BindEnv("auth.mode", "AUTH_MODE")
	viper.BindEnv("schema.mode", "SCHEMA_MODE")
	viper.BindEnv("cors.allow_origins", "CORS_ALLOW_ORIGINS")
	viper.BindEnv("cors.allow_methods", "CORS_ALLOW_METHODS")
	viper.BindEnv("cors.allow_headers", "CORS_ALLOW_HEADERS")
	viper.BindEnv("cors.max_age_seconds", "CORS_MAX_AGE_SECONDS")
	viper.BindEnv("rate_limit.requests", "RATE_LIMIT_REQUESTS")
	viper.BindEnv("rate_limit.window", "RATE_LIMIT_WINDOW")
	viper.BindEnv("http.max_body_bytes", "HTTP_MAX_BODY_BYTES")
	viper.BindEnv("http.request_timeout", "HTTP_REQUEST_TIMEOUT")
	viper.BindEnv("http.read_timeout", "HTTP_READ_TIMEOUT")
	viper.BindEnv("http.read_header_timeout", "HTTP_READ_HEADER_TIMEOUT")
	viper.BindEnv("http.write_timeout", "HTTP_WRITE_TIMEOUT")
	viper.BindEnv("http.idle_timeout", "HTTP_IDLE_TIMEOUT")
	viper.BindEnv("telemetry.mode", "TELEMETRY_MODE")
	viper.BindEnv("telemetry.retention", "TELEMETRY_RETENTION")
	viper.BindEnv("telemetry.cleanup_interval", "TELEMETRY_CLEANUP_INTERVAL")
	viper.BindEnv("metrics.agent_refresh_interval", "METRICS_AGENT_REFRESH_INTERVAL")
	viper.BindEnv("metrics.agent_kinds", "METRICS_AGENT_KINDS")
	viper.BindEnv("security.trusted_proxies", "TRUSTED_PROXIES")
	viper.BindEnv("log.level", "LOG_LEVEL")
	viper.BindEnv("log.events", "LOG_EVENTS")
	viper.BindEnv("log.http_requests", "HTTP_LOG_REQUESTS")
	viper.BindEnv("http.metrics", "HTTP_METRICS")

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

func addSchemaCommands() {
	schemaCmd := &cobra.Command{
		Use:   "schema",
		Short: "Manage schema migrations for SQL backends",
		Args:  cobra.NoArgs,
	}

	validateCmd := &cobra.Command{
		Use:   "validate",
		Short: "Validate schema version and migration checksums",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return executeSchemaCommand(cmd.Context(), "validate", 0)
		},
	}

	migrateCmd := &cobra.Command{
		Use:   "migrate",
		Short: "Apply up migrations to the requested target version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			target, err := cmd.Flags().GetInt64("to")
			if err != nil {
				return err
			}
			return executeSchemaCommand(cmd.Context(), "migrate", target)
		},
	}
	migrateCmd.Flags().Int64("to", 0, "Target schema version (0 uses binary target version)")

	rollbackCmd := &cobra.Command{
		Use:   "rollback",
		Short: "Apply down migrations to an explicit target version",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			target, err := cmd.Flags().GetInt64("to")
			if err != nil {
				return err
			}
			if target < 0 {
				return fmt.Errorf("rollback --to must be >= 0")
			}
			return executeSchemaCommand(cmd.Context(), "rollback", target)
		},
	}
	rollbackCmd.Flags().Int64("to", -1, "Target schema version to roll back to (required)")
	if err := rollbackCmd.MarkFlagRequired("to"); err != nil {
		panic(err)
	}

	schemaCmd.AddCommand(validateCmd, migrateCmd, rollbackCmd)
	rootCmd.AddCommand(schemaCmd)
}

func executeSchemaCommand(ctx context.Context, action string, requestedVersion int64) error {
	if ctx == nil {
		ctx = context.Background()
	}

	storeTypeRaw := strings.ToLower(strings.TrimSpace(viper.GetString("store.type")))
	if storeTypeRaw == "" {
		storeTypeRaw = string(store.StoreTypePostgres)
	}

	var (
		result any
		err    error
	)

	switch store.StoreType(storeTypeRaw) {
	case store.StoreTypePostgres:
		result, err = postgresbackend.ExecuteSchemaAction(ctx, postgresConfigFromViper(), action, requestedVersion)
	case store.StoreTypePostgresSharded:
		result, err = postgresshardedbackend.ExecuteSchemaAction(ctx, postgresShardedConfigFromViper(), action, requestedVersion)
	case store.StoreTypeSQLite, store.StoreTypeSQLiteMem:
		result, err = sqlitebackend.ExecuteSchemaAction(ctx, sqliteConfigFromViper(store.StoreType(storeTypeRaw)), action, requestedVersion)
	default:
		return fmt.Errorf("schema commands are supported only for postgres/postgres-sharded/sqlite backends; got %q", storeTypeRaw)
	}
	if err != nil {
		return err
	}

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(result)
}

func postgresConfigFromViper() *postgresbackend.PostgresConfig {
	sslMode := strings.TrimSpace(viper.GetString("postgres.ssl_mode"))
	if sslMode == "" {
		sslMode = "disable"
	}
	return &postgresbackend.PostgresConfig{
		Host:               viper.GetString("postgres.host"),
		Port:               viper.GetInt("postgres.port"),
		User:               viper.GetString("postgres.user"),
		Password:           viper.GetString("postgres.password"),
		Database:           viper.GetString("postgres.database"),
		SSLMode:            sslMode,
		SchemaMode:         strings.ToLower(strings.TrimSpace(viper.GetString("schema.mode"))),
		MaxConns:           viper.GetInt("postgres.max_conns"),
		MinConns:           viper.GetInt("postgres.min_conns"),
		SlimEvents:         viper.GetBool("events.slim"),
		QueueClaimMode:     viper.GetString("postgres.queue_claim_mode"),
		DeferEvents:        viper.GetBool("events.defer"),
		EventBatchSize:     viper.GetInt("events.batch_size"),
		PrefetchEnabled:    viper.GetBool("prefetch.enabled"),
		PrefetchBufferSize: viper.GetInt("prefetch.buffer_size"),
		PrefetchBatchSize:  viper.GetInt("prefetch.batch_size"),
	}
}

func postgresShardedConfigFromViper() *postgresshardedbackend.Config {
	return &postgresshardedbackend.Config{
		ShardMapFile:          viper.GetString("postgres_sharded.shard_map_file"),
		NodeID:                viper.GetString("postgres_sharded.node_id"),
		AdvertiseAddr:         viper.GetString("postgres_sharded.advertise_addr"),
		ShardMapRefreshPeriod: viper.GetString("postgres_sharded.shard_map_refresh_interval"),
		SchemaMode:            strings.ToLower(strings.TrimSpace(viper.GetString("schema.mode"))),
		MaxConns:              viper.GetInt("postgres.max_conns"),
		MinConns:              viper.GetInt("postgres.min_conns"),
		MaxConnAge:            viper.GetString("postgres.max_conn_age"),
		MaxIdleTime:           viper.GetString("postgres.max_idle_time"),
		SlimEvents:            viper.GetBool("events.slim"),
		QueueClaimMode:        viper.GetString("postgres.queue_claim_mode"),
		DeferEvents:           viper.GetBool("events.defer"),
		EventBatchSize:        viper.GetInt("events.batch_size"),
	}
}

func sqliteConfigFromViper(storeType store.StoreType) *sqlitebackend.SQLiteConfig {
	cfg := sqlitebackend.DefaultSQLiteConfig()
	cfg.Path = viper.GetString("sqlite.path")
	cfg.InMemory = viper.GetBool("sqlite.inmemory")
	if storeType == store.StoreTypeSQLiteMem {
		cfg.InMemory = true
	}
	cfg.SchemaMode = strings.ToLower(strings.TrimSpace(viper.GetString("schema.mode")))
	cfg.BusyTimeout = viper.GetString("sqlite.busy_timeout")
	cfg.JournalMode = viper.GetString("sqlite.journal_mode")
	cfg.Synchronous = viper.GetString("sqlite.synchronous")
	cfg.CacheSizeKB = viper.GetInt("sqlite.cache_kb")
	return cfg
}

func splitCommaList(value string) []string {
	if strings.TrimSpace(value) == "" {
		return nil
	}
	parts := strings.Split(value, ",")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if trimmed := strings.TrimSpace(part); trimmed != "" {
			out = append(out, trimmed)
		}
	}
	return out
}

func splitCommaListDedup(value string) []string {
	parts := splitCommaList(value)
	if len(parts) == 0 {
		return nil
	}

	seen := make(map[string]struct{}, len(parts))
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		if _, ok := seen[part]; ok {
			continue
		}
		seen[part] = struct{}{}
		out = append(out, part)
	}
	return out
}

func defaultLocalCORSOrigins() []string {
	return []string{
		"http://127.0.0.1",
		"http://127.0.0.1:3000",
		"http://127.0.0.1:5173",
		"http://127.0.0.1:8080",
		"http://localhost",
		"http://localhost:3000",
		"http://localhost:5173",
		"http://localhost:8080",
		"https://127.0.0.1",
		"https://localhost",
	}
}

func runServer(cmd *cobra.Command, args []string) {
	// Initialize logger
	var err error

	// Check DEBUG environment variable first
	if os.Getenv("DEBUG") == "true" {
		config := zap.NewDevelopmentConfig()
		config.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
		logger, err = config.Build()
		if err != nil {
			log.Fatal("Failed to initialize debug logger:", err)
		}
		logger.Info("Debug mode enabled")
	} else {
		logLevel := strings.ToLower(strings.TrimSpace(viper.GetString("log.level")))
		var config zap.Config
		level := zap.InfoLevel
		switch logLevel {
		case "debug":
			config = zap.NewDevelopmentConfig()
			level = zap.DebugLevel
		case "warn", "warning":
			config = zap.NewProductionConfig()
			level = zap.WarnLevel
		case "error":
			config = zap.NewProductionConfig()
			level = zap.ErrorLevel
		default:
			config = zap.NewProductionConfig()
			level = zap.InfoLevel
		}
		config.Level = zap.NewAtomicLevelAt(level)
		logger, err = config.Build()
		if err != nil {
			log.Fatal("Failed to initialize logger:", err)
		}
	}
	defer logger.Sync()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	runtimeProfile := strings.ToLower(strings.TrimSpace(viper.GetString("runtime.profile")))
	if runtimeProfile == "" {
		runtimeProfile = "dev"
	}
	if !isValidRuntimeProfile(runtimeProfile) {
		logger.Fatal("invalid runtime profile", zap.String("runtime_profile", runtimeProfile))
	}

	schemaMode := strings.ToLower(strings.TrimSpace(viper.GetString("schema.mode")))
	if schemaMode == "" {
		if runtimeProfile == "production" {
			schemaMode = "validate"
		} else {
			schemaMode = "migrate"
		}
	}
	if schemaMode != "migrate" && schemaMode != "validate" {
		logger.Fatal("invalid schema mode", zap.String("schema_mode", schemaMode))
	}

	// Resolve PostgreSQL SSL mode
	pgSSLMode := strings.TrimSpace(viper.GetString("postgres.ssl_mode"))
	if pgSSLMode == "" {
		pgSSLMode = "disable"
	}
	if runtimeProfile == "production" && pgSSLMode == "disable" {
		logger.Warn("forcing postgres-ssl-mode=require for production profile")
		pgSSLMode = "require"
	}

	// Create store config
	storeCfg := &store.Config{
		Type: store.StoreType(viper.GetString("store.type")),
		Postgres: &store.PostgresConfig{
			Host:               viper.GetString("postgres.host"),
			Port:               viper.GetInt("postgres.port"),
			User:               viper.GetString("postgres.user"),
			Password:           viper.GetString("postgres.password"),
			Database:           viper.GetString("postgres.database"),
			SSLMode:            pgSSLMode,
			SchemaMode:         schemaMode,
			MaxConns:           viper.GetInt("postgres.max_conns"),
			MinConns:           viper.GetInt("postgres.min_conns"),
			SlimEvents:         viper.GetBool("events.slim"),
			QueueClaimMode:     viper.GetString("postgres.queue_claim_mode"),
			DeferEvents:        viper.GetBool("events.defer"),
			EventBatchSize:     viper.GetInt("events.batch_size"),
			PrefetchEnabled:    viper.GetBool("prefetch.enabled"),
			PrefetchBufferSize: viper.GetInt("prefetch.buffer_size"),
			PrefetchBatchSize:  viper.GetInt("prefetch.batch_size"),
		},
		PostgresSharded: &store.PostgresShardedConfig{
			ShardMapFile:          viper.GetString("postgres_sharded.shard_map_file"),
			NodeID:                viper.GetString("postgres_sharded.node_id"),
			AdvertiseAddr:         viper.GetString("postgres_sharded.advertise_addr"),
			ShardMapRefreshPeriod: viper.GetString("postgres_sharded.shard_map_refresh_interval"),
			SchemaMode:            schemaMode,
			MaxConns:              viper.GetInt("postgres.max_conns"),
			MinConns:              viper.GetInt("postgres.min_conns"),
			SlimEvents:            viper.GetBool("events.slim"),
			QueueClaimMode:        viper.GetString("postgres.queue_claim_mode"),
			DeferEvents:           viper.GetBool("events.defer"),
			EventBatchSize:        viper.GetInt("events.batch_size"),
			PrefetchEnabled:       viper.GetBool("prefetch.enabled"),
			PrefetchBufferSize:    viper.GetInt("prefetch.buffer_size"),
			PrefetchBatchSize:     viper.GetInt("prefetch.batch_size"),
		},
		SQLite: func() *store.SQLiteConfig {
			cfg := store.DefaultSQLiteConfig()
			cfg.Path = viper.GetString("sqlite.path")
			cfg.InMemory = viper.GetBool("sqlite.inmemory")
			cfg.SchemaMode = schemaMode
			cfg.BusyTimeout = viper.GetString("sqlite.busy_timeout")
			cfg.JournalMode = viper.GetString("sqlite.journal_mode")
			cfg.Synchronous = viper.GetString("sqlite.synchronous")
			cfg.CacheSizeKB = viper.GetInt("sqlite.cache_kb")
			return cfg
		}(),
	}

	// Create agent space
	space, err := store.NewStore(ctx, storeCfg, logger)
	if err != nil {
		logger.Fatal("Failed to create agent space", zap.Error(err))
	}

	telemetryMode := strings.ToLower(strings.TrimSpace(viper.GetString("telemetry.mode")))
	if telemetryMode == "" {
		telemetryMode = "optional"
	}
	if telemetryMode != "disabled" && telemetryMode != "optional" && telemetryMode != "required" {
		logger.Fatal("invalid telemetry mode", zap.String("telemetry_mode", telemetryMode))
	}
	_, telemetrySupported := space.(telemetryCapability)
	if telemetryMode == "required" && !telemetrySupported {
		logger.Fatal("telemetry-mode=required but selected backend does not support telemetry persistence",
			zap.String("store", string(storeCfg.Type)))
	}
	if telemetryMode == "disabled" && telemetrySupported {
		logger.Info("Telemetry endpoints are disabled by configuration despite backend support",
			zap.String("store", string(storeCfg.Type)))
	}

	telemetryRetention := parseDurationOrZero(viper.GetString("telemetry.retention"))
	telemetryCleanupInterval := parseDurationOrZero(viper.GetString("telemetry.cleanup_interval"))
	if telemetryRetention > 0 && telemetryCleanupInterval > 0 {
		startTelemetryCleanupLoop(ctx, logger, space, telemetryRetention, telemetryCleanupInterval)
	}

	metricsAgentRefreshInterval := parseDurationOrZero(viper.GetString("metrics.agent_refresh_interval"))
	metricsAgentKinds := splitCommaListDedup(viper.GetString("metrics.agent_kinds"))
	startAgentMetricsLoop(ctx, logger, space, string(storeCfg.Type), metricsAgentRefreshInterval, metricsAgentKinds)

	authMode := strings.ToLower(strings.TrimSpace(viper.GetString("auth.mode")))
	if authMode == "" {
		authMode = "optional"
	}
	if runtimeProfile == "production" && authMode != string(api.AuthModeRequired) {
		logger.Warn("forcing auth-mode=required for production profile")
		authMode = string(api.AuthModeRequired)
	}
	if authMode != string(api.AuthModeOptional) && authMode != string(api.AuthModeRequired) {
		logger.Fatal("invalid auth mode", zap.String("auth_mode", authMode))
	}

	apiKey := strings.TrimSpace(viper.GetString("api.key"))
	authKeysFile := strings.TrimSpace(viper.GetString("auth.keys_file"))
	legacyNamespaceID := strings.TrimSpace(viper.GetString("auth.legacy_namespace_id"))
	keyPrincipals, err := api.LoadAuthKeyPrincipals(authKeysFile)
	if err != nil {
		logger.Fatal("failed to load auth keys file", zap.String("auth_keys_file", authKeysFile), zap.Error(err))
	}
	if runtimeProfile == "production" && authKeysFile == "" {
		logger.Fatal("production profile requires auth-keys-file/AUTH_KEYS_FILE")
	}
	if runtimeProfile == "production" && len(keyPrincipals) == 0 {
		logger.Fatal("production profile requires at least one auth key principal")
	}
	if authMode == string(api.AuthModeRequired) && apiKey == "" && len(keyPrincipals) == 0 {
		logger.Fatal("auth-mode=required requires api-key or auth-keys-file to be set")
	}
	if len(keyPrincipals) > 0 && apiKey != "" {
		logger.Warn("auth-keys-file configured; ignoring legacy single api-key")
	}
	if len(keyPrincipals) > 0 && legacyNamespaceID != "" {
		logger.Warn("legacy-namespace-id is ignored when auth-keys-file is configured")
	}
	if authMode == string(api.AuthModeOptional) && apiKey == "" && len(keyPrincipals) == 0 {
		logger.Warn("SECURITY: authentication is disabled — only local-only development exposure is supported",
			zap.String("auth_mode", authMode),
			zap.String("profile", runtimeProfile))
	}

	corsOrigins := splitCommaList(viper.GetString("cors.allow_origins"))
	if len(corsOrigins) == 0 {
		if runtimeProfile == "production" {
			corsOrigins = nil // deny all cross-origin by default in production profile
		} else {
			corsOrigins = defaultLocalCORSOrigins()
		}
	}
	if runtimeProfile == "production" {
		for _, origin := range corsOrigins {
			if origin == "*" {
				logger.Fatal("wildcard CORS origin is not allowed in production profile")
			}
		}
	}
	corsMethods := splitCommaList(viper.GetString("cors.allow_methods"))
	corsHeaders := splitCommaList(viper.GetString("cors.allow_headers"))

	rateLimitWindow := parseDurationOrDefault(viper.GetString("rate_limit.window"), time.Second)
	rateLimitRequests := viper.GetInt("rate_limit.requests")
	if runtimeProfile == "production" && rateLimitRequests <= 0 {
		rateLimitRequests = 200
	}
	maxBodyBytes := viper.GetInt64("http.max_body_bytes")
	if runtimeProfile == "production" && maxBodyBytes <= 0 {
		maxBodyBytes = 1024 * 1024
	}

	requestTimeout := parseDurationOrDefault(viper.GetString("http.request_timeout"), 30*time.Second)
	readTimeout := parseDurationOrDefault(viper.GetString("http.read_timeout"), 30*time.Second)
	readHeaderTimeout := parseDurationOrDefault(viper.GetString("http.read_header_timeout"), 10*time.Second)
	writeTimeout := parseDurationOrDefault(viper.GetString("http.write_timeout"), 30*time.Second)
	idleTimeout := parseDurationOrDefault(viper.GetString("http.idle_timeout"), 120*time.Second)

	// Create API handlers
	handlers := api.NewHandlers(space, logger)

	// Create HTTP server
	httpAddr := viper.GetString("http.addr")
	mux := http.NewServeMux()

	// API v1 routes
	mux.HandleFunc("/api/v1/agents", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handlers.CreateAgent(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/agents/", func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		if path == "/api/v1/agents/query" && r.Method == http.MethodPost {
			handlers.QueryAgents(w, r)
		} else if path == "/api/v1/agents/stats" && r.Method == http.MethodPost {
			handlers.AgentStats(w, r)
		} else if path == "/api/v1/agents/read" && r.Method == http.MethodPost {
			handlers.ReadAgent(w, r)
		} else if path == "/api/v1/agents/take" && r.Method == http.MethodPost {
			handlers.TakeAgent(w, r)
		} else if path == "/api/v1/agents/batch-take" && r.Method == http.MethodPost {
			handlers.BatchTakeAgent(w, r)
		} else if path == "/api/v1/agents/in" && r.Method == http.MethodPost {
			handlers.InAgent(w, r)
		} else if strings.HasSuffix(path, "/complete-and-out") && r.Method == http.MethodPost {
			handlers.CompleteAndOutAgent(w, r)
		} else if strings.HasSuffix(path, "/complete") && r.Method == http.MethodPost {
			handlers.CompleteAgent(w, r)
		} else if strings.HasSuffix(path, "/release") && r.Method == http.MethodPost {
			handlers.ReleaseAgent(w, r)
		} else if strings.HasSuffix(path, "/renew") && r.Method == http.MethodPost {
			handlers.RenewLeaseAgent(w, r)
		} else if r.Method == http.MethodGet {
			handlers.GetAgent(w, r)
		} else if r.Method == http.MethodPut {
			handlers.UpdateAgent(w, r)
		} else {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/events", handlers.GetEvents)
	mux.HandleFunc("/api/v1/admin/shards", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		handlers.GetShardMap(w, r)
	})
	mux.HandleFunc("/api/v1/admin/shards/health", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		handlers.GetShardHealth(w, r)
	})
	mux.HandleFunc("/api/v1/admin/shards/queues/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		handlers.GetShardQueueStats(w, r)
	})
	mux.HandleFunc("/api/v1/telemetry/spans", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodPost:
			handlers.IngestTelemetrySpan(w, r)
		case http.MethodGet:
			handlers.ListTelemetrySpans(w, r)
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})
	mux.HandleFunc("/api/v1/telemetry/spans:batch", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		handlers.IngestTelemetrySpanBatch(w, r)
	})
	mux.HandleFunc("/api/v1/dag/", handlers.GetDAG)
	mux.HandleFunc("/api/v1/health", handlers.HealthCheck)
	mux.HandleFunc("/api/v1/stats", handlers.GetStats)

	// Legacy endpoints for compatibility
	mux.HandleFunc("/health", handlers.HealthCheck)
	mux.HandleFunc("/stats", handlers.GetStats)

	// Metrics endpoint
	mux.Handle("/metrics", api.PrometheusHandler())

	if debugEnv := strings.ToLower(os.Getenv("DEBUG_INVARIANTS")); debugEnv == "true" || debugEnv == "1" {
		if runtimeProfile == "production" {
			logger.Warn("DEBUG_INVARIANTS is ignored in production profile")
		} else {
			logger.Info("Debug invariants endpoint enabled", zap.String("path", "/debug/invariants"))
			mux.HandleFunc("/debug/invariants", handlers.DebugInvariants)
		}
	}

	// Parse trusted proxies for X-Forwarded-For validation
	trustedProxies := splitCommaList(viper.GetString("security.trusted_proxies"))

	// Apply middleware
	authExemptPaths := api.DefaultAuthExemptPaths()
	profileOptions := resolveHTTPRuntimeOptions(runtimeProfile)
	middleware := []func(http.Handler) http.Handler{}
	if profileOptions.enableSecurityHeaders {
		middleware = append(middleware, api.SecurityHeadersMiddleware)
	}
	if profileOptions.enableCORS {
		middleware = append(middleware, api.CORSMiddlewareWithConfig(api.CORSConfig{
			AllowedOrigins: corsOrigins,
			AllowedMethods: corsMethods,
			AllowedHeaders: corsHeaders,
			MaxAgeSeconds:  viper.GetInt("cors.max_age_seconds"),
		}))
	}
	middleware = append(middleware,
		api.TelemetryModeMiddleware(telemetryMode),
		api.AuthMiddleware(api.AuthConfig{
			APIKey:            apiKey,
			KeyPrincipals:     keyPrincipals,
			LegacyNamespaceID: legacyNamespaceID,
			Mode:              api.AuthMode(authMode),
			ExemptPaths:       authExemptPaths,
		}),
		api.AdminOnlyMiddleware(api.DefaultAdminRouteConfig()),
	)
	if profileOptions.enableRequestTimeout {
		middleware = append(middleware, api.RequestTimeoutMiddleware(requestTimeout, true))
	}
	if profileOptions.enableMaxBody {
		middleware = append(middleware, api.MaxBodyBytesMiddleware(maxBodyBytes, true))
	}
	middleware = append(middleware, api.RateLimitMiddleware(api.RateLimitConfig{
		RequestsPerWindow: rateLimitRequests,
		Window:            rateLimitWindow,
		MutatingOnly:      true,
		TrustedProxies:    trustedProxies,
	}, logger))
	if viper.GetBool("log.http_requests") {
		middleware = append(middleware, api.LoggingMiddleware(logger))
	}
	if viper.GetBool("http.metrics") {
		middleware = append(middleware, api.MetricsMiddleware)
	}
	handler := api.ApplyMiddleware(mux, middleware...)

	server := &http.Server{
		Addr:              httpAddr,
		Handler:           handler,
		ReadTimeout:       readTimeout,
		ReadHeaderTimeout: readHeaderTimeout,
		WriteTimeout:      writeTimeout,
		IdleTimeout:       idleTimeout,
	}

	// Start HTTP server
	go func() {
		logger.Info("Starting HTTP server", zap.String("addr", httpAddr))
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Fatal("HTTP server failed", zap.Error(err))
		}
	}()

	// Subscribe to all events for logging (debug/opt-in)
	logEvents := viper.GetBool("log.events")
	if logEvents || logger.Core().Enabled(zap.DebugLevel) {
		eventLevel := zap.DebugLevel
		if logEvents {
			eventLevel = zap.InfoLevel
		}
		space.Subscribe(&agent.EventFilter{}, func(event *agent.Event) {
			if ce := logger.Check(eventLevel, "event"); ce != nil {
				ce.Write(
					zap.String("type", string(event.Type)),
					zap.String("tuple_id", event.TupleID),
					zap.String("agent_id", event.AgentID),
					zap.String("trace_id", event.TraceID),
				)
			}
		})
	}

	logger.Info("Agent space server started",
		zap.String("store", string(storeCfg.Type)),
		zap.String("schema_mode", schemaMode),
		zap.String("http", httpAddr))

	// Wait for interrupt signal
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	logger.Info("Shutting down server...")

	// Graceful shutdown
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Error("HTTP server shutdown failed", zap.Error(err))
	}

	cancel()
	logger.Info("Server stopped")
}

func parseDurationOrDefault(value string, fallback time.Duration) time.Duration {
	if strings.TrimSpace(value) == "" {
		return fallback
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func parseDurationOrZero(value string) time.Duration {
	if strings.TrimSpace(value) == "" {
		return 0
	}
	parsed, err := time.ParseDuration(value)
	if err != nil || parsed <= 0 {
		return 0
	}
	return parsed
}

func collectAgentMetrics(statusCounter agent.StatusCounter, counter agent.Counter, kinds []string) (map[string]int, map[string]int, map[string]int, error) {
	if statusCounter == nil && counter == nil {
		return nil, nil, nil, fmt.Errorf("agent store does not support status counting")
	}

	statuses := []agent.Status{
		agent.StatusNew,
		agent.StatusInProgress,
		agent.StatusCompleted,
		agent.StatusFailed,
		agent.StatusCancelled,
	}

	byStatus := make(map[string]int, len(statuses))
	if statusCounter != nil {
		counts, err := statusCounter.CountByStatus(&agent.Query{})
		if err != nil {
			return nil, nil, nil, err
		}
		for _, status := range statuses {
			byStatus[string(status)] = counts[status]
		}
	} else {
		for _, status := range statuses {
			count, err := counter.Count(&agent.Query{Status: status})
			if err != nil {
				return nil, nil, nil, err
			}
			byStatus[string(status)] = count
		}
	}

	byKind := make(map[string]int, len(kinds))
	backlogByKind := make(map[string]int, len(kinds))
	for _, kind := range kinds {
		if statusCounter != nil {
			counts, err := statusCounter.CountByStatus(&agent.Query{Kind: kind})
			if err != nil {
				return nil, nil, nil, err
			}

			total := 0
			for _, count := range counts {
				total += count
			}
			byKind[kind] = total
			backlogByKind[kind] = counts[agent.StatusNew] + counts[agent.StatusInProgress]
			continue
		}

		total := 0
		newCount := 0
		inProgressCount := 0
		for _, status := range statuses {
			count, err := counter.Count(&agent.Query{Kind: kind, Status: status})
			if err != nil {
				return nil, nil, nil, err
			}
			total += count
			if status == agent.StatusNew {
				newCount = count
			}
			if status == agent.StatusInProgress {
				inProgressCount = count
			}
		}

		byKind[kind] = total
		backlogByKind[kind] = newCount + inProgressCount
	}

	return byStatus, byKind, backlogByKind, nil
}

func startAgentMetricsLoop(ctx context.Context, logger *zap.Logger, space agent.AgentSpace, storeType string, interval time.Duration, kinds []string) {
	if interval <= 0 {
		logger.Info("Agent metrics loop disabled", zap.String("store", storeType))
		return
	}

	statusCounter, _ := space.(agent.StatusCounter)
	counter, _ := space.(agent.Counter)
	if statusCounter == nil && counter == nil {
		logger.Warn("Agent metrics loop disabled: store does not support status/count metrics",
			zap.String("store", storeType))
		return
	}

	collectAndPublish := func() {
		byStatus, byKind, backlogByKind, err := collectAgentMetrics(statusCounter, counter, kinds)
		if err != nil {
			logger.Warn("Agent metrics collection failed", zap.Error(err))
			return
		}
		metrics.UpdateAgentMetrics(storeType, byStatus, byKind)
		for kind, backlog := range backlogByKind {
			metrics.UpdateBacklogMetric(storeType, kind, backlog)
		}
	}

	collectAndPublish()
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				collectAndPublish()
			}
		}
	}()
}

func startTelemetryCleanupLoop(ctx context.Context, logger *zap.Logger, space agent.AgentSpace, retention, interval time.Duration) {
	cleaner, ok := space.(telemetryRetentionStore)
	if !ok {
		logger.Info("Telemetry cleanup skipped: backend does not implement retention cleanup",
			zap.Duration("retention", retention),
			zap.Duration("interval", interval))
		return
	}

	runCleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		cutoff := time.Now().UTC().Add(-retention)
		removed, err := cleaner.CleanupSpansBefore(cleanupCtx, cutoff)
		if err != nil {
			logger.Warn("Telemetry cleanup failed", zap.Error(err))
			return
		}
		if removed > 0 {
			logger.Info("Telemetry cleanup removed old spans",
				zap.Int64("removed", removed),
				zap.Time("cutoff", cutoff))
		}
	}

	go func() {
		runCleanup()
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				runCleanup()
			}
		}
	}()
}

func main() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
