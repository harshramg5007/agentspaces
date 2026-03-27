package sqlite

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "modernc.org/sqlite"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/store/internal/common"
	"github.com/urobora-ai/agentspaces/pkg/store/internal/core"
)

const defaultSQLitePollInterval = 100 * time.Millisecond
const defaultSQLiteLeaseReapInterval = 2 * time.Second

const sqliteAgentSelectColumns = `
	id, kind, status, payload, tags, owner, parent_id, trace_id,
	ttl_ms, created_at_ms, updated_at_ms, owner_time_ms, lease_token, lease_until_ms, version, metadata
`

const sqliteEventSelectColumns = `
	id, tuple_id, type, kind, timestamp_ms, agent_id, trace_id, version, data
`

// SQLiteStore implements the AgentSpace interface using SQLite.
type SQLiteStore struct {
	db                *sql.DB
	logger            *zap.Logger
	ctx               context.Context
	cancel            context.CancelFunc
	subscriptions     *common.SubscriptionRegistry
	runtime           *core.StoreRuntime
	pollInterval      time.Duration
	leaseDuration     time.Duration
	leaseReapInterval time.Duration
	schemaMode        string
	storeLabel        string
}

func NewSQLiteStore(ctx context.Context, cfg *SQLiteConfig, logger *zap.Logger) (*SQLiteStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("sqlite config is nil")
	}
	if logger == nil {
		logger = zap.NewNop()
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

	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)

	if err := applySQLitePragmas(db, cfg); err != nil {
		db.Close()
		return nil, err
	}

	storeCtx, cancel := context.WithCancel(ctx)
	store := &SQLiteStore{
		db:                db,
		logger:            logger,
		ctx:               storeCtx,
		cancel:            cancel,
		subscriptions:     common.NewSubscriptionRegistry(),
		pollInterval:      defaultSQLitePollInterval,
		leaseDuration:     agent.DefaultLeaseOptions().LeaseDuration,
		leaseReapInterval: defaultSQLiteLeaseReapInterval,
		schemaMode:        cfg.SchemaMode,
		storeLabel:        sqliteStoreLabel(cfg),
	}
	store.runtime = core.NewStoreRuntime(store)

	if err := store.initSchema(storeCtx); err != nil {
		db.Close()
		return nil, err
	}

	go store.leaseReaperLoop()

	return store, nil
}

// Close closes the store.
func (s *SQLiteStore) Close() error {
	s.cancel()
	return s.db.Close()
}

// Health checks whether the SQLite backend is reachable.
func (s *SQLiteStore) Health(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var one int
	if err := s.db.QueryRowContext(ctx, "select 1").Scan(&one); err != nil {
		return err
	}
	if one != 1 {
		return fmt.Errorf("unexpected health query result: %d", one)
	}
	return nil
}

// Out writes a agent to the space.

func buildSQLiteDSN(cfg *SQLiteConfig) (string, string, error) {
	if cfg.InMemory {
		return "file:agent_space?mode=memory&cache=shared", "", nil
	}

	path := cfg.Path
	if path == "" {
		path = DefaultSQLiteConfig().Path
	}
	if path == ":memory:" {
		return "file:agent_space?mode=memory&cache=shared", "", nil
	}
	return path, path, nil
}

func sqliteStoreLabel(cfg *SQLiteConfig) string {
	if cfg != nil && cfg.InMemory {
		return "sqlite-memory"
	}
	return "sqlite-file"
}

var allowedJournalModes = map[string]bool{
	"DELETE":   true,
	"TRUNCATE": true,
	"PERSIST":  true,
	"MEMORY":   true,
	"WAL":      true,
	"OFF":      true,
}

var allowedSynchronousModes = map[string]bool{
	"OFF":    true,
	"NORMAL": true,
	"FULL":   true,
	"EXTRA":  true,
}

func applySQLitePragmas(db *sql.DB, cfg *SQLiteConfig) error {
	pragma := func(stmt string) error {
		_, err := db.Exec(stmt)
		return err
	}

	journalMode := cfg.JournalMode
	if cfg.InMemory {
		if journalMode == "" || strings.EqualFold(journalMode, "WAL") {
			journalMode = "MEMORY"
		}
	}
	if journalMode != "" {
		if !allowedJournalModes[strings.ToUpper(journalMode)] {
			return fmt.Errorf("invalid sqlite journal_mode %q (allowed: DELETE, TRUNCATE, PERSIST, MEMORY, WAL, OFF)", journalMode)
		}
		if err := pragma(fmt.Sprintf("PRAGMA journal_mode=%s", journalMode)); err != nil {
			return fmt.Errorf("failed to set journal_mode: %w", err)
		}
	}
	if cfg.Synchronous != "" {
		if !allowedSynchronousModes[strings.ToUpper(cfg.Synchronous)] {
			return fmt.Errorf("invalid sqlite synchronous mode %q (allowed: OFF, NORMAL, FULL, EXTRA)", cfg.Synchronous)
		}
		if err := pragma(fmt.Sprintf("PRAGMA synchronous=%s", cfg.Synchronous)); err != nil {
			return fmt.Errorf("failed to set synchronous: %w", err)
		}
	}
	if cfg.BusyTimeout != "" {
		if d, err := time.ParseDuration(cfg.BusyTimeout); err == nil {
			ms := int(d / time.Millisecond)
			if err := pragma(fmt.Sprintf("PRAGMA busy_timeout=%d", ms)); err != nil {
				return fmt.Errorf("failed to set busy_timeout: %w", err)
			}
		}
	}
	if cfg.CacheSizeKB > 0 {
		if err := pragma(fmt.Sprintf("PRAGMA cache_size=%d", -cfg.CacheSizeKB)); err != nil {
			return fmt.Errorf("failed to set cache_size: %w", err)
		}
	}
	if err := pragma("PRAGMA foreign_keys=ON"); err != nil {
		return fmt.Errorf("failed to enable foreign_keys: %w", err)
	}
	return nil
}
