package store

import (
	"context"

	"go.uber.org/zap"

	sqlitebackend "github.com/urobora-ai/agentspaces/pkg/store/sqlite"
)

type SQLiteStore = sqlitebackend.SQLiteStore

func NewSQLiteStore(ctx context.Context, cfg *SQLiteConfig, logger *zap.Logger) (*SQLiteStore, error) {
	var backendCfg *sqlitebackend.SQLiteConfig
	if cfg != nil {
		backendCfg = &sqlitebackend.SQLiteConfig{
			Path:        cfg.Path,
			InMemory:    cfg.InMemory,
			SchemaMode:  cfg.SchemaMode,
			BusyTimeout: cfg.BusyTimeout,
			JournalMode: cfg.JournalMode,
			Synchronous: cfg.Synchronous,
			CacheSizeKB: cfg.CacheSizeKB,
		}
	}
	return sqlitebackend.NewSQLiteStore(ctx, backendCfg, logger)
}
