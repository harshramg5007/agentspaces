package sqlite

// SQLiteConfig holds SQLite-specific configuration.
type SQLiteConfig struct {
	Path        string
	InMemory    bool
	SchemaMode  string
	BusyTimeout string
	JournalMode string
	Synchronous string
	CacheSizeKB int
}

// DefaultSQLiteConfig returns default SQLite configuration.
func DefaultSQLiteConfig() *SQLiteConfig {
	return &SQLiteConfig{
		Path:        "./data/sqlite/agent_space.db",
		InMemory:    false,
		SchemaMode:  "migrate",
		BusyTimeout: "5s",
		JournalMode: "WAL",
		Synchronous: "NORMAL",
		CacheSizeKB: 16384,
	}
}
