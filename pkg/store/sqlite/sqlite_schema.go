package sqlite

import (
	"context"
)

func (s *SQLiteStore) initSchema(ctx context.Context) error {
	return runSchemaMode(ctx, s.db, s.schemaMode)
}
