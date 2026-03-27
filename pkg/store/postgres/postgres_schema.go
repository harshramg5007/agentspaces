package postgres

import (
	"context"
	"time"
)

func (s *PostgresStore) initSchema(ctx context.Context) error {
	ctx = defaultContext(ctx)
	if err := runSchemaMode(ctx, s.pool, s.schemaMode); err != nil {
		return err
	}
	return s.ensureEventPartitions(ctx, time.Now().UTC(), 8)
}
