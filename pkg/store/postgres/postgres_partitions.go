package postgres

import (
	"context"
	"fmt"
	"strings"
	"time"
)

const partitionTimestampLayout = "2006-01-02 15:04:05-07:00"

func (s *PostgresStore) ensureEventPartitions(ctx context.Context, now time.Time, totalDays int) error {
	ctx = defaultContext(ctx)
	if totalDays <= 0 {
		totalDays = 1
	}
	start := truncateToUTCDate(now)
	specs := []struct {
		table  string
		column string
	}{
		{table: "agent_events", column: "timestamp"},
		{table: "telemetry_events", column: "ts_start"},
	}
	for _, spec := range specs {
		if err := s.ensureDailyPartitions(ctx, spec.table, spec.column, start, totalDays); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresStore) ensureDailyPartitions(ctx context.Context, parentTable, partitionColumn string, start time.Time, totalDays int) error {
	for i := 0; i < totalDays; i++ {
		from := start.AddDate(0, 0, i)
		until := from.AddDate(0, 0, 1)
		childName := fmt.Sprintf("%s_%s", parentTable, from.Format("20060102"))
		stmt := buildDailyPartitionDDL(parentTable, childName, from, until)
		if _, err := s.pool.Exec(ctx, stmt); err != nil {
			return fmt.Errorf(
				"ensure partition parent=%s child=%s column=%s range=[%s,%s): %w",
				parentTable,
				childName,
				partitionColumn,
				formatPartitionTimestamp(from),
				formatPartitionTimestamp(until),
				err,
			)
		}
	}
	return nil
}

func (s *PostgresStore) dropExpiredTelemetryPartitions(ctx context.Context, cutoff time.Time) error {
	cutoffDay := truncateToUTCDate(cutoff)
	rows, err := s.pool.Query(ctx, `
		SELECT child.relname
		FROM pg_inherits
		JOIN pg_class parent ON pg_inherits.inhparent = parent.oid
		JOIN pg_class child ON pg_inherits.inhrelid = child.oid
		JOIN pg_namespace parent_ns ON parent.relnamespace = parent_ns.oid
		JOIN pg_namespace child_ns ON child.relnamespace = child_ns.oid
		WHERE parent_ns.nspname = current_schema()
		  AND child_ns.nspname = current_schema()
		  AND parent.relname = 'telemetry_events'
	`)
	if err != nil {
		return err
	}
	defer rows.Close()

	var partitions []string
	for rows.Next() {
		var relname string
		if err := rows.Scan(&relname); err != nil {
			return err
		}
		if shouldDropDailyPartition(relname, "telemetry_events_", cutoffDay) {
			partitions = append(partitions, relname)
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}

	for _, relname := range partitions {
		if _, err := s.pool.Exec(ctx, fmt.Sprintf("DROP TABLE IF EXISTS %s", pgQuoteIdentifier(relname))); err != nil {
			return err
		}
	}
	return nil
}

func shouldDropDailyPartition(name, prefix string, cutoffDay time.Time) bool {
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	datePart := strings.TrimPrefix(name, prefix)
	partitionDay, err := time.Parse("20060102", datePart)
	if err != nil {
		return false
	}
	return partitionDay.Before(cutoffDay)
}

func truncateToUTCDate(ts time.Time) time.Time {
	return ts.UTC().Truncate(24 * time.Hour)
}

func buildDailyPartitionDDL(parentTable, childName string, from, until time.Time) string {
	return fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS %s PARTITION OF %s FOR VALUES FROM (TIMESTAMPTZ %s) TO (TIMESTAMPTZ %s)",
		pgQuoteIdentifier(childName),
		pgQuoteIdentifier(parentTable),
		pgQuoteLiteral(formatPartitionTimestamp(from)),
		pgQuoteLiteral(formatPartitionTimestamp(until)),
	)
}

func formatPartitionTimestamp(ts time.Time) string {
	return ts.UTC().Format(partitionTimestampLayout)
}
