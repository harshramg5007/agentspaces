package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"

	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

func (s *PostgresStore) RecordSpans(ctx context.Context, spans []telemetry.Span) error {
	if len(spans) == 0 {
		return nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	batch := &pgx.Batch{}
	now := time.Now().UTC()
	queued := 0

	for _, span := range spans {
		if span.TraceID == "" {
			continue
		}
		if span.TsStart.IsZero() {
			span.TsStart = now
		}
		attrs := span.Attrs
		if attrs == nil {
			attrs = map[string]interface{}{}
		}
		if span.CallID != "" {
			if _, exists := attrs["call_id"]; !exists {
				attrs["call_id"] = span.CallID
			}
		}
		if span.NamespaceID == "" {
			if namespaceRaw, exists := attrs[namespaceMetadataKey]; exists {
				if namespaceValue, ok := namespaceRaw.(string); ok {
					span.NamespaceID = namespaceValue
				}
			}
		}
		if span.NamespaceID != "" {
			if _, exists := attrs[namespaceMetadataKey]; !exists {
				attrs[namespaceMetadataKey] = span.NamespaceID
			}
		}
		if len(span.Links) > 0 {
			if _, exists := attrs["links"]; !exists {
				attrs["links"] = span.Links
			}
		}
		attrsJSON, err := json.Marshal(attrs)
		if err != nil {
			return fmt.Errorf("failed to marshal telemetry attrs: %w", err)
		}

		metricKey := ""
		var metricBool *bool
		var metricNum *float64
		var metricStr *string
		if span.Metric != nil {
			metricKey = span.Metric.Key
			metricBool = span.Metric.ValueBool
			metricNum = span.Metric.ValueNum
			metricStr = span.Metric.ValueStr
		}

		batch.Queue(
			`INSERT INTO telemetry_events (
				event_id, trace_id, span_id, parent_span_id, tuple_id, kind, name, ts_start, ts_end, status,
				attrs, metric_key, metric_value_bool, metric_value_num, metric_value_str, namespace_id, shard_id
			) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16,$17)`,
			uuid.New().String(),
			span.TraceID,
			span.SpanID,
			span.ParentSpanID,
			span.TupleID,
			span.Kind,
			span.Name,
			span.TsStart,
			span.TsEnd,
			span.Status,
			attrsJSON,
			metricKey,
			metricBool,
			metricNum,
			metricStr,
			span.NamespaceID,
			s.shardID,
		)
		queued++
	}

	if queued == 0 {
		return nil
	}

	results := s.pool.SendBatch(ctx, batch)
	defer results.Close()

	for i := 0; i < queued; i++ {
		if _, err := results.Exec(); err != nil {
			return err
		}
	}

	return nil
}

// ListSpans returns telemetry spans by trace ID, optionally scoped to a namespace.
func (s *PostgresStore) ListSpans(ctx context.Context, traceID string, namespaceID string, limit, offset int) ([]telemetry.Span, error) {
	if traceID == "" {
		return nil, fmt.Errorf("trace_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	if ctx == nil {
		ctx = context.Background()
	}

	query := fmt.Sprintf("SELECT %s FROM telemetry_events WHERE trace_id=$1", telemetrySelectColumns)
	args := []interface{}{traceID}
	if strings.TrimSpace(namespaceID) != "" {
		query += " AND namespace_id=$2"
		args = append(args, namespaceID)
	}
	query += fmt.Sprintf(" ORDER BY ts_start ASC LIMIT $%d OFFSET $%d", len(args)+1, len(args)+2)
	args = append(args, limit, offset)

	rows, err := s.pool.Query(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	spans := make([]telemetry.Span, 0)
	for rows.Next() {
		var (
			span            telemetry.Span
			spanID          string
			parentSpanID    string
			tupleID         string
			kind            string
			name            string
			status          string
			attrsRaw        []byte
			metricKey       string
			metricValueBool *bool
			metricValueNum  *float64
			metricValueStr  *string
			namespaceID     string
		)
		if err := rows.Scan(
			&span.TraceID,
			&spanID,
			&parentSpanID,
			&tupleID,
			&kind,
			&name,
			&span.TsStart,
			&span.TsEnd,
			&status,
			&attrsRaw,
			&metricKey,
			&metricValueBool,
			&metricValueNum,
			&metricValueStr,
			&namespaceID,
		); err != nil {
			return nil, err
		}

		span.SpanID = spanID
		span.ParentSpanID = parentSpanID
		span.TupleID = tupleID
		span.NamespaceID = namespaceID
		span.Kind = kind
		span.Name = name
		span.Status = status

		if len(attrsRaw) > 0 {
			var attrs map[string]interface{}
			if err := json.Unmarshal(attrsRaw, &attrs); err == nil {
				span.Attrs = attrs
			}
		}

		if metricKey != "" || metricValueBool != nil || metricValueNum != nil || metricValueStr != nil {
			span.Metric = &telemetry.Metric{
				Key:       metricKey,
				ValueBool: metricValueBool,
				ValueNum:  metricValueNum,
				ValueStr:  metricValueStr,
			}
		}

		spans = append(spans, span)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return spans, nil
}

// CleanupSpansBefore deletes telemetry rows older than the provided cutoff.
func (s *PostgresStore) CleanupSpansBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	ctx = defaultContext(ctx)
	if err := s.dropExpiredTelemetryPartitions(ctx, cutoff); err != nil && !strings.Contains(err.Error(), "does not exist") {
		return 0, err
	}
	cmdTag, err := s.pool.Exec(ctx, "DELETE FROM telemetry_events WHERE ts_start < $1", cutoff)
	if err != nil {
		return 0, err
	}
	return cmdTag.RowsAffected(), nil
}

// GetDAG returns the DAG of related agents.
