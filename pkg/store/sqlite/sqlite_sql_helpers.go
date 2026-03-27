package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func scanSQLiteAgent(row sqliteRow) (*agent.Agent, error) {
	var (
		id          string
		kind        string
		status      string
		payloadRaw  string
		tagsRaw     string
		owner       string
		parentID    string
		traceID     string
		ttlMs       int64
		createdAtMs int64
		updatedAtMs int64
		ownerTimeMs sql.NullInt64
		leaseToken  string
		leaseUntil  sql.NullInt64
		version     int64
		metadataRaw string
	)

	if err := row.Scan(
		&id,
		&kind,
		&status,
		&payloadRaw,
		&tagsRaw,
		&owner,
		&parentID,
		&traceID,
		&ttlMs,
		&createdAtMs,
		&updatedAtMs,
		&ownerTimeMs,
		&leaseToken,
		&leaseUntil,
		&version,
		&metadataRaw,
	); err != nil {
		return nil, err
	}

	payload := map[string]interface{}{}
	if payloadRaw != "" {
		if err := json.Unmarshal([]byte(payloadRaw), &payload); err != nil {
			return nil, err
		}
	}

	tags := []string{}
	if tagsRaw != "" {
		_ = json.Unmarshal([]byte(tagsRaw), &tags)
	}

	metadata := map[string]string{}
	if metadataRaw != "" {
		if err := json.Unmarshal([]byte(metadataRaw), &metadata); err != nil {
			var metaAny map[string]interface{}
			if err := json.Unmarshal([]byte(metadataRaw), &metaAny); err == nil {
				for k, v := range metaAny {
					if str, ok := v.(string); ok {
						metadata[k] = str
					}
				}
			}
		}
	}

	t := &agent.Agent{
		ID:         id,
		Kind:       kind,
		Status:     agent.Status(status),
		Payload:    payload,
		Tags:       tags,
		Owner:      owner,
		ParentID:   parentID,
		TraceID:    traceID,
		TTL:        time.Duration(ttlMs) * time.Millisecond,
		CreatedAt:  time.UnixMilli(createdAtMs).UTC(),
		UpdatedAt:  time.UnixMilli(updatedAtMs).UTC(),
		LeaseToken: leaseToken,
		Version:    version,
		Metadata:   metadata,
	}
	if ns := metadata[namespaceMetadataKey]; ns != "" {
		t.NamespaceID = ns
	}

	if ownerTimeMs.Valid {
		t.OwnerTime = time.UnixMilli(ownerTimeMs.Int64).UTC()
	}
	if leaseUntil.Valid {
		t.LeaseUntil = time.UnixMilli(leaseUntil.Int64).UTC()
	}

	return t, nil
}

func scanSQLiteEvent(row sqliteRow) (*agent.Event, error) {
	var (
		id        string
		tupleID   string
		eventType string
		kind      string
		timestamp int64
		agentID   string
		traceID   string
		version   int64
		dataRaw   string
	)

	if err := row.Scan(
		&id,
		&tupleID,
		&eventType,
		&kind,
		&timestamp,
		&agentID,
		&traceID,
		&version,
		&dataRaw,
	); err != nil {
		return nil, err
	}

	data := map[string]interface{}{}
	if dataRaw != "" {
		if err := json.Unmarshal([]byte(dataRaw), &data); err != nil {
			return nil, err
		}
	}

	return &agent.Event{
		ID:        id,
		TupleID:   tupleID,
		Type:      agent.EventType(eventType),
		Kind:      kind,
		Timestamp: time.UnixMilli(timestamp).UTC(),
		AgentID:   agentID,
		TraceID:   traceID,
		Version:   version,
		Data:      data,
	}, nil
}

func upsertSQLiteAgent(ctx context.Context, tx *sql.Tx, t *agent.Agent) error {
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	if t.NamespaceID != "" {
		t.Metadata[namespaceMetadataKey] = t.NamespaceID
	} else if ns := t.Metadata[namespaceMetadataKey]; ns != "" {
		t.NamespaceID = ns
	}

	payloadJSON, err := json.Marshal(t.Payload)
	if err != nil {
		return err
	}
	metadataJSON, err := json.Marshal(t.Metadata)
	if err != nil {
		return err
	}
	tagsJSON, err := json.Marshal(t.Tags)
	if err != nil {
		return err
	}

	ttlMs := int64(t.TTL / time.Millisecond)
	if t.TTL == 0 {
		ttlMs = 0
	}

	var ownerTime interface{} = nil
	if !t.OwnerTime.IsZero() {
		ownerTime = t.OwnerTime.UTC().UnixMilli()
	}
	var leaseUntil interface{} = nil
	if !t.LeaseUntil.IsZero() {
		leaseUntil = t.LeaseUntil.UTC().UnixMilli()
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO agents (
			id, kind, status, payload, tags, owner, parent_id, trace_id, ttl_ms,
			created_at_ms, updated_at_ms, owner_time_ms, lease_token, lease_until_ms, version, metadata
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?,
			?, ?, ?, ?, ?, ?, ?
		)
		ON CONFLICT(id) DO UPDATE SET
			kind = excluded.kind,
			status = excluded.status,
			payload = excluded.payload,
			tags = excluded.tags,
			owner = excluded.owner,
			parent_id = excluded.parent_id,
			trace_id = excluded.trace_id,
			ttl_ms = excluded.ttl_ms,
			created_at_ms = excluded.created_at_ms,
			updated_at_ms = excluded.updated_at_ms,
			owner_time_ms = excluded.owner_time_ms,
			lease_token = excluded.lease_token,
			lease_until_ms = excluded.lease_until_ms,
			version = excluded.version,
			metadata = excluded.metadata
	`,
		t.ID,
		t.Kind,
		string(t.Status),
		string(payloadJSON),
		string(tagsJSON),
		t.Owner,
		t.ParentID,
		t.TraceID,
		ttlMs,
		t.CreatedAt.UTC().UnixMilli(),
		t.UpdatedAt.UTC().UnixMilli(),
		ownerTime,
		t.LeaseToken,
		leaseUntil,
		t.Version,
		string(metadataJSON),
	)
	return err
}

func updateSQLiteAgent(ctx context.Context, tx *sql.Tx, t *agent.Agent) error {
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	if t.NamespaceID != "" {
		t.Metadata[namespaceMetadataKey] = t.NamespaceID
	} else if ns := t.Metadata[namespaceMetadataKey]; ns != "" {
		t.NamespaceID = ns
	}

	payloadJSON, err := json.Marshal(t.Payload)
	if err != nil {
		return err
	}
	metadataJSON, err := json.Marshal(t.Metadata)
	if err != nil {
		return err
	}
	tagsJSON, err := json.Marshal(t.Tags)
	if err != nil {
		return err
	}

	ttlMs := int64(t.TTL / time.Millisecond)
	if t.TTL == 0 {
		ttlMs = 0
	}

	var ownerTime interface{} = nil
	if !t.OwnerTime.IsZero() {
		ownerTime = t.OwnerTime.UTC().UnixMilli()
	}
	var leaseUntil interface{} = nil
	if !t.LeaseUntil.IsZero() {
		leaseUntil = t.LeaseUntil.UTC().UnixMilli()
	}

	_, err = tx.ExecContext(ctx, `
		UPDATE agents SET
			kind=?,
			status=?,
			payload=?,
			tags=?,
			owner=?,
			parent_id=?,
			trace_id=?,
			ttl_ms=?,
			created_at_ms=?,
			updated_at_ms=?,
			owner_time_ms=?,
			lease_token=?,
			lease_until_ms=?,
			version=?,
			metadata=?
		WHERE id=?
	`,
		t.Kind,
		string(t.Status),
		string(payloadJSON),
		string(tagsJSON),
		t.Owner,
		t.ParentID,
		t.TraceID,
		ttlMs,
		t.CreatedAt.UTC().UnixMilli(),
		t.UpdatedAt.UTC().UnixMilli(),
		ownerTime,
		t.LeaseToken,
		leaseUntil,
		t.Version,
		string(metadataJSON),
		t.ID,
	)
	return err
}

func claimSQLiteAgent(ctx context.Context, tx *sql.Tx, t *agent.Agent, nowMs int64) (bool, error) {
	var ownerTime interface{} = nil
	if !t.OwnerTime.IsZero() {
		ownerTime = t.OwnerTime.UTC().UnixMilli()
	}
	var leaseUntil interface{} = nil
	if !t.LeaseUntil.IsZero() {
		leaseUntil = t.LeaseUntil.UTC().UnixMilli()
	}

	res, err := tx.ExecContext(ctx, `
		UPDATE agents SET
			status=?,
			owner=?,
			owner_time_ms=?,
			lease_token=?,
			lease_until_ms=?,
			updated_at_ms=?,
			version=?
		WHERE id=?
		  AND status=?
		  AND (ttl_ms = 0 OR created_at_ms + ttl_ms > ?)
	`,
		string(t.Status),
		t.Owner,
		ownerTime,
		t.LeaseToken,
		leaseUntil,
		t.UpdatedAt.UTC().UnixMilli(),
		t.Version,
		t.ID,
		string(agent.StatusNew),
		nowMs,
	)
	if err != nil {
		return false, err
	}
	affected, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	return affected > 0, nil
}

func insertSQLiteEvent(ctx context.Context, tx *sql.Tx, event *agent.Event) error {
	dataJSON, err := json.Marshal(event.Data)
	if err != nil {
		return err
	}

	_, err = tx.ExecContext(ctx, `
		INSERT INTO agent_events (
			id, tuple_id, type, kind, timestamp_ms, agent_id, trace_id, version, data
		) VALUES (
			?, ?, ?, ?, ?, ?, ?, ?, ?
		)
	`,
		event.ID,
		event.TupleID,
		string(event.Type),
		event.Kind,
		event.Timestamp.UTC().UnixMilli(),
		event.AgentID,
		event.TraceID,
		event.Version,
		string(dataJSON),
	)
	return err
}

func sqliteIsNoRows(err error) bool {
	return errors.Is(err, sql.ErrNoRows)
}
