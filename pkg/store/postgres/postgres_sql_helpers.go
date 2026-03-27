package postgres

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

func scanAgent(row pgx.Row) (*agent.Agent, error) {
	var (
		id          string
		kind        string
		status      string
		payloadRaw  []byte
		tags        []string
		owner       string
		parentID    string
		traceID     string
		namespaceID string
		shardID     string
		ttlMs       int64
		createdAt   time.Time
		updatedAt   time.Time
		ownerTime   pgtype.Timestamptz
		leaseToken  string
		leaseUntil  pgtype.Timestamptz
		version     int64
		metadataRaw []byte
	)

	if err := row.Scan(
		&id,
		&kind,
		&status,
		&payloadRaw,
		&tags,
		&owner,
		&parentID,
		&traceID,
		&namespaceID,
		&shardID,
		&ttlMs,
		&createdAt,
		&updatedAt,
		&ownerTime,
		&leaseToken,
		&leaseUntil,
		&version,
		&metadataRaw,
	); err != nil {
		return nil, err
	}

	payload := map[string]interface{}{}
	if len(payloadRaw) > 0 {
		if err := json.Unmarshal(payloadRaw, &payload); err != nil {
			return nil, err
		}
	}

	metadata := map[string]string{}
	if len(metadataRaw) > 0 {
		if err := json.Unmarshal(metadataRaw, &metadata); err != nil {
			var metaAny map[string]interface{}
			if err := json.Unmarshal(metadataRaw, &metaAny); err == nil {
				for k, v := range metaAny {
					if str, ok := v.(string); ok {
						metadata[k] = str
					}
				}
			}
		}
	}

	t := &agent.Agent{
		ID:          id,
		Kind:        kind,
		Status:      agent.Status(status),
		Payload:     payload,
		Tags:        tags,
		Owner:       owner,
		ParentID:    parentID,
		TraceID:     traceID,
		NamespaceID: namespaceID,
		ShardID:     shardID,
		TTL:         time.Duration(ttlMs) * time.Millisecond,
		CreatedAt:   createdAt,
		UpdatedAt:   updatedAt,
		LeaseToken:  leaseToken,
		Version:     version,
		Metadata:    metadata,
	}
	if t.NamespaceID == "" {
		if ns := metadata[namespaceMetadataKey]; ns != "" {
			t.NamespaceID = ns
		}
	}
	if ns := t.NamespaceID; ns != "" {
		t.NamespaceID = ns
	}

	if ownerTime.Valid {
		t.OwnerTime = ownerTime.Time
	}
	if leaseUntil.Valid {
		t.LeaseUntil = leaseUntil.Time
	}

	return t, nil
}

func scanEvent(row pgx.Row) (*agent.Event, error) {
	var (
		id        string
		tupleID   string
		eventType string
		kind      string
		timestamp time.Time
		agentID   string
		traceID   string
		version   int64
		dataRaw   []byte
		namespace string
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
		&namespace,
	); err != nil {
		return nil, err
	}

	data := map[string]interface{}{}
	if len(dataRaw) > 0 {
		if err := json.Unmarshal(dataRaw, &data); err != nil {
			return nil, err
		}
	}

	return &agent.Event{
		ID:        id,
		TupleID:   tupleID,
		Type:      agent.EventType(eventType),
		Kind:      kind,
		Timestamp: timestamp,
		AgentID:   agentID,
		TraceID:   traceID,
		Version:   version,
		Data:      data,
	}, nil
}

func upsertAgent(ctx context.Context, tx pgx.Tx, t *agent.Agent) error {
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
	queue := ""
	if t.Metadata != nil {
		queue = t.Metadata[queueMetadataKey]
	}

	var ownerTime *time.Time
	if !t.OwnerTime.IsZero() {
		ot := t.OwnerTime.UTC()
		ownerTime = &ot
	}
	var leaseUntil *time.Time
	if !t.LeaseUntil.IsZero() {
		lu := t.LeaseUntil.UTC()
		leaseUntil = &lu
	}

	ttlMs := int64(t.TTL / time.Millisecond)
	if t.TTL == 0 {
		ttlMs = 0
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO agents (
			id, kind, status, payload, tags, owner, parent_id, trace_id, queue, namespace_id, shard_id, ttl_ms,
			created_at, updated_at, owner_time, lease_token, lease_until, version, metadata
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,
			$11,$12,$13,$14,$15,$16,$17,$18,$19
		)
		ON CONFLICT (id) DO UPDATE SET
			kind = EXCLUDED.kind,
			status = EXCLUDED.status,
			payload = EXCLUDED.payload,
			tags = EXCLUDED.tags,
			owner = EXCLUDED.owner,
			parent_id = EXCLUDED.parent_id,
			trace_id = EXCLUDED.trace_id,
			queue = EXCLUDED.queue,
			namespace_id = EXCLUDED.namespace_id,
			shard_id = EXCLUDED.shard_id,
			ttl_ms = EXCLUDED.ttl_ms,
			created_at = EXCLUDED.created_at,
			updated_at = EXCLUDED.updated_at,
			owner_time = EXCLUDED.owner_time,
			lease_token = EXCLUDED.lease_token,
			lease_until = EXCLUDED.lease_until,
			version = EXCLUDED.version,
			metadata = EXCLUDED.metadata
	`,
		t.ID,
		t.Kind,
		string(t.Status),
		payloadJSON,
		t.Tags,
		t.Owner,
		t.ParentID,
		t.TraceID,
		queue,
		t.NamespaceID,
		t.ShardID,
		ttlMs,
		t.CreatedAt.UTC(),
		t.UpdatedAt.UTC(),
		ownerTime,
		t.LeaseToken,
		leaseUntil,
		t.Version,
		metadataJSON,
	)
	return err
}

func updateAgentRow(ctx context.Context, tx pgx.Tx, t *agent.Agent) error {
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
	queue := ""
	if t.Metadata != nil {
		queue = t.Metadata[queueMetadataKey]
	}

	var ownerTime *time.Time
	if !t.OwnerTime.IsZero() {
		ot := t.OwnerTime.UTC()
		ownerTime = &ot
	}
	var leaseUntil *time.Time
	if !t.LeaseUntil.IsZero() {
		lu := t.LeaseUntil.UTC()
		leaseUntil = &lu
	}

	ttlMs := int64(t.TTL / time.Millisecond)
	if t.TTL == 0 {
		ttlMs = 0
	}

	_, err = tx.Exec(ctx, `
		UPDATE agents SET
			kind=$1,
			status=$2,
			payload=$3,
			tags=$4,
			owner=$5,
			parent_id=$6,
			trace_id=$7,
			queue=$8,
			namespace_id=$9,
			shard_id=$10,
			ttl_ms=$11,
			created_at=$12,
			updated_at=$13,
			owner_time=$14,
			lease_token=$15,
			lease_until=$16,
			version=$17,
			metadata=$18
		WHERE id=$19
	`,
		t.Kind,
		string(t.Status),
		payloadJSON,
		t.Tags,
		t.Owner,
		t.ParentID,
		t.TraceID,
		queue,
		t.NamespaceID,
		t.ShardID,
		ttlMs,
		t.CreatedAt.UTC(),
		t.UpdatedAt.UTC(),
		ownerTime,
		t.LeaseToken,
		leaseUntil,
		t.Version,
		metadataJSON,
		t.ID,
	)
	return err
}

func updateAgentCompletionRow(ctx context.Context, tx pgx.Tx, t *agent.Agent) error {
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

	var ownerTime *time.Time
	if !t.OwnerTime.IsZero() {
		ot := t.OwnerTime.UTC()
		ownerTime = &ot
	}
	var leaseUntil *time.Time
	if !t.LeaseUntil.IsZero() {
		lu := t.LeaseUntil.UTC()
		leaseUntil = &lu
	}

	_, err = tx.Exec(ctx, `
		UPDATE agents SET
			status=$1,
			payload=$2,
			owner=$3,
			owner_time=$4,
			lease_token=$5,
			lease_until=$6,
			updated_at=$7,
			version=$8,
			metadata=$9,
			namespace_id=$10
		WHERE id=$11
	`,
		string(t.Status),
		payloadJSON,
		t.Owner,
		ownerTime,
		t.LeaseToken,
		leaseUntil,
		t.UpdatedAt.UTC(),
		t.Version,
		metadataJSON,
		t.NamespaceID,
		t.ID,
	)
	return err
}

func updateAgentReleaseRow(ctx context.Context, tx pgx.Tx, tupleID string, updatedAt time.Time, version int64) error {
	_, err := tx.Exec(ctx, `
		UPDATE agents SET
			status=$1,
			owner='',
			owner_time=NULL,
			lease_token='',
			lease_until=NULL,
			updated_at=$2,
			version=$3
		WHERE id=$4
	`,
		string(agent.StatusNew),
		updatedAt.UTC(),
		version,
		tupleID,
	)
	return err
}

func updateAgentRenewLeaseRow(ctx context.Context, tx pgx.Tx, tupleID string, ownerTime time.Time, leaseUntil time.Time, updatedAt time.Time) error {
	_, err := tx.Exec(ctx, `
		UPDATE agents SET
			owner_time=$1,
			lease_until=$2,
			updated_at=$3
		WHERE id=$4
	`,
		ownerTime.UTC(),
		leaseUntil.UTC(),
		updatedAt.UTC(),
		tupleID,
	)
	return err
}

func queueInsertEvent(batch *pgx.Batch, event *agent.Event, namespaceID string, shardID string) error {
	dataJSON, err := json.Marshal(event.Data)
	if err != nil {
		return err
	}

	batch.Queue(`
		INSERT INTO agent_events (
			id, tuple_id, type, kind, timestamp, agent_id, trace_id, version, data, namespace_id, shard_id
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
		)
	`,
		event.ID,
		event.TupleID,
		string(event.Type),
		event.Kind,
		event.Timestamp.UTC(),
		event.AgentID,
		event.TraceID,
		event.Version,
		dataJSON,
		namespaceID,
		shardID,
	)
	return nil
}

func insertEvent(ctx context.Context, tx pgx.Tx, event *agent.Event, namespaceID string, shardID string) error {
	dataJSON, err := json.Marshal(event.Data)
	if err != nil {
		return err
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO agent_events (
			id, tuple_id, type, kind, timestamp, agent_id, trace_id, version, data, namespace_id, shard_id
		) VALUES (
			$1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11
		)
	`,
		event.ID,
		event.TupleID,
		string(event.Type),
		event.Kind,
		event.Timestamp.UTC(),
		event.AgentID,
		event.TraceID,
		event.Version,
		dataJSON,
		namespaceID,
		shardID,
	)
	return err
}

func errorsIsNoRows(err error) bool {
	return err == pgx.ErrNoRows
}

// leaseReaperLoop periodically checks for and reaps expired leases.
