package postgressharded

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strings"
	"sync"
	"time"

	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/store/internal/common"
	postgresbackend "github.com/urobora-ai/agentspaces/pkg/store/postgres"
	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

const (
	namespaceMetadataKey = "namespace_id"
	queueMetadataKey     = "queue"
)

var errCrossShardAtomicUnsupported = errors.New("cross_shard_atomic_unsupported")

type shardHandle struct {
	spec  ShardSpec
	store *postgresbackend.PostgresStore
}

type remoteShardOwnerError struct {
	shardID       string
	ownerNodeID   string
	advertiseAddr string
}

func (e *remoteShardOwnerError) Error() string {
	if e == nil {
		return ""
	}
	if e.advertiseAddr != "" {
		return fmt.Sprintf("shard %s owned by %s at %s", e.shardID, e.ownerNodeID, e.advertiseAddr)
	}
	if e.ownerNodeID != "" {
		return fmt.Sprintf("shard %s owned by %s", e.shardID, e.ownerNodeID)
	}
	return fmt.Sprintf("shard %s not owned by this node", e.shardID)
}

type PostgresShardedStore struct {
	logger        *zap.Logger
	ctx           context.Context
	cancel        context.CancelFunc
	cfg           *Config
	shardMap      *ShardMap
	specByShardID map[string]ShardSpec
	shards        []*shardHandle
	byShardID     map[string]*shardHandle
	idToShard     sync.Map
	subscriptions *common.SubscriptionRegistry
}

func New(ctx context.Context, cfg *Config, logger *zap.Logger) (*PostgresShardedStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("postgres sharded config is nil")
	}
	if strings.TrimSpace(cfg.ShardMapFile) == "" {
		return nil, fmt.Errorf("postgres sharded config missing shard_map_file")
	}
	shardMap, err := loadShardMap(cfg.ShardMapFile)
	if err != nil {
		return nil, err
	}
	storeCtx, cancel := context.WithCancel(defaultContext(ctx))
	s := &PostgresShardedStore{
		logger:        logger,
		ctx:           storeCtx,
		cancel:        cancel,
		cfg:           cfg,
		shardMap:      shardMap,
		specByShardID: make(map[string]ShardSpec, len(shardMap.Shards)),
		byShardID:     make(map[string]*shardHandle, len(shardMap.Shards)),
		subscriptions: common.NewSubscriptionRegistry(),
	}
	for _, spec := range shardMap.Shards {
		s.specByShardID[spec.ShardID] = spec
		if !s.ownsShard(spec) {
			continue
		}
		pgCfg, err := postgresConfigFromShardSpec(cfg, spec)
		if err != nil {
			cancel()
			return nil, err
		}
		pgStore, err := postgresbackend.NewPostgresStore(storeCtx, pgCfg, logger)
		if err != nil {
			cancel()
			return nil, fmt.Errorf("create shard %q store: %w", spec.ShardID, err)
		}
		handle := &shardHandle{spec: spec, store: pgStore}
		s.shards = append(s.shards, handle)
		s.byShardID[spec.ShardID] = handle
	}
	return s, nil
}

func (s *PostgresShardedStore) ownsShard(spec ShardSpec) bool {
	if s == nil || s.cfg == nil {
		return true
	}
	nodeID := strings.TrimSpace(s.cfg.NodeID)
	if nodeID == "" {
		return true
	}
	ownerNodeID := strings.TrimSpace(spec.OwnerNodeID)
	if ownerNodeID == "" {
		return true
	}
	return ownerNodeID == nodeID
}

func defaultContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (s *PostgresShardedStore) Close() error {
	s.cancel()
	var firstErr error
	for _, shard := range s.shards {
		if err := shard.store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func (s *PostgresShardedStore) BindContext(ctx context.Context) agent.AgentSpace {
	return &contextBoundStore{store: s, ctx: defaultContext(ctx)}
}

func hashKey(namespaceID string, queue string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(namespaceID))
	_, _ = h.Write([]byte(":"))
	_, _ = h.Write([]byte(queue))
	return h.Sum32()
}

func (s *PostgresShardedStore) targetShardSpec(namespaceID string, queue string) (ShardSpec, error) {
	namespaceID = strings.TrimSpace(namespaceID)
	queue = strings.TrimSpace(queue)
	if namespaceID == "" {
		return ShardSpec{}, fmt.Errorf("namespace_id is required for postgres-sharded hot-path routing")
	}
	if queue == "" {
		return ShardSpec{}, fmt.Errorf("queue metadata is required for postgres-sharded hot-path routing")
	}
	if s == nil || s.shardMap == nil || len(s.shardMap.Shards) == 0 {
		return ShardSpec{}, fmt.Errorf("postgres-sharded store has no shards")
	}
	idx := int(hashKey(namespaceID, queue) % uint32(len(s.shardMap.Shards)))
	return s.shardMap.Shards[idx], nil
}

func (s *PostgresShardedStore) remoteShardOwner(spec ShardSpec) error {
	return &remoteShardOwnerError{
		shardID:       spec.ShardID,
		ownerNodeID:   spec.OwnerNodeID,
		advertiseAddr: spec.AdvertiseAddr,
	}
}

func (s *PostgresShardedStore) shardForNamespaceQueue(namespaceID string, queue string) (*shardHandle, error) {
	spec, err := s.targetShardSpec(namespaceID, queue)
	if err != nil {
		return nil, err
	}
	if shard, ok := s.byShardID[spec.ShardID]; ok {
		return shard, nil
	}
	return nil, s.remoteShardOwner(spec)
}

func (s *PostgresShardedStore) shardFromHint(ctx context.Context) (*shardHandle, bool, error) {
	shardID := strings.TrimSpace(agent.ShardIDFromContext(ctx))
	if shardID == "" {
		return nil, false, nil
	}
	if shard, ok := s.byShardID[shardID]; ok {
		return shard, true, nil
	}
	if spec, ok := s.specByShardID[shardID]; ok {
		return nil, true, s.remoteShardOwner(spec)
	}
	return nil, true, fmt.Errorf("unknown shard_id %q", shardID)
}

func (s *PostgresShardedStore) shardFromAgent(t *agent.Agent) (*shardHandle, error) {
	if t == nil {
		return nil, fmt.Errorf("agent is nil")
	}
	namespaceID := strings.TrimSpace(t.NamespaceID)
	if namespaceID == "" && t.Metadata != nil {
		namespaceID = strings.TrimSpace(t.Metadata[namespaceMetadataKey])
	}
	queue := ""
	if t.Metadata != nil {
		queue = strings.TrimSpace(t.Metadata[queueMetadataKey])
	}
	return s.shardForNamespaceQueue(namespaceID, queue)
}

func queryNamespaceAndQueue(query *agent.Query) (string, string) {
	if query == nil {
		return "", ""
	}
	namespaceID := strings.TrimSpace(query.NamespaceID)
	queue := ""
	if query.Metadata != nil {
		queue = strings.TrimSpace(query.Metadata[queueMetadataKey])
	}
	return namespaceID, queue
}

func (s *PostgresShardedStore) shardFromQuery(query *agent.Query) (*shardHandle, bool, error) {
	namespaceID, queue := queryNamespaceAndQueue(query)
	if namespaceID == "" || queue == "" {
		return nil, false, nil
	}
	shard, err := s.shardForNamespaceQueue(namespaceID, queue)
	return shard, true, err
}

func (s *PostgresShardedStore) annotateAgent(t *agent.Agent, shardID string) *agent.Agent {
	if t == nil {
		return nil
	}
	t.ShardID = shardID
	s.idToShard.Store(t.ID, shardID)
	return t
}

func (s *PostgresShardedStore) annotateAgents(values []*agent.Agent, shardID string) []*agent.Agent {
	for _, value := range values {
		s.annotateAgent(value, shardID)
	}
	return values
}

func (s *PostgresShardedStore) orderedShardIDs() []string {
	out := make([]string, 0, len(s.shards))
	for _, shard := range s.shards {
		out = append(out, shard.spec.ShardID)
	}
	sort.Strings(out)
	return out
}

func (s *PostgresShardedStore) shardByTupleID(ctx context.Context, id string) (*shardHandle, error) {
	if shard, ok, err := s.shardFromHint(ctx); ok || err != nil {
		return shard, err
	}
	if cached, ok := s.idToShard.Load(id); ok {
		if shardID, ok := cached.(string); ok {
			if shard, exists := s.byShardID[shardID]; exists {
				if t, err := shard.store.Get(id); err == nil && t != nil {
					s.annotateAgent(t, shardID)
					return shard, nil
				}
			}
		}
	}
	for _, shardID := range s.orderedShardIDs() {
		shard := s.byShardID[shardID]
		t, err := shard.store.Get(id)
		if err != nil || t == nil {
			continue
		}
		s.annotateAgent(t, shardID)
		return shard, nil
	}
	return nil, fmt.Errorf("agent not found: %s", id)
}

func applyGlobalLimitOffset(values []*agent.Agent, limit, offset int) []*agent.Agent {
	if offset > 0 {
		if offset >= len(values) {
			return []*agent.Agent{}
		}
		values = values[offset:]
	}
	if limit > 0 && limit < len(values) {
		values = values[:limit]
	}
	return values
}

func (s *PostgresShardedStore) Get(id string) (*agent.Agent, error) {
	return s.getWithContext(context.Background(), id)
}

func (s *PostgresShardedStore) getWithContext(ctx context.Context, id string) (*agent.Agent, error) {
	shard, err := s.shardByTupleID(ctx, id)
	if err != nil {
		return nil, err
	}
	t, err := shard.store.Get(id)
	if err != nil {
		return nil, err
	}
	return s.annotateAgent(t, shard.spec.ShardID), nil
}

func (s *PostgresShardedStore) Out(t *agent.Agent) error {
	shard, err := s.shardFromAgent(t)
	if err != nil {
		return err
	}
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	t.ShardID = shard.spec.ShardID
	if err := shard.store.Out(t); err != nil {
		return err
	}
	s.idToShard.Store(t.ID, shard.spec.ShardID)
	return nil
}

func (s *PostgresShardedStore) Take(query *agent.Query) (*agent.Agent, error) {
	shard, ok, err := s.shardFromQuery(query)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("postgres-sharded take requires namespace_id and metadata.queue")
	}
	t, err := shard.store.Take(query)
	if err != nil {
		return nil, err
	}
	return s.annotateAgent(t, shard.spec.ShardID), nil
}

func (s *PostgresShardedStore) BatchTake(query *agent.Query, limit int) ([]*agent.Agent, error) {
	shard, ok, err := s.shardFromQuery(query)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("postgres-sharded batch-take requires namespace_id and metadata.queue")
	}
	agents, err := shard.store.BatchTake(query, limit)
	if err != nil {
		return nil, err
	}
	for i, t := range agents {
		agents[i] = s.annotateAgent(t, shard.spec.ShardID)
	}
	return agents, nil
}

func (s *PostgresShardedStore) In(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	shard, ok, err := s.shardFromQuery(query)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, fmt.Errorf("postgres-sharded in requires namespace_id and metadata.queue")
	}
	t, err := shard.store.In(query, timeout)
	if err != nil {
		return nil, err
	}
	return s.annotateAgent(t, shard.spec.ShardID), nil
}

func (s *PostgresShardedStore) Read(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	if shard, ok, err := s.shardFromQuery(query); err != nil {
		return nil, err
	} else if ok {
		t, err := shard.store.Read(query, timeout)
		if err != nil {
			return nil, err
		}
		return s.annotateAgent(t, shard.spec.ShardID), nil
	}
	deadline := time.Now().Add(timeout)
	for {
		results, err := s.Query(query)
		if err != nil {
			return nil, err
		}
		if len(results) > 0 {
			return results[0], nil
		}
		if timeout <= 0 || time.Now().After(deadline) {
			return nil, nil
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (s *PostgresShardedStore) Query(query *agent.Query) ([]*agent.Agent, error) {
	if shard, ok, err := s.shardFromQuery(query); err != nil {
		return nil, err
	} else if ok {
		results, err := shard.store.Query(query)
		if err != nil {
			return nil, err
		}
		return s.annotateAgents(results, shard.spec.ShardID), nil
	}
	perShardQuery := &agent.Query{}
	if query != nil {
		*perShardQuery = *query
		perShardQuery.Limit = 0
		perShardQuery.Offset = 0
	}
	all := make([]*agent.Agent, 0)
	for _, shard := range s.shards {
		results, err := shard.store.Query(perShardQuery)
		if err != nil {
			return nil, err
		}
		all = append(all, s.annotateAgents(results, shard.spec.ShardID)...)
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].CreatedAt.Equal(all[j].CreatedAt) {
			return all[i].ID < all[j].ID
		}
		return all[i].CreatedAt.Before(all[j].CreatedAt)
	})
	if query == nil {
		return all, nil
	}
	return applyGlobalLimitOffset(all, query.Limit, query.Offset), nil
}

func (s *PostgresShardedStore) Count(query *agent.Query) (int, error) {
	if shard, ok, err := s.shardFromQuery(query); err != nil {
		return 0, err
	} else if ok {
		return shard.store.Count(query)
	}
	total := 0
	for _, shard := range s.shards {
		count, err := shard.store.Count(query)
		if err != nil {
			return 0, err
		}
		total += count
	}
	return total, nil
}

func (s *PostgresShardedStore) CountByStatus(query *agent.Query) (map[agent.Status]int, error) {
	if shard, ok, err := s.shardFromQuery(query); err != nil {
		return nil, err
	} else if ok {
		return shard.store.CountByStatus(query)
	}
	counts := make(map[agent.Status]int)
	for _, shard := range s.shards {
		current, err := shard.store.CountByStatus(query)
		if err != nil {
			return nil, err
		}
		for status, count := range current {
			counts[status] += count
		}
	}
	return counts, nil
}

func (s *PostgresShardedStore) Update(id string, updates map[string]interface{}) error {
	if metadataRaw, ok := updates["metadata"]; ok && metadataRaw != nil {
		if metadata, ok := metadataRaw.(map[string]string); ok {
			if queue := strings.TrimSpace(metadata[queueMetadataKey]); queue != "" {
				return fmt.Errorf("postgres-sharded update cannot move agents across queues/shards")
			}
		}
	}
	return s.updateWithContext(context.Background(), id, updates)
}

func (s *PostgresShardedStore) updateWithContext(ctx context.Context, id string, updates map[string]interface{}) error {
	shard, err := s.shardByTupleID(ctx, id)
	if err != nil {
		return err
	}
	return shard.store.Update(id, updates)
}

func (s *PostgresShardedStore) UpdateAndGet(id string, updates map[string]interface{}) (*agent.Agent, error) {
	if err := s.Update(id, updates); err != nil {
		return nil, err
	}
	return s.Get(id)
}

func (s *PostgresShardedStore) Complete(id string, agentID string, leaseToken string, result map[string]interface{}) error {
	_, err := s.completeAndGetWithContext(context.Background(), id, agentID, leaseToken, result)
	return err
}

func (s *PostgresShardedStore) CompleteAndGet(id string, agentID string, leaseToken string, result map[string]interface{}) (*agent.Agent, error) {
	return s.completeAndGetWithContext(context.Background(), id, agentID, leaseToken, result)
}

func (s *PostgresShardedStore) completeAndGetWithContext(
	ctx context.Context,
	id string,
	agentID string,
	leaseToken string,
	result map[string]interface{},
) (*agent.Agent, error) {
	shard, err := s.shardByTupleID(ctx, id)
	if err != nil {
		return nil, err
	}
	t, err := shard.store.CompleteAndGet(id, agentID, leaseToken, result)
	if err != nil {
		return nil, err
	}
	return s.annotateAgent(t, shard.spec.ShardID), nil
}

func (s *PostgresShardedStore) CompleteAndOut(id string, agentID string, leaseToken string, result map[string]interface{}, outputs []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	return s.completeAndOutWithContext(context.Background(), id, agentID, leaseToken, result, outputs)
}

func (s *PostgresShardedStore) completeAndOutWithContext(
	ctx context.Context,
	id string,
	agentID string,
	leaseToken string,
	result map[string]interface{},
	outputs []*agent.Agent,
) (*agent.Agent, []*agent.Agent, error) {
	source, err := s.getWithContext(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	sourceShard, ok := s.byShardID[source.ShardID]
	if !ok {
		return nil, nil, fmt.Errorf("source shard %q not configured", source.ShardID)
	}
	for _, output := range outputs {
		shard, err := s.shardFromAgent(output)
		if err != nil {
			return nil, nil, err
		}
		if shard.spec.ShardID != sourceShard.spec.ShardID {
			return nil, nil, fmt.Errorf("%w: source=%s output=%s", errCrossShardAtomicUnsupported, sourceShard.spec.ShardID, shard.spec.ShardID)
		}
		output.ShardID = shard.spec.ShardID
	}
	completed, created, err := sourceShard.store.CompleteAndOut(id, agentID, leaseToken, result, outputs)
	if err != nil {
		return nil, nil, err
	}
	s.annotateAgent(completed, sourceShard.spec.ShardID)
	s.annotateAgents(created, sourceShard.spec.ShardID)
	return completed, created, nil
}

func (s *PostgresShardedStore) Release(id string, agentID string, leaseToken string, reason string) error {
	_, err := s.releaseAndGetWithContext(context.Background(), id, agentID, leaseToken, reason)
	return err
}

func (s *PostgresShardedStore) ReleaseAndGet(id string, agentID string, leaseToken string, reason string) (*agent.Agent, error) {
	return s.releaseAndGetWithContext(context.Background(), id, agentID, leaseToken, reason)
}

func (s *PostgresShardedStore) releaseAndGetWithContext(
	ctx context.Context,
	id string,
	agentID string,
	leaseToken string,
	reason string,
) (*agent.Agent, error) {
	shard, err := s.shardByTupleID(ctx, id)
	if err != nil {
		return nil, err
	}
	t, err := shard.store.ReleaseAndGet(id, agentID, leaseToken, reason)
	if err != nil {
		return nil, err
	}
	return s.annotateAgent(t, shard.spec.ShardID), nil
}

func (s *PostgresShardedStore) RenewLease(id string, agentID string, leaseToken string, leaseDuration time.Duration) (*agent.Agent, error) {
	return s.renewLeaseWithContext(context.Background(), id, agentID, leaseToken, leaseDuration)
}

func (s *PostgresShardedStore) renewLeaseWithContext(
	ctx context.Context,
	id string,
	agentID string,
	leaseToken string,
	leaseDuration time.Duration,
) (*agent.Agent, error) {
	shard, err := s.shardByTupleID(ctx, id)
	if err != nil {
		return nil, err
	}
	t, err := shard.store.RenewLease(id, agentID, leaseToken, leaseDuration)
	if err != nil {
		return nil, err
	}
	return s.annotateAgent(t, shard.spec.ShardID), nil
}

func (s *PostgresShardedStore) Subscribe(filter *agent.EventFilter, handler agent.EventHandler) error {
	for _, shard := range s.shards {
		if err := shard.store.Subscribe(filter, handler); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresShardedStore) GetEvents(tupleID string) ([]*agent.Event, error) {
	return s.getEventsWithContext(context.Background(), tupleID)
}

func (s *PostgresShardedStore) getEventsWithContext(ctx context.Context, tupleID string) ([]*agent.Event, error) {
	shard, err := s.shardByTupleID(ctx, tupleID)
	if err != nil {
		return nil, err
	}
	return shard.store.GetEvents(tupleID)
}

func (s *PostgresShardedStore) GetGlobalEvents(limit, offset int) ([]*agent.Event, error) {
	all := make([]*agent.Event, 0)
	fetch := limit + offset
	if fetch <= 0 {
		fetch = 100
	}
	for _, shard := range s.shards {
		events, err := shard.store.GetGlobalEvents(fetch, 0)
		if err != nil {
			return nil, err
		}
		all = append(all, events...)
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Timestamp.Equal(all[j].Timestamp) {
			return all[i].ID < all[j].ID
		}
		return all[i].Timestamp.After(all[j].Timestamp)
	})
	if offset > 0 {
		if offset >= len(all) {
			return []*agent.Event{}, nil
		}
		all = all[offset:]
	}
	if limit > 0 && limit < len(all) {
		all = all[:limit]
	}
	return all, nil
}

func (s *PostgresShardedStore) QueryEvents(query *agent.EventQuery) (*agent.EventPage, error) {
	return s.queryEventsWithContext(context.Background(), query)
}

func (s *PostgresShardedStore) queryEventsWithContext(ctx context.Context, query *agent.EventQuery) (*agent.EventPage, error) {
	if query != nil && query.TupleID != "" {
		shard, err := s.shardByTupleID(ctx, query.TupleID)
		if err != nil {
			return nil, err
		}
		return shard.store.QueryEvents(query)
	}
	all := make([]*agent.Event, 0)
	total := 0
	for _, shard := range s.shards {
		page, err := shard.store.QueryEvents(query)
		if err != nil {
			return nil, err
		}
		if page == nil {
			continue
		}
		total += page.Total
		all = append(all, page.Events...)
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Timestamp.Equal(all[j].Timestamp) {
			return all[i].ID < all[j].ID
		}
		return all[i].Timestamp.After(all[j].Timestamp)
	})
	offset := 0
	limit := 0
	if query != nil {
		offset = query.Offset
		limit = query.Limit
	}
	if offset > 0 {
		if offset >= len(all) {
			return &agent.EventPage{Events: []*agent.Event{}, Total: total}, nil
		}
		all = all[offset:]
	}
	if limit > 0 && limit < len(all) {
		all = all[:limit]
	}
	return &agent.EventPage{Events: all, Total: total}, nil
}

func (s *PostgresShardedStore) GetDAG(rootID string) (*agent.DAG, error) {
	return s.getDAGWithContext(context.Background(), rootID)
}

func (s *PostgresShardedStore) getDAGWithContext(ctx context.Context, rootID string) (*agent.DAG, error) {
	shard, err := s.shardByTupleID(ctx, rootID)
	if err != nil {
		return nil, err
	}
	return shard.store.GetDAG(rootID)
}

func (s *PostgresShardedStore) Health(ctx context.Context) error {
	for _, shard := range s.shards {
		if err := shard.store.Health(ctx); err != nil {
			return fmt.Errorf("shard %s unhealthy: %w", shard.spec.ShardID, err)
		}
	}
	return nil
}

func (s *PostgresShardedStore) RecordSpans(ctx context.Context, spans []telemetry.Span) error {
	byShard := make(map[string][]telemetry.Span)
	for _, span := range spans {
		var shard *shardHandle
		if span.TupleID != "" {
			found, err := s.shardByTupleID(ctx, span.TupleID)
			if err == nil {
				shard = found
			}
		}
		if shard == nil {
			if len(s.shards) == 0 {
				continue
			}
			shard = s.shards[0]
		}
		byShard[shard.spec.ShardID] = append(byShard[shard.spec.ShardID], span)
	}
	for shardID, shardSpans := range byShard {
		if err := s.byShardID[shardID].store.RecordSpans(ctx, shardSpans); err != nil {
			return err
		}
	}
	return nil
}

func (s *PostgresShardedStore) ListSpans(ctx context.Context, traceID string, namespaceID string, limit, offset int) ([]telemetry.Span, error) {
	all := make([]telemetry.Span, 0)
	for _, shard := range s.shards {
		values, err := shard.store.ListSpans(ctx, traceID, namespaceID, limit+offset, 0)
		if err != nil {
			return nil, err
		}
		all = append(all, values...)
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].TsStart.Equal(all[j].TsStart) {
			return all[i].SpanID < all[j].SpanID
		}
		return all[i].TsStart.Before(all[j].TsStart)
	})
	if offset > 0 {
		if offset >= len(all) {
			return []telemetry.Span{}, nil
		}
		all = all[offset:]
	}
	if limit > 0 && limit < len(all) {
		all = all[:limit]
	}
	return all, nil
}

type ShardSummary struct {
	ShardID        string `json:"shard_id"`
	OwnerNodeID    string `json:"owner_node_id,omitempty"`
	AdvertiseAddr  string `json:"advertise_addr,omitempty"`
	WriterEndpoint string `json:"writer_endpoint"`
}

type ShardHealth struct {
	ShardID       string `json:"shard_id"`
	OwnerNodeID   string `json:"owner_node_id,omitempty"`
	AdvertiseAddr string `json:"advertise_addr,omitempty"`
	Ready         bool   `json:"ready"`
	Error         string `json:"error,omitempty"`
}

func (s *PostgresShardedStore) ShardMapInfo(ctx context.Context) (map[string]interface{}, error) {
	summaries := make([]ShardSummary, 0, len(s.shards))
	for _, shard := range s.shards {
		summaries = append(summaries, ShardSummary{
			ShardID:        shard.spec.ShardID,
			OwnerNodeID:    shard.spec.OwnerNodeID,
			AdvertiseAddr:  shard.spec.AdvertiseAddr,
			WriterEndpoint: redactDSN(shard.spec.WriterDSN),
		})
	}
	return map[string]interface{}{
		"version":        s.shardMap.Version,
		"epoch":          s.shardMap.Epoch,
		"node_id":        s.cfg.NodeID,
		"advertise_addr": s.cfg.AdvertiseAddr,
		"shards":         summaries,
	}, nil
}

func (s *PostgresShardedStore) ShardHealthInfo(ctx context.Context) ([]ShardHealth, error) {
	out := make([]ShardHealth, 0, len(s.shards))
	for _, shard := range s.shards {
		err := shard.store.Health(ctx)
		out = append(out, ShardHealth{
			ShardID:       shard.spec.ShardID,
			OwnerNodeID:   shard.spec.OwnerNodeID,
			AdvertiseAddr: shard.spec.AdvertiseAddr,
			Ready:         err == nil,
			Error:         errorString(err),
		})
	}
	return out, nil
}

func (s *PostgresShardedStore) ShardQueueStats(ctx context.Context, namespaceID string, queue string) (map[string]interface{}, error) {
	shard, err := s.shardForNamespaceQueue(namespaceID, queue)
	if err != nil {
		return nil, err
	}
	counts, err := shard.store.CountByStatus(&agent.Query{
		NamespaceID: namespaceID,
		Metadata:    map[string]string{queueMetadataKey: queue},
	})
	if err != nil {
		return nil, err
	}
	total := 0
	normalized := make(map[string]int, len(counts))
	for status, count := range counts {
		normalized[string(status)] = count
		total += count
	}
	return map[string]interface{}{
		"namespace_id": namespaceID,
		"queue":        queue,
		"shard_id":     shard.spec.ShardID,
		"counts":       normalized,
		"total":        total,
	}, nil
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

type contextBoundStore struct {
	store *PostgresShardedStore
	ctx   context.Context
}

func (b *contextBoundStore) Get(id string) (*agent.Agent, error) {
	return b.store.getWithContext(b.ctx, id)
}

func (b *contextBoundStore) Out(t *agent.Agent) error {
	return b.store.Out(t)
}

func (b *contextBoundStore) In(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return b.store.In(query, timeout)
}

func (b *contextBoundStore) Read(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return b.store.Read(query, timeout)
}

func (b *contextBoundStore) Query(query *agent.Query) ([]*agent.Agent, error) {
	return b.store.Query(query)
}

func (b *contextBoundStore) Take(query *agent.Query) (*agent.Agent, error) {
	return b.store.Take(query)
}

func (b *contextBoundStore) Update(id string, updates map[string]interface{}) error {
	return b.store.updateWithContext(b.ctx, id, updates)
}

func (b *contextBoundStore) Complete(id string, agentID string, leaseToken string, result map[string]interface{}) error {
	_, err := b.store.completeAndGetWithContext(b.ctx, id, agentID, leaseToken, result)
	return err
}

func (b *contextBoundStore) CompleteAndOut(id string, agentID string, leaseToken string, result map[string]interface{}, outputs []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	return b.store.completeAndOutWithContext(b.ctx, id, agentID, leaseToken, result, outputs)
}

func (b *contextBoundStore) Release(id string, agentID string, leaseToken string, reason string) error {
	_, err := b.store.releaseAndGetWithContext(b.ctx, id, agentID, leaseToken, reason)
	return err
}

func (b *contextBoundStore) RenewLease(id string, agentID string, leaseToken string, leaseDuration time.Duration) (*agent.Agent, error) {
	return b.store.renewLeaseWithContext(b.ctx, id, agentID, leaseToken, leaseDuration)
}

func (b *contextBoundStore) Subscribe(filter *agent.EventFilter, handler agent.EventHandler) error {
	return b.store.Subscribe(filter, handler)
}

func (b *contextBoundStore) GetEvents(tupleID string) ([]*agent.Event, error) {
	return b.store.getEventsWithContext(b.ctx, tupleID)
}

func (b *contextBoundStore) GetGlobalEvents(limit, offset int) ([]*agent.Event, error) {
	return b.store.GetGlobalEvents(limit, offset)
}

func (b *contextBoundStore) GetDAG(rootID string) (*agent.DAG, error) {
	return b.store.getDAGWithContext(b.ctx, rootID)
}

func (b *contextBoundStore) QueryEvents(query *agent.EventQuery) (*agent.EventPage, error) {
	return b.store.queryEventsWithContext(b.ctx, query)
}

func (b *contextBoundStore) Count(query *agent.Query) (int, error) {
	return b.store.Count(query)
}

func (b *contextBoundStore) CountByStatus(query *agent.Query) (map[agent.Status]int, error) {
	return b.store.CountByStatus(query)
}

func (b *contextBoundStore) Health(ctx context.Context) error {
	return b.store.Health(defaultContext(ctx))
}
