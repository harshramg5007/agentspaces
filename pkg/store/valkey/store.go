package valkey

import (
	"context"
	"errors"
	"fmt"
	"hash/fnv"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	json "github.com/goccy/go-json"
	"github.com/google/uuid"
	vk "github.com/valkey-io/valkey-go"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/metrics"
	"github.com/urobora-ai/agentspaces/pkg/store/internal/common"
	"github.com/urobora-ai/agentspaces/pkg/store/internal/core"
	"github.com/urobora-ai/agentspaces/pkg/store/shardadmin"
	"github.com/urobora-ai/agentspaces/pkg/telemetry"
)

const (
	queueMetadataKey        = common.QueueMetadataKey
	queueCreatedMsKey       = common.QueueCreatedMsKey
	namespaceMetadataKey    = common.NamespaceMetadataKey
	tokenMismatchStaleErr   = common.TokenMismatchStaleErr
	internalCreatedMsKey    = "__created_ms"
	internalLeaseUntilMsKey = "__lease_until_ms"

	defaultValkeyPollInterval = 100 * time.Millisecond
	valkeyWakeupChannel       = "agent_wakeup"
	globalOrderKeyName        = "order:new"
	globalLeaseKeyName        = "lease:by_until"
	globalAllIndexKeyName     = "index:all"
	globalEventsStreamKey     = "stream:events"
	telemetryCleanupKeyName   = "telemetry:cleanup"
)

type shardSpec struct {
	ShardID       string `json:"shard_id"`
	Addr          string `json:"addr"`
	OwnerNodeID   string `json:"owner_node_id,omitempty"`
	AdvertiseAddr string `json:"advertise_addr,omitempty"`
}

type shardHandle struct {
	spec   shardSpec
	client vk.Client
}

type ValkeyStore struct {
	logger            *zap.Logger
	ctx               context.Context
	cancel            context.CancelFunc
	shards            []*shardHandle
	routingSpecs      []shardSpec
	byShardID         map[string]*shardHandle
	specByShardID     map[string]shardSpec
	ownedShardIDs     map[string]struct{}
	idToShard         sync.Map
	subscriptions     *common.SubscriptionRegistry
	runtime           *core.StoreRuntime
	pollInterval      time.Duration
	leaseDuration     time.Duration
	leaseReapInterval time.Duration
	leaseReapBatch    int
	publishPubSub     bool
	slimEvents        bool
	deferEvents       bool
	eventBatchSize    int
	eventQueue        chan *pendingValkeyEvent
	eventDone         chan struct{}
	ownerRouted       bool
	nodeID            string
	advertiseAddr     string
	reaperID          string
	wakeupMu          sync.RWMutex
	wakeupCh          chan struct{}
	pubsubWG          sync.WaitGroup

	outScript            *vk.Lua
	updateScript         *vk.Lua
	claimScript          *vk.Lua
	queueTakeScript      *vk.Lua
	queueBatchTakeScript *vk.Lua
	completeScript       *vk.Lua
	releaseScript        *vk.Lua
	renewLeaseScript     *vk.Lua
	completeAndOutScript *vk.Lua
	reapScript           *vk.Lua
}

type scriptResponse struct {
	OK      bool     `json:"ok"`
	Code    string   `json:"code,omitempty"`
	Agent   string   `json:"agent,omitempty"`
	Agents  []string `json:"agents,omitempty"`
	Created []string `json:"created,omitempty"`
	Count   int      `json:"count,omitempty"`
}

type contextBoundStore struct {
	store *ValkeyStore
	ctx   context.Context
}

func NewValkeyStore(ctx context.Context, cfg *ValkeyConfig, logger *zap.Logger) (*ValkeyStore, error) {
	if cfg == nil {
		cfg = DefaultValkeyConfig()
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	nodeID := strings.TrimSpace(cfg.NodeID)
	advertiseAddr := strings.TrimSpace(cfg.AdvertiseAddr)
	ownerRouted := strings.TrimSpace(cfg.ShardMapFile) != ""
	specs := make([]shardSpec, 0)
	if ownerRouted {
		shardMap, err := loadShardMap(cfg.ShardMapFile)
		if err != nil {
			return nil, err
		}
		specs = append(specs, shardMap.Shards...)
		if nodeID == "" {
			return nil, fmt.Errorf("valkey owner-routed mode requires node_id")
		}
	} else {
		addrs := collectValkeyAddrs(cfg)
		if len(addrs) == 0 {
			return nil, fmt.Errorf("valkey config missing addr/shards")
		}
		for i, addr := range addrs {
			specs = append(specs, shardSpec{
				ShardID: fmt.Sprintf("valkey-%d", i),
				Addr:    addr,
			})
		}
	}
	reapInterval := time.Second
	if raw := strings.TrimSpace(cfg.LeaseReapInterval); raw != "" {
		if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
			reapInterval = parsed
		}
	}
	reapBatch := cfg.LeaseReapBatch
	if reapBatch <= 0 {
		reapBatch = DefaultValkeyConfig().LeaseReapBatch
	}

	storeCtx, cancel := context.WithCancel(defaultContext(ctx))
	store := &ValkeyStore{
		logger:               logger,
		ctx:                  storeCtx,
		cancel:               cancel,
		routingSpecs:         append([]shardSpec(nil), specs...),
		byShardID:            make(map[string]*shardHandle, len(specs)),
		specByShardID:        make(map[string]shardSpec, len(specs)),
		ownedShardIDs:        make(map[string]struct{}, len(specs)),
		subscriptions:        common.NewSubscriptionRegistry(),
		pollInterval:         defaultValkeyPollInterval,
		leaseDuration:        agent.DefaultLeaseOptions().LeaseDuration,
		leaseReapInterval:    reapInterval,
		leaseReapBatch:       reapBatch,
		publishPubSub:        cfg.PublishPubSub,
		slimEvents:           cfg.SlimEvents,
		deferEvents:          cfg.DeferEvents,
		eventBatchSize:       cfg.EventBatchSize,
		ownerRouted:          ownerRouted,
		nodeID:               nodeID,
		advertiseAddr:        advertiseAddr,
		reaperID:             nodeID,
		wakeupCh:             make(chan struct{}),
		outScript:            vk.NewLuaScript(outScriptSource),
		updateScript:         vk.NewLuaScript(updateScriptSource),
		claimScript:          vk.NewLuaScript(claimScriptSource),
		queueTakeScript:      vk.NewLuaScript(queueTakeScriptSource),
		queueBatchTakeScript: vk.NewLuaScript(queueBatchTakeScriptSource),
		completeScript:       vk.NewLuaScript(completeScriptSource),
		releaseScript:        vk.NewLuaScript(releaseScriptSource),
		renewLeaseScript:     vk.NewLuaScript(renewLeaseScriptSource),
		completeAndOutScript: vk.NewLuaScript(completeAndOutScriptSource),
		reapScript:           vk.NewLuaScript(reapScriptSource),
	}
	if store.reaperID == "" {
		store.reaperID = uuid.NewString()
	}
	if store.eventBatchSize <= 0 {
		store.eventBatchSize = 100
	}
	if store.deferEvents {
		store.eventQueue = make(chan *pendingValkeyEvent, max(256, store.eventBatchSize*4))
		store.eventDone = make(chan struct{})
	}

	for _, spec := range specs {
		store.specByShardID[spec.ShardID] = spec
		if store.ownsShard(spec) {
			store.ownedShardIDs[spec.ShardID] = struct{}{}
		}
		client, err := vk.NewClient(vk.ClientOption{
			InitAddress: []string{spec.Addr},
		})
		if err != nil {
			cancel()
			store.closeClients()
			return nil, fmt.Errorf("create valkey client for %s: %w", spec.Addr, err)
		}
		shard := &shardHandle{
			spec:   spec,
			client: client,
		}
		store.shards = append(store.shards, shard)
		store.byShardID[shard.spec.ShardID] = shard
	}
	if ownerRouted && len(store.ownedShardIDs) == 0 {
		cancel()
		store.closeClients()
		return nil, fmt.Errorf("valkey shard map %q has no shards owned by node %q", cfg.ShardMapFile, nodeID)
	}
	store.runtime = core.NewStoreRuntime(store)

	if store.deferEvents {
		go store.eventWriterLoop(store.eventBatchSize)
	}
	go store.leaseReaperLoop()
	if store.publishPubSub {
		store.startWakeupListeners()
	}
	return store, nil
}

func (s *ValkeyStore) Close() error {
	s.cancel()
	if s.eventDone != nil {
		<-s.eventDone
	}
	s.closeClients()
	s.pubsubWG.Wait()
	return nil
}

func (s *ValkeyStore) closeClients() {
	for _, shard := range s.shards {
		if shard != nil && shard.client != nil {
			shard.client.Close()
		}
	}
}

func defaultContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func collectValkeyAddrs(cfg *ValkeyConfig) []string {
	if cfg == nil {
		return nil
	}
	if len(cfg.Shards) > 0 {
		out := make([]string, 0, len(cfg.Shards))
		for _, shard := range cfg.Shards {
			shard = strings.TrimSpace(shard)
			if shard != "" {
				out = append(out, shard)
			}
		}
		return out
	}
	addr := strings.TrimSpace(cfg.Addr)
	if addr == "" {
		return nil
	}
	return []string{addr}
}

func (s *ValkeyStore) isSharded() bool {
	return len(s.routingSpecs) > 1
}

func hashKey(namespaceID string, queue string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(namespaceID))
	_, _ = h.Write([]byte(":"))
	_, _ = h.Write([]byte(queue))
	return h.Sum32()
}

func (s *ValkeyStore) ownsShard(spec shardSpec) bool {
	if s == nil {
		return true
	}
	if strings.TrimSpace(s.nodeID) == "" {
		return true
	}
	ownerNodeID := strings.TrimSpace(spec.OwnerNodeID)
	if ownerNodeID == "" {
		return true
	}
	return ownerNodeID == strings.TrimSpace(s.nodeID)
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

func (s *ValkeyStore) remoteShardOwner(spec shardSpec) error {
	return &remoteShardOwnerError{
		shardID:       spec.ShardID,
		ownerNodeID:   spec.OwnerNodeID,
		advertiseAddr: spec.AdvertiseAddr,
	}
}

func (s *ValkeyStore) targetShardSpec(namespaceID string, queue string) (shardSpec, error) {
	namespaceID = strings.TrimSpace(namespaceID)
	queue = strings.TrimSpace(queue)
	if namespaceID == "" {
		return shardSpec{}, fmt.Errorf("namespace_id is required for valkey hot-path routing")
	}
	if queue == "" {
		return shardSpec{}, fmt.Errorf("queue metadata is required for valkey hot-path routing")
	}
	if len(s.routingSpecs) == 0 {
		return shardSpec{}, fmt.Errorf("valkey store has no shards")
	}
	idx := int(hashKey(namespaceID, queue) % uint32(len(s.routingSpecs)))
	return s.routingSpecs[idx], nil
}

func (s *ValkeyStore) targetShard(namespaceID string, queue string) (*shardHandle, error) {
	spec, err := s.targetShardSpec(namespaceID, queue)
	if err != nil {
		return nil, err
	}
	if s.ownerRouted && !s.ownsShard(spec) {
		return nil, s.remoteShardOwner(spec)
	}
	shard, ok := s.byShardID[spec.ShardID]
	if !ok {
		return nil, fmt.Errorf("valkey shard %q is not configured locally", spec.ShardID)
	}
	return shard, nil
}

func (s *ValkeyStore) shardFromHint(ctx context.Context) (*shardHandle, bool, error) {
	shardID := strings.TrimSpace(agent.ShardIDFromContext(ctx))
	if shardID == "" {
		return nil, false, nil
	}
	if shard, ok := s.byShardID[shardID]; ok {
		if s.ownerRouted {
			if spec, specOK := s.specByShardID[shardID]; specOK && !s.ownsShard(spec) {
				return nil, true, s.remoteShardOwner(spec)
			}
		}
		return shard, true, nil
	}
	if spec, ok := s.specByShardID[shardID]; ok {
		if s.ownerRouted {
			return nil, true, s.remoteShardOwner(spec)
		}
	}
	return nil, true, fmt.Errorf("unknown shard_id %q", shardID)
}

func (s *ValkeyStore) shardFromAgent(t *agent.Agent) (*shardHandle, error) {
	if !s.isSharded() {
		return s.shards[0], nil
	}
	namespaceID := ""
	queue := ""
	if t != nil {
		namespaceID = strings.TrimSpace(t.NamespaceID)
		if t.Metadata != nil {
			if namespaceID == "" {
				namespaceID = strings.TrimSpace(t.Metadata[namespaceMetadataKey])
			}
			queue = strings.TrimSpace(t.Metadata[queueMetadataKey])
		}
	}
	return s.targetShard(namespaceID, queue)
}

func (s *ValkeyStore) shardFromQuery(query *agent.Query) (*shardHandle, bool, error) {
	if !s.isSharded() {
		return s.shards[0], true, nil
	}
	if query == nil {
		return nil, false, nil
	}
	queue := ""
	if query.Metadata != nil {
		queue = strings.TrimSpace(query.Metadata[queueMetadataKey])
	}
	if strings.TrimSpace(query.NamespaceID) == "" || queue == "" {
		return nil, false, nil
	}
	shard, err := s.targetShard(query.NamespaceID, queue)
	return shard, true, err
}

func (s *ValkeyStore) shardForMutation(ctx context.Context, query *agent.Query) (*shardHandle, bool, error) {
	if shard, hinted, err := s.shardFromHint(ctx); hinted || err != nil {
		return shard, hinted, err
	}
	return s.shardFromQuery(query)
}

func (s *ValkeyStore) orderedShardIDs() []string {
	out := make([]string, 0, len(s.shards))
	for _, shard := range s.shards {
		out = append(out, shard.spec.ShardID)
	}
	sort.Strings(out)
	return out
}

func (s *ValkeyStore) shardByID(ctx context.Context, id string) (*shardHandle, error) {
	if cached, ok := s.idToShard.Load(id); ok {
		if shardID, ok := cached.(string); ok {
			if shard, exists := s.byShardID[shardID]; exists {
				if current, err := s.getFromShard(ctx, shard, id); err == nil && current != nil {
					return shard, nil
				}
			}
		}
	}
	for _, shardID := range s.orderedShardIDs() {
		shard := s.byShardID[shardID]
		current, err := s.getFromShard(ctx, shard, id)
		if err != nil || current == nil {
			continue
		}
		return shard, nil
	}
	return nil, fmt.Errorf("agent not found: %s", id)
}

func (s *ValkeyStore) BindContext(ctx context.Context) agent.AgentSpace {
	return &contextBoundStore{store: s, ctx: defaultContext(ctx)}
}

func (s *ValkeyStore) Health(ctx context.Context) error {
	ctx = defaultContext(ctx)
	for _, shard := range s.shards {
		if err := shard.client.Do(ctx, shard.client.B().Ping().Build()).Error(); err != nil {
			return fmt.Errorf("shard %s unhealthy: %w", shard.spec.ShardID, err)
		}
	}
	return nil
}

func (s *ValkeyStore) Get(id string) (*agent.Agent, error) {
	return s.getWithContext(context.Background(), id)
}

func (s *ValkeyStore) getWithContext(ctx context.Context, id string) (*agent.Agent, error) {
	shard, err := s.shardByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return s.getFromShard(ctx, shard, id)
}

func (s *ValkeyStore) getFromShard(ctx context.Context, shard *shardHandle, id string) (*agent.Agent, error) {
	if shard == nil {
		return nil, fmt.Errorf("agent not found: %s", id)
	}
	raw, err := shard.client.Do(ctx, shard.client.B().Get().Key(agentKey(id)).Build()).ToString()
	if err != nil {
		if isValkeyNil(err) {
			return nil, fmt.Errorf("agent not found: %s", id)
		}
		return nil, err
	}
	t, err := decodeAgent(raw)
	if err != nil {
		return nil, err
	}
	if isExpired(t, time.Now().UTC()) {
		return nil, fmt.Errorf("agent not found: %s", id)
	}
	t.ShardID = shard.spec.ShardID
	s.idToShard.Store(t.ID, shard.spec.ShardID)
	return t, nil
}

func (s *ValkeyStore) Out(t *agent.Agent) error {
	start := time.Now()
	success := false
	defer func() {
		metrics.RecordOperation("out", s.storeLabel(), success, time.Since(start).Seconds())
	}()

	shard, err := s.shardFromAgent(t)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	normalizeAgentForOut(t, now, shard.spec.ShardID)
	agentJSON, err := json.Marshal(t)
	if err != nil {
		return err
	}
	eventData, err := json.Marshal(createdEventData(t, s.slimEvents))
	if err != nil {
		return err
	}
	resp, err := execScriptJSON(s.outScript, s.ctx, shard.client, []string{agentKey(t.ID)}, []string{
		string(agentJSON),
		t.ID,
		now.Format(time.RFC3339Nano),
		t.Metadata["agent_id"],
		string(eventData),
		boolLuaArg(!s.deferEvents),
	})
	if err != nil {
		return err
	}
	switch resp.Code {
	case "", "CREATED":
	default:
		return errors.New(resp.Code)
	}
	s.idToShard.Store(t.ID, shard.spec.ShardID)
	createdEvent := &agent.Event{
		ID:        uuid.New().String(),
		TupleID:   t.ID,
		Type:      agent.EventCreated,
		Kind:      t.Kind,
		Timestamp: now,
		AgentID:   t.Metadata["agent_id"],
		TraceID:   t.TraceID,
		Version:   t.Version,
		Data:      createdEventData(t, s.slimEvents),
	}
	s.notifySubscribers(createdEvent)
	if s.deferEvents {
		s.enqueueEvent(createdEvent, t.NamespaceID, shard.spec.ShardID)
	}
	s.signalWakeups()
	s.publishWakeup(shard, t.NamespaceID, t.Metadata[queueMetadataKey])
	success = true
	return nil
}

func (s *ValkeyStore) Take(query *agent.Query) (*agent.Agent, error) {
	start := time.Now()
	success := false
	defer func() {
		metrics.RecordOperation("take", s.storeLabel(), success, time.Since(start).Seconds())
	}()
	t, err := s.take(defaultContext(context.Background()), query)
	if err != nil {
		return nil, err
	}
	success = t != nil
	return t, nil
}

func (s *ValkeyStore) take(ctx context.Context, query *agent.Query) (*agent.Agent, error) {
	ctx = defaultContext(ctx)
	if query != nil && query.Status != "" && query.Status != agent.StatusNew {
		return nil, nil
	}
	shard, ok, err := s.shardForMutation(ctx, query)
	if err != nil {
		return nil, err
	}
	if s.isSharded() && !ok {
		return nil, fmt.Errorf("valkey take requires namespace_id and metadata.queue")
	}
	if shard == nil {
		shard = s.shards[0]
	}

	agentID := extractAgentID(query)
	if agentID == "" {
		agentID = "agent"
	}
	if canUseQueueTakeScript(query) {
		return s.takeFromQueueHotPath(ctx, shard, query, agentID)
	}

	for attempt := 0; attempt < 64; attempt++ {
		candidate, err := s.nextTakeCandidate(ctx, shard, query)
		if err != nil || candidate == nil {
			return candidate, err
		}
		if !matchesQueryFilters(candidate, query) {
			if query != nil && query.Metadata != nil && strings.TrimSpace(query.Metadata[queueMetadataKey]) != "" {
				return nil, nil
			}
			continue
		}
		leaseDuration := s.leaseDurationForAgent(candidate)
		now := time.Now().UTC()
		leaseUntil := now.Add(leaseDuration)
		leaseToken := uuid.New().String()
		resp, err := execScriptJSON(s.claimScript, ctx, shard.client, []string{agentKey(candidate.ID)}, []string{
			candidate.ID,
			agentID,
			leaseToken,
			now.Format(time.RFC3339Nano),
			strconv.FormatInt(leaseUntil.UnixMilli(), 10),
			leaseUntil.Format(time.RFC3339Nano),
			boolLuaArg(!s.deferEvents),
		})
		if err != nil {
			return nil, err
		}
		switch resp.Code {
		case "", "CLAIMED":
			claimed, err := decodeAgent(resp.Agent)
			if err != nil {
				return nil, err
			}
			claimed.ShardID = shard.spec.ShardID
			s.idToShard.Store(claimed.ID, shard.spec.ShardID)
			claimedEvent := &agent.Event{
				ID:        uuid.New().String(),
				TupleID:   claimed.ID,
				Type:      agent.EventClaimed,
				Kind:      claimed.Kind,
				Timestamp: now,
				AgentID:   agentID,
				TraceID:   claimed.TraceID,
				Version:   claimed.Version,
				Data:      map[string]interface{}{"agent_id": agentID},
			}
			s.notifySubscribers(claimedEvent)
			if s.deferEvents {
				s.enqueueEvent(claimedEvent, claimed.NamespaceID, shard.spec.ShardID)
			}
			return claimed, nil
		case "NOT_QUEUE_HEAD":
			return nil, nil
		case "STATUS_CHANGED", "NOT_FOUND":
			continue
		default:
			return nil, errors.New(resp.Code)
		}
	}
	return nil, nil
}

func (s *ValkeyStore) nextTakeCandidate(ctx context.Context, shard *shardHandle, query *agent.Query) (*agent.Agent, error) {
	if shard == nil {
		return nil, nil
	}
	queue := ""
	if query != nil && query.Metadata != nil {
		queue = strings.TrimSpace(query.Metadata[queueMetadataKey])
	}
	if queue != "" {
		namespaceID := ""
		if query != nil {
			namespaceID = query.NamespaceID
		}
		ids, err := shard.client.Do(ctx, shard.client.B().Zrange().Key(queueIndexKey(namespaceID, queue)).Min("0").Max("0").Build()).AsStrSlice()
		if err != nil {
			if isValkeyNil(err) {
				return nil, nil
			}
			return nil, err
		}
		if len(ids) == 0 {
			return nil, nil
		}
		return s.getFromShard(ctx, shard, ids[0])
	}
	ids, err := shard.client.Do(ctx, shard.client.B().Zrange().Key(globalOrderKey()).Min("0").Max("127").Build()).AsStrSlice()
	if err != nil {
		if isValkeyNil(err) {
			return nil, nil
		}
		return nil, err
	}
	hotStates, err := s.getManyHotStatesFromShard(ctx, shard, ids)
	if err != nil {
		return nil, err
	}
	for _, id := range ids {
		state := hotStates[id]
		if state == nil || !hotStateMatchesQuery(state, query) {
			continue
		}
		t, err := s.getFromShard(ctx, shard, id)
		if err != nil {
			continue
		}
		if matchesQueryFilters(t, query) {
			return t, nil
		}
	}
	return nil, nil
}

func (s *ValkeyStore) BatchTake(query *agent.Query, limit int) ([]*agent.Agent, error) {
	if limit <= 0 {
		return nil, nil
	}
	if canUseQueueTakeScript(query) {
		shard, ok, err := s.shardForMutation(context.Background(), query)
		if err != nil {
			return nil, err
		}
		if s.isSharded() && !ok {
			return nil, fmt.Errorf("valkey take requires namespace_id and metadata.queue")
		}
		if shard == nil {
			shard = s.shards[0]
		}
		agentID := extractAgentID(query)
		if agentID == "" {
			agentID = "agent"
		}
		return s.batchTakeFromQueueHotPath(defaultContext(context.Background()), shard, query, agentID, limit)
	}
	out := make([]*agent.Agent, 0, limit)
	for i := 0; i < limit; i++ {
		t, err := s.Take(query)
		if err != nil {
			return nil, err
		}
		if t == nil {
			break
		}
		out = append(out, t)
	}
	return out, nil
}

func (s *ValkeyStore) In(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	ctx, cancel := context.WithCancel(defaultContext(context.Background()))
	defer cancel()
	return s.in(ctx, query, timeout)
}

func (s *ValkeyStore) in(ctx context.Context, query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	deadline := time.Now().Add(timeout)
	for {
		t, err := s.take(ctx, query)
		if err != nil || t != nil {
			return t, err
		}
		if timeout <= 0 || time.Now().After(deadline) {
			return nil, nil
		}
		if err := s.waitForWakeup(ctx, deadline); err != nil {
			return nil, err
		}
	}
}

func (s *ValkeyStore) Read(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	ctx, cancel := context.WithCancel(defaultContext(context.Background()))
	defer cancel()
	deadline := time.Now().Add(timeout)
	for {
		values, err := s.query(ctx, query)
		if err != nil {
			return nil, err
		}
		if len(values) > 0 {
			return values[0], nil
		}
		if timeout <= 0 || time.Now().After(deadline) {
			return nil, nil
		}
		if err := s.waitForWakeup(ctx, deadline); err != nil {
			return nil, err
		}
	}
}

func (s *ValkeyStore) Query(query *agent.Query) ([]*agent.Agent, error) {
	return s.query(context.Background(), query)
}

func (s *ValkeyStore) query(ctx context.Context, query *agent.Query) ([]*agent.Agent, error) {
	ctx = defaultContext(ctx)
	if shard, ok, err := s.shardFromQuery(query); err != nil {
		return nil, err
	} else if ok {
		values, err := s.queryShard(ctx, shard, query)
		if err != nil {
			return nil, err
		}
		return applyAgentLimitOffset(values, query), nil
	}

	all := make([]*agent.Agent, 0)
	for _, shard := range s.shards {
		values, err := s.queryShard(ctx, shard, cloneQueryWithoutPagination(query))
		if err != nil {
			return nil, err
		}
		all = append(all, values...)
	}
	sortAgents(all)
	return applyAgentLimitOffset(all, query), nil
}

func (s *ValkeyStore) queryShard(ctx context.Context, shard *shardHandle, query *agent.Query) ([]*agent.Agent, error) {
	candidateIDs, err := s.candidateIDsForQuery(ctx, shard, query)
	if err != nil {
		return nil, err
	}
	if len(candidateIDs) == 0 {
		return []*agent.Agent{}, nil
	}
	values, err := s.getManyFromShard(ctx, shard, candidateIDs)
	if err != nil {
		return nil, err
	}
	filtered := make([]*agent.Agent, 0, len(values))
	for _, t := range values {
		if matchesQueryFilters(t, query) {
			t.ShardID = shard.spec.ShardID
			s.idToShard.Store(t.ID, shard.spec.ShardID)
			filtered = append(filtered, t)
		}
	}
	sortAgents(filtered)
	return filtered, nil
}

func (s *ValkeyStore) candidateIDsForQuery(ctx context.Context, shard *shardHandle, query *agent.Query) ([]string, error) {
	if shard == nil {
		return nil, nil
	}
	type candidateSet struct {
		ids []string
	}
	sources := make([]candidateSet, 0, 8)
	addSource := func(ids []string) {
		if len(ids) == 0 {
			sources = append(sources, candidateSet{ids: []string{}})
			return
		}
		sources = append(sources, candidateSet{ids: ids})
	}

	if query != nil {
		if query.Status != "" {
			ids, err := shard.client.Do(ctx, shard.client.B().Smembers().Key(statusIndexKey(query.Status)).Build()).AsStrSlice()
			if err != nil && !isValkeyNil(err) {
				return nil, err
			}
			addSource(ids)
		}
		if query.Kind != "" {
			ids, err := shard.client.Do(ctx, shard.client.B().Smembers().Key(kindIndexKey(query.Kind)).Build()).AsStrSlice()
			if err != nil && !isValkeyNil(err) {
				return nil, err
			}
			addSource(ids)
		}
		if query.Owner != "" {
			ids, err := shard.client.Do(ctx, shard.client.B().Smembers().Key(ownerIndexKey(query.Owner)).Build()).AsStrSlice()
			if err != nil && !isValkeyNil(err) {
				return nil, err
			}
			addSource(ids)
		}
		if query.ParentID != "" {
			ids, err := shard.client.Do(ctx, shard.client.B().Smembers().Key(parentIndexKey(query.ParentID)).Build()).AsStrSlice()
			if err != nil && !isValkeyNil(err) {
				return nil, err
			}
			addSource(ids)
		}
		if query.TraceID != "" {
			ids, err := shard.client.Do(ctx, shard.client.B().Smembers().Key(traceIndexKey(query.TraceID)).Build()).AsStrSlice()
			if err != nil && !isValkeyNil(err) {
				return nil, err
			}
			addSource(ids)
		}
		if query.NamespaceID != "" {
			ids, err := shard.client.Do(ctx, shard.client.B().Smembers().Key(namespaceIndexKey(query.NamespaceID)).Build()).AsStrSlice()
			if err != nil && !isValkeyNil(err) {
				return nil, err
			}
			addSource(ids)
		}
		for _, tag := range query.Tags {
			ids, err := shard.client.Do(ctx, shard.client.B().Smembers().Key(tagIndexKey(tag)).Build()).AsStrSlice()
			if err != nil && !isValkeyNil(err) {
				return nil, err
			}
			addSource(ids)
		}
		if query.Metadata != nil {
			for k, v := range query.Metadata {
				if k == "agent_id" {
					continue
				}
				ids, err := shard.client.Do(ctx, shard.client.B().Smembers().Key(metadataIndexKey(k, v)).Build()).AsStrSlice()
				if err != nil && !isValkeyNil(err) {
					return nil, err
				}
				addSource(ids)
			}
		}
	}
	if len(sources) == 0 {
		ids, err := shard.client.Do(ctx, shard.client.B().Smembers().Key(allIndexKey()).Build()).AsStrSlice()
		if err != nil && !isValkeyNil(err) {
			return nil, err
		}
		return ids, nil
	}
	sort.SliceStable(sources, func(i, j int) bool {
		return len(sources[i].ids) < len(sources[j].ids)
	})
	base := sources[0].ids
	if len(base) == 0 {
		return []string{}, nil
	}
	if len(sources) == 1 {
		return base, nil
	}
	allowed := make(map[string]int, len(base))
	for _, id := range base {
		allowed[id] = 1
	}
	for _, source := range sources[1:] {
		next := make(map[string]int, len(source.ids))
		for _, id := range source.ids {
			if allowed[id] > 0 {
				next[id] = 1
			}
		}
		allowed = next
		if len(allowed) == 0 {
			return []string{}, nil
		}
	}
	out := make([]string, 0, len(allowed))
	for id := range allowed {
		out = append(out, id)
	}
	return out, nil
}

func (s *ValkeyStore) getManyFromShard(ctx context.Context, shard *shardHandle, ids []string) ([]*agent.Agent, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, agentKey(id))
	}
	values, err := shard.client.Do(ctx, shard.client.B().Mget().Key(keys...).Build()).ToArray()
	if err != nil {
		return nil, err
	}
	out := make([]*agent.Agent, 0, len(values))
	now := time.Now().UTC()
	for i, value := range values {
		raw, err := value.ToString()
		if err != nil || raw == "" {
			continue
		}
		t, err := decodeAgent(raw)
		if err != nil || isExpired(t, now) {
			continue
		}
		t.ShardID = shard.spec.ShardID
		s.idToShard.Store(ids[i], shard.spec.ShardID)
		out = append(out, t)
	}
	return out, nil
}

func (s *ValkeyStore) Count(query *agent.Query) (int, error) {
	values, err := s.query(context.Background(), cloneQueryWithoutPagination(query))
	if err != nil {
		return 0, err
	}
	return len(values), nil
}

func (s *ValkeyStore) CountByStatus(query *agent.Query) (map[agent.Status]int, error) {
	base := cloneQueryWithoutPagination(query)
	if base != nil {
		base.Status = ""
	}
	values, err := s.query(context.Background(), base)
	if err != nil {
		return nil, err
	}
	counts := make(map[agent.Status]int)
	for _, t := range values {
		counts[t.Status]++
	}
	return counts, nil
}

func (s *ValkeyStore) Update(id string, updates map[string]interface{}) error {
	_, err := s.UpdateAndGet(id, updates)
	return err
}

func (s *ValkeyStore) UpdateAndGet(id string, updates map[string]interface{}) (*agent.Agent, error) {
	shard, err := s.shardByID(context.Background(), id)
	if err != nil {
		return nil, err
	}
	current, err := s.getFromShard(context.Background(), shard, id)
	if err != nil {
		return nil, err
	}
	if s.isSharded() {
		if metadataRaw, ok := updates["metadata"]; ok && metadataRaw != nil {
			if metadata, ok := metadataRaw.(map[string]string); ok {
				if queue := strings.TrimSpace(metadata[queueMetadataKey]); queue != "" && queue != current.Metadata[queueMetadataKey] {
					return nil, fmt.Errorf("valkey update cannot move agents across queues/shards")
				}
			}
		}
	}
	next := cloneAgent(current)
	common.ApplyUpdates(next, updates)
	next.UpdatedAt = time.Now().UTC()
	next.Version++
	normalizeAgentMetadata(next, shard.spec.ShardID)
	nextJSON, err := json.Marshal(next)
	if err != nil {
		return nil, err
	}
	eventData, err := json.Marshal(updates)
	if err != nil {
		return nil, err
	}
	resp, err := execScriptJSON(s.updateScript, s.ctx, shard.client, []string{agentKey(id)}, []string{
		string(nextJSON),
		next.ID,
		next.UpdatedAt.Format(time.RFC3339Nano),
		current.Metadata["agent_id"],
		string(eventData),
		boolLuaArg(!s.deferEvents),
	})
	if err != nil {
		return nil, err
	}
	switch resp.Code {
	case "", "UPDATED":
	default:
		return nil, errors.New(resp.Code)
	}
	updated, err := decodeAgent(resp.Agent)
	if err != nil {
		return nil, err
	}
	updated.ShardID = shard.spec.ShardID
	s.idToShard.Store(updated.ID, shard.spec.ShardID)
	updatedEvent := &agent.Event{
		ID:        uuid.New().String(),
		TupleID:   updated.ID,
		Type:      agent.EventUpdated,
		Kind:      updated.Kind,
		Timestamp: updated.UpdatedAt,
		AgentID:   current.Metadata["agent_id"],
		TraceID:   updated.TraceID,
		Version:   updated.Version,
		Data:      updates,
	}
	s.notifySubscribers(updatedEvent)
	if s.deferEvents {
		s.enqueueEvent(updatedEvent, updated.NamespaceID, shard.spec.ShardID)
	}
	s.signalWakeups()
	s.publishWakeup(shard, updated.NamespaceID, updated.Metadata[queueMetadataKey])
	return updated, nil
}

func (s *ValkeyStore) Complete(id string, agentID string, leaseToken string, result map[string]interface{}) error {
	_, err := s.CompleteAndGet(id, agentID, leaseToken, result)
	return err
}

func (s *ValkeyStore) CompleteAndGet(id string, agentID string, leaseToken string, result map[string]interface{}) (*agent.Agent, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, fmt.Errorf("lease token is required")
	}
	shard, err := s.shardByID(context.Background(), id)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	if result == nil {
		result = map[string]interface{}{}
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	resp, err := execScriptJSON(s.completeScript, s.ctx, shard.client, []string{agentKey(id)}, []string{
		id,
		agentID,
		leaseToken,
		string(resultJSON),
		now.Format(time.RFC3339Nano),
		boolLuaArg(!s.deferEvents),
	})
	if err != nil {
		return nil, err
	}
	switch resp.Code {
	case "", "COMPLETED", "ALREADY_COMPLETED":
	default:
		return nil, classifyValkeyCompleteCode(resp.Code, id)
	}
	t, err := decodeAgent(resp.Agent)
	if err != nil {
		return nil, err
	}
	t.ShardID = shard.spec.ShardID
	s.idToShard.Store(t.ID, shard.spec.ShardID)
	if resp.Code != "ALREADY_COMPLETED" {
		completedEvent := &agent.Event{
			ID:        uuid.New().String(),
			TupleID:   t.ID,
			Type:      agent.EventCompleted,
			Kind:      t.Kind,
			Timestamp: now,
			AgentID:   agentID,
			TraceID:   t.TraceID,
			Version:   t.Version,
			Data:      result,
		}
		s.notifySubscribers(completedEvent)
		if s.deferEvents {
			s.enqueueEvent(completedEvent, t.NamespaceID, shard.spec.ShardID)
		}
		s.signalWakeups()
		s.publishWakeup(shard, t.NamespaceID, t.Metadata[queueMetadataKey])
	}
	return t, nil
}

func (s *ValkeyStore) CompleteAndOut(id string, agentID string, leaseToken string, result map[string]interface{}, outputs []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	if agentID == "" {
		return nil, nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, nil, fmt.Errorf("lease token is required")
	}
	source, err := s.getWithContext(context.Background(), id)
	if err != nil {
		return nil, nil, err
	}
	shard, ok := s.byShardID[source.ShardID]
	if !ok {
		return nil, nil, fmt.Errorf("source shard %q not configured", source.ShardID)
	}
	now := time.Now().UTC()
	if result == nil {
		result = map[string]interface{}{}
	}
	for _, out := range outputs {
		normalizeOutputForParent(out, source, now, shard.spec.ShardID)
		targetShard, err := s.shardFromAgent(out)
		if err != nil {
			return nil, nil, err
		}
		if targetShard.spec.ShardID != shard.spec.ShardID {
			return nil, nil, fmt.Errorf("cross_shard_atomic_unsupported: source=%s output=%s", shard.spec.ShardID, targetShard.spec.ShardID)
		}
	}
	resultJSON, err := json.Marshal(result)
	if err != nil {
		return nil, nil, err
	}
	outputsJSON, err := json.Marshal(outputs)
	if err != nil {
		return nil, nil, err
	}
	resp, err := execScriptJSON(s.completeAndOutScript, s.ctx, shard.client, []string{agentKey(id)}, []string{
		id,
		agentID,
		leaseToken,
		string(resultJSON),
		string(outputsJSON),
		now.Format(time.RFC3339Nano),
		boolLuaArg(!s.deferEvents),
		boolLuaArg(s.slimEvents),
	})
	if err != nil {
		return nil, nil, err
	}
	switch resp.Code {
	case "", "COMPLETED_AND_OUT":
	default:
		return nil, nil, classifyValkeyCompleteCode(resp.Code, id)
	}
	completed, err := decodeAgent(resp.Agent)
	if err != nil {
		return nil, nil, err
	}
	completed.ShardID = shard.spec.ShardID
	s.idToShard.Store(completed.ID, shard.spec.ShardID)
	created := make([]*agent.Agent, 0, len(resp.Created))
	for _, raw := range resp.Created {
		out, err := decodeAgent(raw)
		if err != nil {
			return nil, nil, err
		}
		out.ShardID = shard.spec.ShardID
		s.idToShard.Store(out.ID, shard.spec.ShardID)
		created = append(created, out)
	}
	completedEvent := &agent.Event{
		ID:        uuid.New().String(),
		TupleID:   completed.ID,
		Type:      agent.EventCompleted,
		Kind:      completed.Kind,
		Timestamp: now,
		AgentID:   agentID,
		TraceID:   completed.TraceID,
		Version:   completed.Version,
		Data:      result,
	}
	s.notifySubscribers(completedEvent)
	if s.deferEvents {
		s.enqueueEvent(completedEvent, completed.NamespaceID, shard.spec.ShardID)
	}
	for _, out := range created {
		eventData := createdEventData(out, s.slimEvents)
		eventData["parent_id"] = completed.ID
		createdEvent := &agent.Event{
			ID:        uuid.New().String(),
			TupleID:   out.ID,
			Type:      agent.EventCreated,
			Kind:      out.Kind,
			Timestamp: now,
			AgentID:   agentID,
			TraceID:   out.TraceID,
			Version:   out.Version,
			Data:      eventData,
		}
		s.notifySubscribers(createdEvent)
		if s.deferEvents {
			s.enqueueEvent(createdEvent, out.NamespaceID, shard.spec.ShardID)
		}
		s.publishWakeup(shard, out.NamespaceID, out.Metadata[queueMetadataKey])
	}
	s.signalWakeups()
	s.publishWakeup(shard, completed.NamespaceID, completed.Metadata[queueMetadataKey])
	return completed, created, nil
}

func (s *ValkeyStore) Release(id string, agentID string, leaseToken string, reason string) error {
	_, err := s.ReleaseAndGet(id, agentID, leaseToken, reason)
	return err
}

func (s *ValkeyStore) ReleaseAndGet(id string, agentID string, leaseToken string, reason string) (*agent.Agent, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, fmt.Errorf("lease token is required")
	}
	shard, err := s.shardByID(context.Background(), id)
	if err != nil {
		return nil, err
	}
	now := time.Now().UTC()
	resp, err := execScriptJSON(s.releaseScript, s.ctx, shard.client, []string{agentKey(id)}, []string{
		id,
		agentID,
		leaseToken,
		reason,
		now.Format(time.RFC3339Nano),
		boolLuaArg(!s.deferEvents),
	})
	if err != nil {
		return nil, err
	}
	switch resp.Code {
	case "", "RELEASED":
	default:
		return nil, classifyValkeyReleaseCode(resp.Code, id)
	}
	t, err := decodeAgent(resp.Agent)
	if err != nil {
		return nil, err
	}
	t.ShardID = shard.spec.ShardID
	s.idToShard.Store(t.ID, shard.spec.ShardID)
	eventData := map[string]interface{}{"previous_owner": agentID}
	if reason != "" {
		eventData["reap_reason"] = reason
	}
	releasedEvent := &agent.Event{
		ID:        uuid.New().String(),
		TupleID:   t.ID,
		Type:      agent.EventReleased,
		Kind:      t.Kind,
		Timestamp: now,
		AgentID:   agentID,
		TraceID:   t.TraceID,
		Version:   t.Version,
		Data:      eventData,
	}
	s.notifySubscribers(releasedEvent)
	if s.deferEvents {
		s.enqueueEvent(releasedEvent, t.NamespaceID, shard.spec.ShardID)
	}
	s.signalWakeups()
	s.publishWakeup(shard, t.NamespaceID, t.Metadata[queueMetadataKey])
	return t, nil
}

func (s *ValkeyStore) RenewLease(id string, agentID string, leaseToken string, leaseDuration time.Duration) (*agent.Agent, error) {
	if agentID == "" {
		return nil, fmt.Errorf("agent ID is required")
	}
	if leaseToken == "" {
		return nil, fmt.Errorf("lease token is required")
	}
	shard, err := s.shardByID(context.Background(), id)
	if err != nil {
		return nil, err
	}
	current, err := s.getFromShard(context.Background(), shard, id)
	if err != nil {
		return nil, err
	}
	if leaseDuration <= 0 {
		leaseDuration = s.leaseDurationForAgent(current)
	}
	now := time.Now().UTC()
	leaseUntil := now.Add(leaseDuration)
	resp, err := execScriptJSON(s.renewLeaseScript, s.ctx, shard.client, []string{agentKey(id)}, []string{
		id,
		agentID,
		leaseToken,
		now.Format(time.RFC3339Nano),
		strconv.FormatInt(leaseUntil.UnixMilli(), 10),
		leaseUntil.Format(time.RFC3339Nano),
		boolLuaArg(!s.deferEvents),
	})
	if err != nil {
		return nil, err
	}
	switch resp.Code {
	case "", "RENEWED":
	default:
		return nil, classifyValkeyRenewCode(resp.Code, id)
	}
	t, err := decodeAgent(resp.Agent)
	if err != nil {
		return nil, err
	}
	t.ShardID = shard.spec.ShardID
	s.idToShard.Store(t.ID, shard.spec.ShardID)
	return t, nil
}

func (s *ValkeyStore) Subscribe(filter *agent.EventFilter, handler agent.EventHandler) error {
	s.subscriptions.Subscribe(filter, handler)
	return nil
}

func (s *ValkeyStore) GetEvents(tupleID string) ([]*agent.Event, error) {
	shard, err := s.shardByID(context.Background(), tupleID)
	if err != nil {
		return nil, err
	}
	entries, err := shard.client.Do(context.Background(), shard.client.B().Xrange().Key(agentEventsStreamKey(tupleID)).Start("-").End("+").Build()).AsXRange()
	if err != nil {
		if isValkeyNil(err) {
			return []*agent.Event{}, nil
		}
		return nil, err
	}
	return parseStreamEntries(entries), nil
}

func (s *ValkeyStore) GetGlobalEvents(limit, offset int) ([]*agent.Event, error) {
	if limit <= 0 {
		limit = 100
	}
	fetch := limit + offset
	if fetch <= 0 {
		fetch = limit
	}
	all := make([]*agent.Event, 0)
	for _, shard := range s.shards {
		entries, err := shard.client.Do(context.Background(), shard.client.B().Xrevrange().Key(globalEventsKey()).End("+").Start("-").Count(int64(fetch)).Build()).AsXRange()
		if err != nil && !isValkeyNil(err) {
			return nil, err
		}
		all = append(all, parseStreamEntries(entries)...)
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

func (s *ValkeyStore) QueryEvents(query *agent.EventQuery) (*agent.EventPage, error) {
	if query == nil {
		query = &agent.EventQuery{}
	}
	limit := query.Limit
	if limit <= 0 {
		limit = 100
	}
	offset := query.Offset
	if offset < 0 {
		offset = 0
	}
	fetch := limit + offset

	if query.TupleID != "" {
		shard, err := s.shardByID(context.Background(), query.TupleID)
		if err != nil {
			return nil, err
		}
		entries, err := shard.client.Do(context.Background(), shard.client.B().Xrange().Key(agentEventsStreamKey(query.TupleID)).Start("-").End("+").Count(int64(fetch)).Build()).AsXRange()
		if err != nil && !isValkeyNil(err) {
			return nil, err
		}
		events := filterEvents(parseStreamEntries(entries), query)
		total := len(events)
		events = applyEventLimitOffset(events, limit, offset)
		return &agent.EventPage{Events: events, Total: total}, nil
	}

	all := make([]*agent.Event, 0)
	for _, shard := range s.shards {
		entries, err := shard.client.Do(context.Background(), shard.client.B().Xrevrange().Key(globalEventsKey()).End("+").Start("-").Count(int64(fetch)).Build()).AsXRange()
		if err != nil && !isValkeyNil(err) {
			return nil, err
		}
		all = append(all, parseStreamEntries(entries)...)
	}
	sort.SliceStable(all, func(i, j int) bool {
		if all[i].Timestamp.Equal(all[j].Timestamp) {
			return all[i].ID < all[j].ID
		}
		return all[i].Timestamp.After(all[j].Timestamp)
	})
	filtered := filterEvents(all, query)
	total := len(filtered)
	filtered = applyEventLimitOffset(filtered, limit, offset)
	return &agent.EventPage{Events: filtered, Total: total}, nil
}

func (s *ValkeyStore) GetDAG(rootID string) (*agent.DAG, error) {
	dag := &agent.DAG{
		Nodes: make(map[string]*agent.DAGNode),
		Edges: []agent.Edge{},
	}
	visited := make(map[string]bool)
	queue := []string{rootID}
	for len(queue) > 0 {
		id := queue[0]
		queue = queue[1:]
		if visited[id] {
			continue
		}
		visited[id] = true
		t, err := s.Get(id)
		if err != nil {
			continue
		}
		dag.Nodes[id] = &agent.DAGNode{
			TupleID:   t.ID,
			Kind:      t.Kind,
			Status:    t.Status,
			CreatedAt: t.CreatedAt,
			Metadata:  t.Metadata,
		}
		if t.ParentID != "" {
			dag.Edges = append(dag.Edges, agent.Edge{From: t.ParentID, To: t.ID, Type: "parent"})
			queue = append(queue, t.ParentID)
		}
		children, err := s.Query(&agent.Query{ParentID: t.ID})
		if err != nil {
			continue
		}
		for _, child := range children {
			dag.Edges = append(dag.Edges, agent.Edge{From: t.ID, To: child.ID, Type: "spawned"})
			queue = append(queue, child.ID)
		}
	}
	return dag, nil
}

func (s *ValkeyStore) RecordSpans(ctx context.Context, spans []telemetry.Span) error {
	ctx = defaultContext(ctx)
	byShard := make(map[string][]telemetry.Span)
	for _, span := range spans {
		shard := s.shardForSpan(span)
		byShard[shard.spec.ShardID] = append(byShard[shard.spec.ShardID], span)
	}
	for shardID, shardSpans := range byShard {
		shard := s.byShardID[shardID]
		cmds := make([]vk.Completed, 0, len(shardSpans)*3)
		for i, span := range shardSpans {
			unique := span.SpanID
			if unique == "" {
				unique = fmt.Sprintf("generated-%d-%s", i, uuid.New().String())
			}
			span.SpanID = unique
			body, err := json.Marshal(span)
			if err != nil {
				return err
			}
			key := telemetrySpanKey(span.TraceID, unique)
			score := span.TsStart.UTC().UnixMilli()
			cmds = append(cmds,
				shard.client.B().Set().Key(key).Value(string(body)).Build(),
				shard.client.B().Zadd().Key(telemetryTraceKey(span.TraceID)).ScoreMember().ScoreMember(float64(score), key).Build(),
				shard.client.B().Zadd().Key(telemetryCleanupKey()).ScoreMember().ScoreMember(float64(score), key).Build(),
			)
		}
		for _, resp := range shard.client.DoMulti(ctx, cmds...) {
			if err := resp.Error(); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *ValkeyStore) ListSpans(ctx context.Context, traceID string, namespaceID string, limit, offset int) ([]telemetry.Span, error) {
	ctx = defaultContext(ctx)
	if strings.TrimSpace(traceID) == "" {
		return nil, fmt.Errorf("trace_id is required")
	}
	if limit <= 0 {
		limit = 100
	}
	fetch := limit + offset
	if fetch <= 0 {
		fetch = limit
	}
	shards := s.shards
	if s.isSharded() && strings.TrimSpace(namespaceID) != "" {
		shards = []*shardHandle{s.shardForTelemetry(namespaceID, traceID)}
	}
	all := make([]telemetry.Span, 0)
	for _, shard := range shards {
		keys, err := shard.client.Do(ctx, shard.client.B().Zrange().Key(telemetryTraceKey(traceID)).Min("0").Max(strconv.Itoa(fetch-1)).Build()).AsStrSlice()
		if err != nil && !isValkeyNil(err) {
			return nil, err
		}
		if len(keys) == 0 {
			continue
		}
		values, err := shard.client.Do(ctx, shard.client.B().Mget().Key(keys...).Build()).ToArray()
		if err != nil {
			return nil, err
		}
		for _, value := range values {
			raw, err := value.ToString()
			if err != nil || raw == "" {
				continue
			}
			var span telemetry.Span
			if err := json.Unmarshal([]byte(raw), &span); err != nil {
				continue
			}
			if namespaceID != "" && span.NamespaceID != namespaceID {
				continue
			}
			all = append(all, span)
		}
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

func (s *ValkeyStore) CleanupSpansBefore(ctx context.Context, cutoff time.Time) (int64, error) {
	ctx = defaultContext(ctx)
	var removed int64
	cutoffScore := cutoff.UTC().UnixMilli()
	for _, shard := range s.shards {
		keys, err := shard.client.Do(ctx, shard.client.B().Zrangebyscore().Key(telemetryCleanupKey()).Min("-inf").Max(strconv.FormatInt(cutoffScore, 10)).Limit(0, int64(s.leaseReapBatch)).Build()).AsStrSlice()
		if err != nil && !isValkeyNil(err) {
			return removed, err
		}
		if len(keys) == 0 {
			continue
		}
		values, err := shard.client.Do(ctx, shard.client.B().Mget().Key(keys...).Build()).ToArray()
		if err != nil {
			return removed, err
		}
		cmds := make([]vk.Completed, 0, len(keys)*3)
		for i, key := range keys {
			cmds = append(cmds, shard.client.B().Del().Key(key).Build(), shard.client.B().Zrem().Key(telemetryCleanupKey()).Member(key).Build())
			raw, err := values[i].ToString()
			if err == nil && raw != "" {
				var span telemetry.Span
				if json.Unmarshal([]byte(raw), &span) == nil {
					cmds = append(cmds, shard.client.B().Zrem().Key(telemetryTraceKey(span.TraceID)).Member(key).Build())
				}
			}
			removed++
		}
		for _, resp := range shard.client.DoMulti(ctx, cmds...) {
			if err := resp.Error(); err != nil {
				return removed, err
			}
		}
	}
	return removed, nil
}

func (s *ValkeyStore) ShardMapInfo(ctx context.Context) (map[string]interface{}, error) {
	summaries := make([]map[string]interface{}, 0, len(s.shards))
	for _, shard := range s.shards {
		summaries = append(summaries, map[string]interface{}{
			"shard_id":        shard.spec.ShardID,
			"writer_endpoint": shard.spec.Addr,
			"owner_node_id":   shard.spec.OwnerNodeID,
			"advertise_addr":  shard.spec.AdvertiseAddr,
		})
	}
	return map[string]interface{}{
		"version":        1,
		"epoch":          0,
		"node_id":        s.nodeID,
		"advertise_addr": s.advertiseAddr,
		"shards":         summaries,
	}, nil
}

func (s *ValkeyStore) ShardHealthInfo(ctx context.Context) ([]shardadmin.ShardHealth, error) {
	out := make([]shardadmin.ShardHealth, 0, len(s.shards))
	for _, shard := range s.shards {
		err := shard.client.Do(defaultContext(ctx), shard.client.B().Ping().Build()).Error()
		out = append(out, shardadmin.ShardHealth{
			ShardID:       shard.spec.ShardID,
			OwnerNodeID:   shard.spec.OwnerNodeID,
			AdvertiseAddr: shard.spec.AdvertiseAddr,
			Ready:         err == nil,
			Error:         errorString(err),
		})
	}
	return out, nil
}

func (s *ValkeyStore) ShardQueueStats(ctx context.Context, namespaceID string, queue string) (map[string]interface{}, error) {
	shard := s.shardForQueueStats(namespaceID, queue)
	if shard == nil {
		return nil, fmt.Errorf("valkey store has no shards")
	}
	values, err := s.queryShard(defaultContext(ctx), shard, &agent.Query{
		NamespaceID: namespaceID,
		Metadata:    map[string]string{queueMetadataKey: queue},
	})
	if err != nil {
		return nil, err
	}
	counts := make(map[string]int)
	for _, value := range values {
		counts[string(value.Status)]++
	}
	total := 0
	for _, count := range counts {
		total += count
	}
	return map[string]interface{}{
		"namespace_id": namespaceID,
		"queue":        queue,
		"shard_id":     shard.spec.ShardID,
		"counts":       counts,
		"total":        total,
	}, nil
}

func (s *ValkeyStore) startWakeupListeners() {
	for _, shard := range s.shards {
		s.pubsubWG.Add(1)
		go func(shard *shardHandle) {
			defer s.pubsubWG.Done()
			_ = shard.client.Receive(s.ctx, shard.client.B().Subscribe().Channel(valkeyWakeupChannel).Build(), func(_ vk.PubSubMessage) {
				s.signalWakeups()
			})
		}(shard)
	}
}

func (s *ValkeyStore) currentWakeupChan() <-chan struct{} {
	s.wakeupMu.RLock()
	defer s.wakeupMu.RUnlock()
	return s.wakeupCh
}

func (s *ValkeyStore) signalWakeups() {
	s.wakeupMu.Lock()
	defer s.wakeupMu.Unlock()
	close(s.wakeupCh)
	s.wakeupCh = make(chan struct{})
}

func (s *ValkeyStore) waitForWakeup(ctx context.Context, deadline time.Time) error {
	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	case <-s.currentWakeupChan():
		return nil
	}
}

func (s *ValkeyStore) publishWakeup(shard *shardHandle, namespaceID string, queue string) {
	if !s.publishPubSub || shard == nil {
		return
	}
	payload := strings.TrimSpace(namespaceID) + "|" + strings.TrimSpace(queue)
	if err := shard.client.Do(context.Background(), shard.client.B().Publish().Channel(valkeyWakeupChannel).Message(payload).Build()).Error(); err != nil {
		s.logger.Debug("publish valkey wakeup failed", zap.Error(err))
	}
}

func (s *ValkeyStore) notifySubscribers(event *agent.Event) {
	s.subscriptions.Notify(event)
}

func (s *ValkeyStore) leaseDurationForAgent(t *agent.Agent) time.Duration {
	if t == nil {
		return s.leaseDuration
	}
	return leaseDurationFromMetadata(t.Metadata, s.leaseDuration)
}

func (s *ValkeyStore) leaseReaperLoop() {
	ticker := time.NewTicker(s.leaseReapInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return
		case <-ticker.C:
			s.reapExpiredLeases()
		}
	}
}

func (s *ValkeyStore) tryAcquireReaperLeadership(ctx context.Context, shard *shardHandle) bool {
	if shard == nil {
		return false
	}
	if s.ownerRouted {
		return s.ownsShard(shard.spec)
	}
	ttl := max(int64(1000), s.leaseReapInterval.Milliseconds()*3)
	lockKey := reaperLeaderKey(shard.spec.ShardID)
	res := shard.client.Do(ctx, shard.client.B().Set().Key(lockKey).Value(s.reaperID).Nx().PxMilliseconds(ttl).Build())
	if err := res.Error(); err == nil {
		return true
	}
	currentOwner, err := shard.client.Do(ctx, shard.client.B().Get().Key(lockKey).Build()).ToString()
	if err != nil || currentOwner != s.reaperID {
		return false
	}
	return shard.client.Do(ctx, shard.client.B().Set().Key(lockKey).Value(s.reaperID).Xx().PxMilliseconds(ttl).Build()).Error() == nil
}

func (s *ValkeyStore) reapExpiredLeases() {
	now := time.Now().UTC()
	for _, shard := range s.shards {
		if s.ownerRouted && !s.ownsShard(shard.spec) {
			continue
		}
		if !s.tryAcquireReaperLeadership(s.ctx, shard) {
			continue
		}
		resp, err := execScriptJSON(s.reapScript, s.ctx, shard.client, nil, []string{
			now.Format(time.RFC3339Nano),
			strconv.FormatInt(now.UnixMilli(), 10),
			strconv.Itoa(s.leaseReapBatch),
			"1",
		})
		if err != nil {
			s.logger.Debug("valkey lease reap failed", zap.Error(err), zap.String("shard_id", shard.spec.ShardID))
			continue
		}
		if resp.Count > 0 {
			s.signalWakeups()
			s.publishWakeup(shard, "", "")
		}
	}
}

func (s *ValkeyStore) shardForSpan(span telemetry.Span) *shardHandle {
	if !s.isSharded() {
		return s.shards[0]
	}
	if namespaceID := strings.TrimSpace(span.NamespaceID); namespaceID != "" {
		return s.shardForTelemetry(namespaceID, span.TraceID)
	}
	return s.shardForTelemetry("__trace__", span.TraceID)
}

func (s *ValkeyStore) shardForTelemetry(namespaceID string, traceID string) *shardHandle {
	if !s.isSharded() {
		return s.shards[0]
	}
	keyQueue := "__telemetry__"
	if strings.TrimSpace(traceID) != "" {
		keyQueue = "__telemetry__:" + traceID
	}
	shard, err := s.targetShard(namespaceID, keyQueue)
	if err != nil {
		return s.shards[0]
	}
	return shard
}

func (s *ValkeyStore) shardForQueueStats(namespaceID string, queue string) *shardHandle {
	if !s.isSharded() {
		if len(s.shards) == 0 {
			return nil
		}
		return s.shards[0]
	}
	shard, err := s.targetShard(namespaceID, queue)
	if err != nil {
		return nil
	}
	return shard
}

func (s *ValkeyStore) storeLabel() string {
	if s.isSharded() {
		return "valkey"
	}
	return "valkey"
}

func (b *contextBoundStore) Get(id string) (*agent.Agent, error) {
	return b.store.getWithContext(b.ctx, id)
}

func (b *contextBoundStore) Out(t *agent.Agent) error {
	return b.store.Out(t)
}

func (b *contextBoundStore) In(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return b.store.in(b.ctx, query, timeout)
}

func (b *contextBoundStore) Read(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return b.store.Read(query, timeout)
}

func (b *contextBoundStore) Query(query *agent.Query) ([]*agent.Agent, error) {
	return b.store.query(b.ctx, query)
}

func (b *contextBoundStore) Take(query *agent.Query) (*agent.Agent, error) {
	return b.store.take(b.ctx, query)
}

func (b *contextBoundStore) Update(id string, updates map[string]interface{}) error {
	return b.store.Update(id, updates)
}

func (b *contextBoundStore) Complete(id string, agentID string, leaseToken string, result map[string]interface{}) error {
	return b.store.Complete(id, agentID, leaseToken, result)
}

func (b *contextBoundStore) CompleteAndOut(id string, agentID string, leaseToken string, result map[string]interface{}, outputs []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	return b.store.CompleteAndOut(id, agentID, leaseToken, result, outputs)
}

func (b *contextBoundStore) Release(id string, agentID string, leaseToken string, reason string) error {
	return b.store.Release(id, agentID, leaseToken, reason)
}

func (b *contextBoundStore) RenewLease(id string, agentID string, leaseToken string, leaseDuration time.Duration) (*agent.Agent, error) {
	return b.store.RenewLease(id, agentID, leaseToken, leaseDuration)
}

func (b *contextBoundStore) Subscribe(filter *agent.EventFilter, handler agent.EventHandler) error {
	return b.store.Subscribe(filter, handler)
}

func (b *contextBoundStore) GetEvents(tupleID string) ([]*agent.Event, error) {
	return b.store.GetEvents(tupleID)
}

func (b *contextBoundStore) GetGlobalEvents(limit, offset int) ([]*agent.Event, error) {
	return b.store.GetGlobalEvents(limit, offset)
}

func (b *contextBoundStore) QueryEvents(query *agent.EventQuery) (*agent.EventPage, error) {
	return b.store.QueryEvents(query)
}

func (b *contextBoundStore) GetDAG(rootID string) (*agent.DAG, error) {
	return b.store.GetDAG(rootID)
}

func (b *contextBoundStore) Count(query *agent.Query) (int, error) {
	return b.store.Count(query)
}

func (b *contextBoundStore) CountByStatus(query *agent.Query) (map[agent.Status]int, error) {
	return b.store.CountByStatus(query)
}

func (b *contextBoundStore) Health(ctx context.Context) error {
	if ctx == nil {
		ctx = b.ctx
	}
	return b.store.Health(ctx)
}

func normalizeAgentForOut(t *agent.Agent, now time.Time, shardID string) {
	if t.ID == "" {
		t.ID = uuid.New().String()
	}
	if t.CreatedAt.IsZero() {
		t.CreatedAt = now
	}
	if t.Status == "" {
		t.Status = agent.StatusNew
	}
	t.UpdatedAt = now
	if t.Version == 0 {
		t.Version = 1
	}
	if shardID != "" {
		t.ShardID = shardID
	}
	normalizeAgentMetadata(t, shardID)
}

func normalizeOutputForParent(out *agent.Agent, parent *agent.Agent, now time.Time, shardID string) {
	if out == nil {
		return
	}
	if out.ID == "" {
		out.ID = uuid.New().String()
	}
	if out.CreatedAt.IsZero() {
		out.CreatedAt = now
	}
	out.UpdatedAt = now
	if out.Version == 0 {
		out.Version = 1
	}
	if out.Status == "" {
		out.Status = agent.StatusNew
	}
	if out.ParentID == "" && parent != nil {
		out.ParentID = parent.ID
	}
	if out.TraceID == "" && parent != nil {
		out.TraceID = parent.TraceID
	}
	if out.NamespaceID == "" && parent != nil {
		out.NamespaceID = parent.NamespaceID
	}
	if shardID != "" {
		out.ShardID = shardID
	}
	normalizeAgentMetadata(out, shardID)
}

func normalizeAgentMetadata(t *agent.Agent, shardID string) {
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	if t.NamespaceID != "" {
		t.Metadata[namespaceMetadataKey] = t.NamespaceID
	} else if ns := strings.TrimSpace(t.Metadata[namespaceMetadataKey]); ns != "" {
		t.NamespaceID = ns
	}
	t.Metadata[internalCreatedMsKey] = strconv.FormatInt(t.CreatedAt.UTC().UnixMilli(), 10)
	if queue := strings.TrimSpace(t.Metadata[queueMetadataKey]); queue != "" && strings.TrimSpace(t.Metadata[queueCreatedMsKey]) == "" {
		t.Metadata[queueCreatedMsKey] = strconv.FormatInt(t.CreatedAt.UTC().UnixMilli(), 10)
	}
	if t.LeaseUntil.IsZero() {
		delete(t.Metadata, internalLeaseUntilMsKey)
	} else {
		t.Metadata[internalLeaseUntilMsKey] = strconv.FormatInt(t.LeaseUntil.UTC().UnixMilli(), 10)
	}
	if shardID != "" {
		t.ShardID = shardID
	}
}

func cloneAgent(src *agent.Agent) *agent.Agent {
	if src == nil {
		return nil
	}
	dst := *src
	if src.Payload != nil {
		dst.Payload = make(map[string]interface{}, len(src.Payload))
		for k, v := range src.Payload {
			dst.Payload[k] = v
		}
	}
	if src.Metadata != nil {
		dst.Metadata = make(map[string]string, len(src.Metadata))
		for k, v := range src.Metadata {
			dst.Metadata[k] = v
		}
	}
	if src.Tags != nil {
		dst.Tags = append([]string(nil), src.Tags...)
	}
	return &dst
}

func decodeAgent(raw string) (*agent.Agent, error) {
	if strings.Contains(raw, `"tags":{}`) {
		raw = strings.ReplaceAll(raw, `"tags":{}`, `"tags":[]`)
	}
	var t agent.Agent
	if err := json.Unmarshal([]byte(raw), &t); err != nil {
		return nil, err
	}
	if t.Metadata == nil {
		t.Metadata = make(map[string]string)
	}
	if ns := t.Metadata[namespaceMetadataKey]; t.NamespaceID == "" && ns != "" {
		t.NamespaceID = ns
	}
	if t.LeaseUntil.IsZero() {
		if rawLease := strings.TrimSpace(t.Metadata[internalLeaseUntilMsKey]); rawLease != "" {
			if leaseMS, err := strconv.ParseInt(rawLease, 10, 64); err == nil && leaseMS > 0 {
				t.LeaseUntil = time.UnixMilli(leaseMS).UTC()
			}
		}
	}
	return &t, nil
}

func isExpired(t *agent.Agent, now time.Time) bool {
	if t == nil || t.TTL <= 0 {
		return false
	}
	return !t.CreatedAt.IsZero() && t.CreatedAt.Add(t.TTL).Before(now)
}

func matchesQueryFilters(t *agent.Agent, query *agent.Query) bool {
	if t == nil || query == nil {
		return true
	}
	if query.Kind != "" && t.Kind != query.Kind {
		return false
	}
	if query.Status != "" && t.Status != query.Status {
		return false
	}
	if query.Owner != "" && t.Owner != query.Owner {
		return false
	}
	if query.ParentID != "" && t.ParentID != query.ParentID {
		return false
	}
	if query.TraceID != "" && t.TraceID != query.TraceID {
		return false
	}
	if query.NamespaceID != "" {
		namespaceID := t.NamespaceID
		if namespaceID == "" && t.Metadata != nil {
			namespaceID = t.Metadata[namespaceMetadataKey]
		}
		if namespaceID != query.NamespaceID {
			return false
		}
	}
	if len(query.Tags) > 0 {
		for _, tag := range query.Tags {
			found := false
			for _, existing := range t.Tags {
				if existing == tag {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	if query.Metadata != nil {
		for k, v := range query.Metadata {
			if k == "agent_id" {
				continue
			}
			if t.Metadata == nil {
				return false
			}
			if current, ok := t.Metadata[k]; !ok || current != v {
				return false
			}
		}
	}
	return true
}

func extractAgentID(query *agent.Query) string {
	if query == nil {
		return ""
	}
	if strings.TrimSpace(query.AgentID) != "" {
		return strings.TrimSpace(query.AgentID)
	}
	if query.Metadata != nil {
		return strings.TrimSpace(query.Metadata["agent_id"])
	}
	return ""
}

func sortAgents(values []*agent.Agent) {
	sort.SliceStable(values, func(i, j int) bool {
		if values[i].CreatedAt.Equal(values[j].CreatedAt) {
			return values[i].ID < values[j].ID
		}
		return values[i].CreatedAt.Before(values[j].CreatedAt)
	})
}

func applyAgentLimitOffset(values []*agent.Agent, query *agent.Query) []*agent.Agent {
	if query == nil {
		return values
	}
	if query.Offset > 0 {
		if query.Offset >= len(values) {
			return []*agent.Agent{}
		}
		values = values[query.Offset:]
	}
	if query.Limit > 0 && query.Limit < len(values) {
		values = values[:query.Limit]
	}
	return values
}

func cloneQueryWithoutPagination(query *agent.Query) *agent.Query {
	if query == nil {
		return nil
	}
	clone := *query
	clone.Limit = 0
	clone.Offset = 0
	if query.Tags != nil {
		clone.Tags = append([]string(nil), query.Tags...)
	}
	if query.Metadata != nil {
		clone.Metadata = make(map[string]string, len(query.Metadata))
		for k, v := range query.Metadata {
			clone.Metadata[k] = v
		}
	}
	return &clone
}

func applyEventLimitOffset(values []*agent.Event, limit, offset int) []*agent.Event {
	if offset > 0 {
		if offset >= len(values) {
			return []*agent.Event{}
		}
		values = values[offset:]
	}
	if limit > 0 && limit < len(values) {
		values = values[:limit]
	}
	return values
}

func filterEvents(events []*agent.Event, query *agent.EventQuery) []*agent.Event {
	if query == nil {
		return events
	}
	out := make([]*agent.Event, 0, len(events))
	for _, event := range events {
		if query.TupleID != "" && event.TupleID != query.TupleID {
			continue
		}
		if query.AgentID != "" && event.AgentID != query.AgentID {
			continue
		}
		if query.Type != "" && event.Type != query.Type {
			continue
		}
		if query.TraceID != "" && event.TraceID != query.TraceID {
			continue
		}
		out = append(out, event)
	}
	return out
}

func parseStreamEntries(entries []vk.XRangeEntry) []*agent.Event {
	out := make([]*agent.Event, 0, len(entries))
	for _, entry := range entries {
		event := &agent.Event{
			ID:      entry.ID,
			TupleID: entry.FieldValues["tuple_id"],
			Type:    agent.EventType(entry.FieldValues["type"]),
			Kind:    entry.FieldValues["kind"],
			AgentID: entry.FieldValues["agent_id"],
			TraceID: entry.FieldValues["trace_id"],
		}
		if raw := entry.FieldValues["timestamp"]; raw != "" {
			if ts, err := time.Parse(time.RFC3339Nano, raw); err == nil {
				event.Timestamp = ts
			}
		}
		if raw := entry.FieldValues["version"]; raw != "" {
			if version, err := strconv.ParseInt(raw, 10, 64); err == nil {
				event.Version = version
			}
		}
		if raw := entry.FieldValues["data"]; raw != "" {
			data := make(map[string]interface{})
			if err := json.Unmarshal([]byte(raw), &data); err == nil {
				event.Data = data
			}
		}
		out = append(out, event)
	}
	return out
}

func execScriptJSON(script *vk.Lua, ctx context.Context, client vk.Client, keys []string, args []string) (*scriptResponse, error) {
	if script == nil {
		return nil, fmt.Errorf("script is nil")
	}
	result := script.Exec(defaultContext(ctx), client, keys, args)
	if err := result.Error(); err != nil {
		return nil, err
	}
	raw, err := result.ToString()
	if err != nil {
		return nil, err
	}
	resp := &scriptResponse{}
	if err := json.Unmarshal([]byte(raw), resp); err != nil {
		return nil, err
	}
	return resp, nil
}

func boolLuaArg(value bool) string {
	if value {
		return "1"
	}
	return "0"
}

func hotStateKey(id string) string {
	return "agent_hot:" + part(id)
}

func reaperLeaderKey(shardID string) string {
	return "reaper:leader:" + part(shardID)
}

func leaseDurationFromMetadata(metadata map[string]string, fallback time.Duration) time.Duration {
	if metadata == nil {
		return fallback
	}
	if leaseSec, ok := metadata["lease_sec"]; ok {
		if sec, err := strconv.Atoi(leaseSec); err == nil && sec > 0 {
			return time.Duration(sec) * time.Second
		}
	}
	if leaseMs, ok := metadata["lease_ms"]; ok {
		if ms, err := strconv.Atoi(leaseMs); err == nil && ms > 0 {
			return time.Duration(ms) * time.Millisecond
		}
	}
	return fallback
}

func (s *ValkeyStore) getManyHotStatesFromShard(ctx context.Context, shard *shardHandle, ids []string) (map[string]*hotAgentState, error) {
	if shard == nil || len(ids) == 0 {
		return map[string]*hotAgentState{}, nil
	}
	keys := make([]string, 0, len(ids))
	for _, id := range ids {
		keys = append(keys, hotStateKey(id))
	}
	values, err := shard.client.Do(ctx, shard.client.B().Mget().Key(keys...).Build()).ToArray()
	if err != nil {
		return nil, err
	}
	out := make(map[string]*hotAgentState, len(ids))
	for idx, value := range values {
		raw, err := value.ToString()
		if err != nil || raw == "" {
			continue
		}
		state, err := decodeHotAgentState(raw)
		if err != nil {
			continue
		}
		out[ids[idx]] = state
	}
	return out, nil
}

func (s *ValkeyStore) takeFromQueueHotPath(ctx context.Context, shard *shardHandle, query *agent.Query, agentID string) (*agent.Agent, error) {
	if query == nil || query.Metadata == nil || shard == nil {
		return nil, nil
	}
	namespaceID := strings.TrimSpace(query.NamespaceID)
	queue := strings.TrimSpace(query.Metadata[queueMetadataKey])
	if namespaceID == "" || queue == "" {
		return nil, nil
	}
	now := time.Now().UTC()
	nowMS := now.UnixMilli()
	leaseToken := uuid.NewString()
	resp, err := execScriptJSON(s.queueTakeScript, ctx, shard.client, []string{queueIndexKey(namespaceID, queue)}, []string{
		namespaceID,
		queue,
		strings.TrimSpace(query.Kind),
		strings.TrimSpace(query.Owner),
		agentID,
		leaseToken,
		now.Format(time.RFC3339Nano),
		strconv.FormatInt(nowMS, 10),
		strconv.FormatInt(s.leaseDuration.Milliseconds(), 10),
		boolLuaArg(!s.deferEvents),
	})
	if err != nil {
		return nil, err
	}
	switch resp.Code {
	case "", "CLAIMED":
		claimed, err := decodeAgent(resp.Agent)
		if err != nil {
			return nil, fmt.Errorf("decode hot-claim agent json len=%d raw=%q: %w", len(resp.Agent), resp.Agent, err)
		}
		claimed.ShardID = shard.spec.ShardID
		claimed.LeaseUntil = now.Add(leaseDurationFromMetadata(claimed.Metadata, s.leaseDuration))
		s.idToShard.Store(claimed.ID, shard.spec.ShardID)
		claimedEvent := &agent.Event{
			ID:        uuid.NewString(),
			TupleID:   claimed.ID,
			Type:      agent.EventClaimed,
			Kind:      claimed.Kind,
			Timestamp: now,
			AgentID:   agentID,
			TraceID:   claimed.TraceID,
			Version:   claimed.Version,
			Data:      map[string]interface{}{"agent_id": agentID},
		}
		s.notifySubscribers(claimedEvent)
		if s.deferEvents {
			s.enqueueEvent(claimedEvent, claimed.NamespaceID, shard.spec.ShardID)
		}
		return claimed, nil
	case "EMPTY", "NOT_FOUND", "STATUS_CHANGED":
		return nil, nil
	case "NOT_QUEUE_HEAD":
		return nil, nil
	default:
		return nil, errors.New(resp.Code)
	}
}

func (s *ValkeyStore) batchTakeFromQueueHotPath(ctx context.Context, shard *shardHandle, query *agent.Query, agentID string, limit int) ([]*agent.Agent, error) {
	if query == nil || query.Metadata == nil || shard == nil || limit <= 0 {
		return nil, nil
	}
	namespaceID := strings.TrimSpace(query.NamespaceID)
	queue := strings.TrimSpace(query.Metadata[queueMetadataKey])
	if namespaceID == "" || queue == "" {
		return nil, nil
	}
	now := time.Now().UTC()
	nowMS := now.UnixMilli()
	resp, err := execScriptJSON(s.queueBatchTakeScript, ctx, shard.client, []string{queueIndexKey(namespaceID, queue)}, []string{
		namespaceID,
		queue,
		strings.TrimSpace(query.Kind),
		strings.TrimSpace(query.Owner),
		agentID,
		uuid.NewString(),
		now.Format(time.RFC3339Nano),
		strconv.FormatInt(nowMS, 10),
		strconv.FormatInt(s.leaseDuration.Milliseconds(), 10),
		strconv.Itoa(limit),
		boolLuaArg(!s.deferEvents),
	})
	if err != nil {
		return nil, err
	}
	switch resp.Code {
	case "", "BATCH_CLAIMED":
	case "EMPTY", "NOT_FOUND", "STATUS_CHANGED", "NOT_QUEUE_HEAD":
		return []*agent.Agent{}, nil
	default:
		return nil, errors.New(resp.Code)
	}
	out := make([]*agent.Agent, 0, len(resp.Agents))
	for _, raw := range resp.Agents {
		claimed, err := decodeAgent(raw)
		if err != nil {
			return nil, fmt.Errorf("decode hot-batch-claim agent json len=%d raw=%q: %w", len(raw), raw, err)
		}
		claimed.ShardID = shard.spec.ShardID
		claimed.LeaseUntil = now.Add(leaseDurationFromMetadata(claimed.Metadata, s.leaseDuration))
		s.idToShard.Store(claimed.ID, shard.spec.ShardID)
		claimedEvent := &agent.Event{
			ID:        uuid.NewString(),
			TupleID:   claimed.ID,
			Type:      agent.EventClaimed,
			Kind:      claimed.Kind,
			Timestamp: now,
			AgentID:   agentID,
			TraceID:   claimed.TraceID,
			Version:   claimed.Version,
			Data:      map[string]interface{}{"agent_id": agentID},
		}
		s.notifySubscribers(claimedEvent)
		if s.deferEvents {
			s.enqueueEvent(claimedEvent, claimed.NamespaceID, shard.spec.ShardID)
		}
		out = append(out, claimed)
	}
	return out, nil
}

func errorString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

func isValkeyNil(err error) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToUpper(err.Error()), "NIL")
}

func part(value string) string {
	return fmt.Sprintf("%d:%s", len(value), value)
}

func agentKey(id string) string                 { return "agent:" + part(id) }
func allIndexKey() string                       { return globalAllIndexKeyName }
func statusIndexKey(status agent.Status) string { return "index:status:" + part(string(status)) }
func kindIndexKey(kind string) string           { return "index:kind:" + part(kind) }
func ownerIndexKey(owner string) string         { return "index:owner:" + part(owner) }
func traceIndexKey(trace string) string         { return "index:trace:" + part(trace) }
func parentIndexKey(parent string) string       { return "index:parent:" + part(parent) }
func namespaceIndexKey(namespace string) string { return "index:namespace:" + part(namespace) }
func tagIndexKey(tag string) string             { return "index:tag:" + part(tag) }
func metadataIndexKey(key string, value string) string {
	return "index:meta:" + part(key) + ":" + part(value)
}
func queueIndexKey(namespaceID string, queue string) string {
	return "queue:" + part(namespaceID) + ":" + part(queue) + ":new"
}
func globalOrderKey() string  { return globalOrderKeyName }
func globalEventsKey() string { return globalEventsStreamKey }
func agentEventsStreamKey(id string) string {
	return "stream:events:agent:" + part(id)
}
func traceEventsStreamKey(traceID string) string {
	return "stream:events:trace:" + part(traceID)
}
func telemetrySpanKey(traceID string, spanID string) string {
	return "telemetry:span:" + part(traceID) + ":" + part(spanID)
}
func telemetryTraceKey(traceID string) string {
	return "telemetry:trace:" + part(traceID)
}
func telemetryCleanupKey() string { return telemetryCleanupKeyName }

func classifyValkeyCompleteCode(code string, id string) error {
	switch code {
	case "NOT_FOUND":
		return fmt.Errorf("agent not found: %s", id)
	case "STALE_TOKEN":
		return errors.New(tokenMismatchStaleErr)
	case "INVALID_STATUS":
		return fmt.Errorf("invalid status")
	case "OWNER_MISMATCH":
		return fmt.Errorf("cannot complete: ownership mismatch")
	case "LEASE_TOKEN_MISMATCH":
		return fmt.Errorf("lease token mismatch")
	default:
		return errors.New(code)
	}
}

func classifyValkeyReleaseCode(code string, id string) error {
	switch code {
	case "NOT_FOUND":
		return fmt.Errorf("agent not found: %s", id)
	case "INVALID_STATUS":
		return fmt.Errorf("invalid status")
	case "OWNER_MISMATCH":
		return fmt.Errorf("cannot release: ownership mismatch")
	case "LEASE_TOKEN_MISMATCH":
		return fmt.Errorf("lease token mismatch")
	default:
		return errors.New(code)
	}
}

func classifyValkeyRenewCode(code string, id string) error {
	switch code {
	case "NOT_FOUND":
		return fmt.Errorf("agent not found: %s", id)
	case "INVALID_STATUS":
		return fmt.Errorf("invalid status")
	case "OWNER_MISMATCH":
		return fmt.Errorf("cannot renew: ownership mismatch")
	case "LEASE_TOKEN_MISMATCH":
		return fmt.Errorf("lease token mismatch")
	default:
		return errors.New(code)
	}
}

const luaHelpers = `
local function part(v)
  v = tostring(v or '')
  return string.len(v) .. ':' .. v
end

local function ensure_array(value)
  if type(value) == 'table' then
    if next(value) == nil and cjson.array_mt ~= nil and getmetatable(value) == nil then
      setmetatable(value, cjson.array_mt)
    end
    return value
  end
  if cjson.array_mt ~= nil then
    return setmetatable({}, cjson.array_mt)
  end
  return cjson.decode('[]')
end

local function ensure_object(value)
  if type(value) == 'table' then
    return value
  end
  return cjson.decode('{}')
end

local function normalize_agent(agent)
  if agent == nil then
    return nil
  end
  agent.tags = ensure_array(agent.tags)
  agent.metadata = ensure_object(agent.metadata)
  if agent.payload == nil or type(agent.payload) ~= 'table' then
    agent.payload = ensure_object(agent.payload)
  end
  return agent
end

local function metadata_value(agent, key)
  if agent == nil then
    return ''
  end
  local metadata = ensure_object(agent.metadata)
  agent.metadata = metadata
  local v = metadata[key]
  if v == nil then
    return ''
  end
  return tostring(v)
end

local function namespace_value(agent)
  if agent == nil then
    return ''
  end
  if agent.namespace_id ~= nil and tostring(agent.namespace_id) ~= '' then
    return tostring(agent.namespace_id)
  end
  return metadata_value(agent, 'namespace_id')
end

local function queue_value(agent)
  return metadata_value(agent, 'queue')
end

local function created_score(agent)
  local raw = metadata_value(agent, '__created_ms')
  if raw == '' then
    return 0
  end
  local score = tonumber(raw)
  if score == nil then
    return 0
  end
  return score
end

local function queue_score(agent)
  local raw = metadata_value(agent, 'queue_created_ms')
  if raw ~= '' then
    local score = tonumber(raw)
    if score ~= nil then
      return score
    end
  end
  return created_score(agent)
end

local function lease_score(agent)
  local raw = metadata_value(agent, '__lease_until_ms')
  if raw == '' then
    return 0
  end
  local score = tonumber(raw)
  if score == nil then
    return 0
  end
  return score
end

local function status_key(status)
  return 'index:status:' .. part(status or '')
end

local function kind_key(kind)
  return 'index:kind:' .. part(kind or '')
end

local function owner_key(owner)
  return 'index:owner:' .. part(owner or '')
end

local function trace_key(trace_id)
  return 'index:trace:' .. part(trace_id or '')
end

local function parent_key(parent_id)
  return 'index:parent:' .. part(parent_id or '')
end

local function namespace_key(namespace_id)
  return 'index:namespace:' .. part(namespace_id or '')
end

local function tag_key(tag)
  return 'index:tag:' .. part(tag or '')
end

local function metadata_key(key, value)
  return 'index:meta:' .. part(key or '') .. ':' .. part(value or '')
end

local function queue_key(agent)
  local queue = queue_value(agent)
  if queue == '' then
    return ''
  end
  return 'queue:' .. part(namespace_value(agent)) .. ':' .. part(queue) .. ':new'
end

local function hot_key(tuple_id)
  return 'agent_hot:' .. part(tuple_id)
end

local function lease_duration_ms(agent, default_ms)
  local lease_ms_raw = metadata_value(agent, 'lease_ms')
  if lease_ms_raw ~= '' then
    local parsed = tonumber(lease_ms_raw)
    if parsed ~= nil and parsed > 0 then
      return math.floor(parsed)
    end
  end
  local lease_sec_raw = metadata_value(agent, 'lease_sec')
  if lease_sec_raw ~= '' then
    local parsed = tonumber(lease_sec_raw)
    if parsed ~= nil and parsed > 0 then
      return math.floor(parsed * 1000)
    end
  end
  return tonumber(default_ms) or 0
end

local function hot_state(agent, tuple_id, default_lease_ms)
  return {
    id = tuple_id,
    kind = tostring(agent.kind or ''),
    status = tostring(agent.status or ''),
    namespace_id = namespace_value(agent),
    queue = queue_value(agent),
    owner = tostring(agent.owner or ''),
    lease_token = tostring(agent.lease_token or ''),
    lease_duration_ms = lease_duration_ms(agent, default_lease_ms),
    lease_until_ms = lease_score(agent),
    version = tonumber(agent.version or 0) or 0,
    created_ms = created_score(agent),
    queue_created_ms = queue_score(agent),
  }
end

local function trace_stream_key(trace_id)
  if trace_id == nil or trace_id == '' then
    return ''
  end
  return 'stream:events:trace:' .. part(trace_id)
end

local function remove_indexes(agent, tuple_id)
  agent = normalize_agent(agent)
  if agent == nil then
    return
  end
  redis.call('SREM', 'index:all', tuple_id)
  redis.call('SREM', status_key(agent.status), tuple_id)
  if agent.kind ~= nil and tostring(agent.kind) ~= '' then
    redis.call('SREM', kind_key(agent.kind), tuple_id)
  end
  if agent.owner ~= nil and tostring(agent.owner) ~= '' then
    redis.call('SREM', owner_key(agent.owner), tuple_id)
  end
  if agent.trace_id ~= nil and tostring(agent.trace_id) ~= '' then
    redis.call('SREM', trace_key(agent.trace_id), tuple_id)
  end
  if agent.parent_id ~= nil and tostring(agent.parent_id) ~= '' then
    redis.call('SREM', parent_key(agent.parent_id), tuple_id)
  end
  local ns = namespace_value(agent)
  if ns ~= '' then
    redis.call('SREM', namespace_key(ns), tuple_id)
  end
  for _, tag in ipairs(agent.tags) do
    redis.call('SREM', tag_key(tostring(tag)), tuple_id)
  end
  for k, v in pairs(agent.metadata) do
    if v ~= nil then
      redis.call('SREM', metadata_key(tostring(k), tostring(v)), tuple_id)
    end
  end
  if tostring(agent.status or '') == 'NEW' then
    local qk = queue_key(agent)
    if qk ~= '' then
      redis.call('ZREM', qk, tuple_id)
    else
      redis.call('ZREM', 'order:new', tuple_id)
    end
  end
  if tostring(agent.status or '') == 'IN_PROGRESS' then
    redis.call('ZREM', 'lease:by_until', tuple_id)
  end
end

local function add_indexes(agent, tuple_id)
  agent = normalize_agent(agent)
  redis.call('SADD', 'index:all', tuple_id)
  redis.call('SADD', status_key(agent.status), tuple_id)
  if agent.kind ~= nil and tostring(agent.kind) ~= '' then
    redis.call('SADD', kind_key(agent.kind), tuple_id)
  end
  if agent.owner ~= nil and tostring(agent.owner) ~= '' then
    redis.call('SADD', owner_key(agent.owner), tuple_id)
  end
  if agent.trace_id ~= nil and tostring(agent.trace_id) ~= '' then
    redis.call('SADD', trace_key(agent.trace_id), tuple_id)
  end
  if agent.parent_id ~= nil and tostring(agent.parent_id) ~= '' then
    redis.call('SADD', parent_key(agent.parent_id), tuple_id)
  end
  local ns = namespace_value(agent)
  if ns ~= '' then
    redis.call('SADD', namespace_key(ns), tuple_id)
  end
  for _, tag in ipairs(agent.tags) do
    redis.call('SADD', tag_key(tostring(tag)), tuple_id)
  end
  for k, v in pairs(agent.metadata) do
    if v ~= nil then
      redis.call('SADD', metadata_key(tostring(k), tostring(v)), tuple_id)
    end
  end
  if tostring(agent.status or '') == 'NEW' then
    local qk = queue_key(agent)
    if qk ~= '' then
      redis.call('ZADD', qk, queue_score(agent), tuple_id)
    else
      redis.call('ZADD', 'order:new', created_score(agent), tuple_id)
    end
  end
  if tostring(agent.status or '') == 'IN_PROGRESS' then
    local lease_until = lease_score(agent)
    if lease_until > 0 then
      redis.call('ZADD', 'lease:by_until', lease_until, tuple_id)
    end
  end
end

local function append_event(tuple_id, kind, event_type, timestamp, agent_id, trace_id, version, data_json, namespace_id)
  local fields = {
    'tuple_id', tuple_id,
    'type', event_type,
    'kind', kind or '',
    'timestamp', timestamp,
    'agent_id', agent_id or '',
    'trace_id', trace_id or '',
    'version', tostring(version or 0),
    'data', data_json or '{}',
    'namespace_id', namespace_id or '',
  }
  redis.call('XADD', 'stream:events', 'MAXLEN', '~', 500000, '*', unpack(fields))
  redis.call('XADD', 'stream:events:agent:' .. part(tuple_id), 'MAXLEN', '~', 200000, '*', unpack(fields))
  local tsk = trace_stream_key(trace_id)
  if tsk ~= '' then
    redis.call('XADD', tsk, 'MAXLEN', '~', 200000, '*', unpack(fields))
  end
end

local function load_agent(agent_key)
  local agent_data = redis.call('GET', agent_key)
  if not agent_data then
    return nil, nil
  end
  return normalize_agent(cjson.decode(agent_data)), agent_data
end

local function load_hot(tuple_id)
  local raw = redis.call('GET', hot_key(tuple_id))
  if not raw then
    return nil
  end
  return cjson.decode(raw)
end

local function persist_agent(agent_key, agent, tuple_id, default_lease_ms)
  local encoded = cjson.encode(agent)
  redis.call('SET', agent_key, encoded)
  redis.call('SET', hot_key(tuple_id), cjson.encode(hot_state(agent, tuple_id, default_lease_ms)))
  return encoded
end

local function write_events_enabled(flag)
  return flag == nil or flag == '' or flag == '1' or flag == 'true'
end
`

const outScriptSource = luaHelpers + `
local agent_key = KEYS[1]
local new_json = ARGV[1]
local tuple_id = ARGV[2]
local timestamp = ARGV[3]
local agent_id = ARGV[4]
local event_data = ARGV[5]
local write_events = ARGV[6]

local old_agent = nil
local old_json = redis.call('GET', agent_key)
if old_json then
  old_agent = normalize_agent(cjson.decode(old_json))
  remove_indexes(old_agent, tuple_id)
end

local new_agent = normalize_agent(cjson.decode(new_json))
persist_agent(agent_key, new_agent, tuple_id, 0)
add_indexes(new_agent, tuple_id)
if write_events_enabled(write_events) then
  append_event(tuple_id, new_agent.kind, 'CREATED', timestamp, agent_id, new_agent.trace_id or '', new_agent.version or 0, event_data, namespace_value(new_agent))
end

return cjson.encode({ok=true, code='CREATED'})
`

const updateScriptSource = luaHelpers + `
local agent_key = KEYS[1]
local new_json = ARGV[1]
local tuple_id = ARGV[2]
local timestamp = ARGV[3]
local agent_id = ARGV[4]
local event_data = ARGV[5]
local write_events = ARGV[6]

local current, _ = load_agent(agent_key)
if not current then
  return cjson.encode({ok=false, code='NOT_FOUND'})
end
remove_indexes(current, tuple_id)
local next_agent = normalize_agent(cjson.decode(new_json))
persist_agent(agent_key, next_agent, tuple_id, 0)
add_indexes(next_agent, tuple_id)
if write_events_enabled(write_events) then
  append_event(tuple_id, next_agent.kind, 'UPDATED', timestamp, agent_id, next_agent.trace_id or '', next_agent.version or 0, event_data, namespace_value(next_agent))
end

return cjson.encode({ok=true, code='UPDATED', agent=new_json})
`

const claimScriptSource = luaHelpers + `
local agent_key = KEYS[1]
local tuple_id = ARGV[1]
local agent_id = ARGV[2]
local lease_token = ARGV[3]
local timestamp = ARGV[4]
local lease_until_ms = ARGV[5]
local lease_until = ARGV[6]
local write_events = ARGV[7]

local current, _ = load_agent(agent_key)
if not current then
  return cjson.encode({ok=false, code='NOT_FOUND'})
end
if tostring(current.status or '') ~= 'NEW' then
  return cjson.encode({ok=false, code='STATUS_CHANGED'})
end
remove_indexes(current, tuple_id)
current.status = 'IN_PROGRESS'
current.owner = agent_id
current.owner_time = timestamp
current.updated_at = timestamp
current.lease_token = lease_token
current.lease_until = lease_until
current.version = (current.version or 0) + 1
current.metadata = ensure_object(current.metadata)
current.metadata['__lease_until_ms'] = tostring(lease_until_ms)
local next_json = cjson.encode(current)
next_json = persist_agent(agent_key, current, tuple_id, 0)
add_indexes(current, tuple_id)
local event_data = cjson.encode({agent_id=agent_id})
if write_events_enabled(write_events) then
  append_event(tuple_id, current.kind, 'CLAIMED', timestamp, agent_id, current.trace_id or '', current.version or 0, event_data, namespace_value(current))
end
return cjson.encode({ok=true, code='CLAIMED', agent=next_json})
`

const queueTakeScriptSource = luaHelpers + `
local queue_key_name = KEYS[1]
local expected_namespace = ARGV[1]
local expected_queue = ARGV[2]
local required_kind = ARGV[3]
local required_owner = ARGV[4]
local agent_id = ARGV[5]
local lease_token = ARGV[6]
local timestamp = ARGV[7]
local now_ms = tonumber(ARGV[8]) or 0
local default_lease_ms = tonumber(ARGV[9]) or 0
local write_events = ARGV[10]

local candidates = redis.call('ZRANGE', queue_key_name, 0, 15)
for _, tuple_id in ipairs(candidates) do
  local hot = load_hot(tuple_id)
  local agent_key_name = 'agent:' .. part(tuple_id)
  local current, _ = load_agent(agent_key_name)
  if not hot or not current then
    redis.call('ZREM', queue_key_name, tuple_id)
  elseif tostring(hot.status or '') ~= 'NEW' then
    redis.call('ZREM', queue_key_name, tuple_id)
  elseif tostring(hot.queue or '') ~= expected_queue or tostring(hot.namespace_id or '') ~= expected_namespace then
    redis.call('ZREM', queue_key_name, tuple_id)
  elseif required_kind ~= '' and tostring(hot.kind or '') ~= required_kind then
    return cjson.encode({ok=false, code='NOT_QUEUE_HEAD'})
  elseif required_owner ~= '' and tostring(hot.owner or '') ~= required_owner then
    return cjson.encode({ok=false, code='NOT_QUEUE_HEAD'})
  else
    remove_indexes(current, tuple_id)
    local lease_ms = tonumber(hot.lease_duration_ms or 0)
    if lease_ms == nil or lease_ms <= 0 then
      lease_ms = default_lease_ms
    end
    local lease_until_ms = now_ms + lease_ms
    current.status = 'IN_PROGRESS'
    current.owner = agent_id
    current.owner_time = timestamp
    current.updated_at = timestamp
    current.lease_token = lease_token
    current.lease_until = nil
    current.version = (current.version or 0) + 1
    current.metadata = ensure_object(current.metadata)
    current.metadata['__lease_until_ms'] = tostring(lease_until_ms)
    local next_json = persist_agent(agent_key_name, current, tuple_id, default_lease_ms)
    add_indexes(current, tuple_id)
    if write_events_enabled(write_events) then
      append_event(tuple_id, current.kind, 'CLAIMED', timestamp, agent_id, current.trace_id or '', current.version or 0, cjson.encode({agent_id=agent_id}), namespace_value(current))
    end
    return cjson.encode({ok=true, code='CLAIMED', agent=next_json})
  end
end

return cjson.encode({ok=true, code='EMPTY'})
`

const queueBatchTakeScriptSource = luaHelpers + `
local queue_key_name = KEYS[1]
local expected_namespace = ARGV[1]
local expected_queue = ARGV[2]
local required_kind = ARGV[3]
local required_owner = ARGV[4]
local agent_id = ARGV[5]
local lease_token_prefix = ARGV[6]
local timestamp = ARGV[7]
local now_ms = tonumber(ARGV[8]) or 0
local default_lease_ms = tonumber(ARGV[9]) or 0
local limit = tonumber(ARGV[10]) or 1
local write_events = ARGV[11]

local claimed = {}
for index = 1, limit do
  local claimed_this_round = false
  local candidates = redis.call('ZRANGE', queue_key_name, 0, 15)
  for _, tuple_id in ipairs(candidates) do
    local hot = load_hot(tuple_id)
    local agent_key_name = 'agent:' .. part(tuple_id)
    local current, _ = load_agent(agent_key_name)
    if not hot or not current then
      redis.call('ZREM', queue_key_name, tuple_id)
    elseif tostring(hot.status or '') ~= 'NEW' then
      redis.call('ZREM', queue_key_name, tuple_id)
    elseif tostring(hot.queue or '') ~= expected_queue or tostring(hot.namespace_id or '') ~= expected_namespace then
      redis.call('ZREM', queue_key_name, tuple_id)
    elseif required_kind ~= '' and tostring(hot.kind or '') ~= required_kind then
      return cjson.encode({ok=false, code='NOT_QUEUE_HEAD', agents=claimed})
    elseif required_owner ~= '' and tostring(hot.owner or '') ~= required_owner then
      return cjson.encode({ok=false, code='NOT_QUEUE_HEAD', agents=claimed})
    else
      remove_indexes(current, tuple_id)
      local lease_ms = tonumber(hot.lease_duration_ms or 0)
      if lease_ms == nil or lease_ms <= 0 then
        lease_ms = default_lease_ms
      end
      local lease_until_ms = now_ms + lease_ms
      current.status = 'IN_PROGRESS'
      current.owner = agent_id
      current.owner_time = timestamp
      current.updated_at = timestamp
      current.lease_token = lease_token_prefix .. ':' .. tostring(index) .. ':' .. tuple_id
      current.lease_until = nil
      current.version = (current.version or 0) + 1
      current.metadata = ensure_object(current.metadata)
      current.metadata['__lease_until_ms'] = tostring(lease_until_ms)
      local next_json = persist_agent(agent_key_name, current, tuple_id, default_lease_ms)
      add_indexes(current, tuple_id)
      if write_events_enabled(write_events) then
        append_event(tuple_id, current.kind, 'CLAIMED', timestamp, agent_id, current.trace_id or '', current.version or 0, cjson.encode({agent_id=agent_id}), namespace_value(current))
      end
      table.insert(claimed, next_json)
      claimed_this_round = true
      break
    end
  end
  if not claimed_this_round then
    break
  end
end

if #claimed == 0 then
  return cjson.encode({ok=true, code='EMPTY', agents=claimed})
end
return cjson.encode({ok=true, code='BATCH_CLAIMED', agents=claimed})
`

const completeScriptSource = luaHelpers + `
local agent_key = KEYS[1]
local tuple_id = ARGV[1]
local agent_id = ARGV[2]
local lease_token = ARGV[3]
local result_json = ARGV[4]
local timestamp = ARGV[5]
local write_events = ARGV[6]

local current, _ = load_agent(agent_key)
if not current then
  return cjson.encode({ok=false, code='NOT_FOUND'})
end
local metadata = ensure_object(current.metadata)
current.metadata = metadata
if tostring(current.status or '') == 'COMPLETED' then
  if tostring(metadata['_completed_by_token'] or '') == lease_token then
    return cjson.encode({ok=true, code='ALREADY_COMPLETED', agent=cjson.encode(current)})
  end
  return cjson.encode({ok=false, code='STALE_TOKEN'})
end
if tostring(current.status or '') ~= 'IN_PROGRESS' then
  return cjson.encode({ok=false, code='INVALID_STATUS'})
end
if tostring(current.owner or '') ~= agent_id then
  return cjson.encode({ok=false, code='OWNER_MISMATCH'})
end
if tostring(current.lease_token or '') ~= lease_token then
  return cjson.encode({ok=false, code='LEASE_TOKEN_MISMATCH'})
end

remove_indexes(current, tuple_id)
local decoded = cjson.decode(result_json)
current.payload = ensure_object(current.payload)
current.payload['result'] = decoded
current.status = 'COMPLETED'
current.owner = ''
current.owner_time = nil
current.lease_token = ''
current.lease_until = nil
current.updated_at = timestamp
current.version = (current.version or 0) + 1
current.metadata = ensure_object(current.metadata)
current.metadata['_completed_by'] = agent_id
current.metadata['_completed_by_token'] = lease_token
current.metadata['_completed_at'] = timestamp
current.metadata['__lease_until_ms'] = nil
local next_json = cjson.encode(current)
next_json = persist_agent(agent_key, current, tuple_id, 0)
add_indexes(current, tuple_id)
if write_events_enabled(write_events) then
  append_event(tuple_id, current.kind, 'COMPLETED', timestamp, agent_id, current.trace_id or '', current.version or 0, result_json, namespace_value(current))
end
return cjson.encode({ok=true, code='COMPLETED', agent=next_json})
`

const releaseScriptSource = luaHelpers + `
local agent_key = KEYS[1]
local tuple_id = ARGV[1]
local agent_id = ARGV[2]
local lease_token = ARGV[3]
local reason = ARGV[4]
local timestamp = ARGV[5]
local write_events = ARGV[6]

local current, _ = load_agent(agent_key)
if not current then
  return cjson.encode({ok=false, code='NOT_FOUND'})
end
if tostring(current.status or '') ~= 'IN_PROGRESS' then
  return cjson.encode({ok=false, code='INVALID_STATUS'})
end
if tostring(current.owner or '') ~= agent_id then
  return cjson.encode({ok=false, code='OWNER_MISMATCH'})
end
if tostring(current.lease_token or '') ~= lease_token then
  return cjson.encode({ok=false, code='LEASE_TOKEN_MISMATCH'})
end

local previous_owner = tostring(current.owner or '')
remove_indexes(current, tuple_id)
current.status = 'NEW'
current.owner = ''
current.owner_time = nil
current.lease_token = ''
current.lease_until = nil
current.updated_at = timestamp
current.version = (current.version or 0) + 1
current.metadata = ensure_object(current.metadata)
current.metadata['__lease_until_ms'] = nil
local next_json = cjson.encode(current)
next_json = persist_agent(agent_key, current, tuple_id, 0)
add_indexes(current, tuple_id)

local event_data = {previous_owner=previous_owner}
if reason ~= nil and reason ~= '' then
  event_data['reap_reason'] = reason
end
if write_events_enabled(write_events) then
  append_event(tuple_id, current.kind, 'RELEASED', timestamp, agent_id, current.trace_id or '', current.version or 0, cjson.encode(event_data), namespace_value(current))
end
return cjson.encode({ok=true, code='RELEASED', agent=next_json})
`

const renewLeaseScriptSource = luaHelpers + `
local agent_key = KEYS[1]
local tuple_id = ARGV[1]
local agent_id = ARGV[2]
local lease_token = ARGV[3]
local timestamp = ARGV[4]
local lease_until_ms = ARGV[5]
local lease_until = ARGV[6]
local write_events = ARGV[7]

local current, _ = load_agent(agent_key)
if not current then
  return cjson.encode({ok=false, code='NOT_FOUND'})
end
if tostring(current.status or '') ~= 'IN_PROGRESS' then
  return cjson.encode({ok=false, code='INVALID_STATUS'})
end
if tostring(current.owner or '') ~= agent_id then
  return cjson.encode({ok=false, code='OWNER_MISMATCH'})
end
if tostring(current.lease_token or '') ~= lease_token then
  return cjson.encode({ok=false, code='LEASE_TOKEN_MISMATCH'})
end

remove_indexes(current, tuple_id)
current.owner_time = timestamp
current.updated_at = timestamp
current.lease_until = lease_until
current.metadata = ensure_object(current.metadata)
current.metadata['__lease_until_ms'] = tostring(lease_until_ms)
local next_json = cjson.encode(current)
next_json = persist_agent(agent_key, current, tuple_id, 0)
add_indexes(current, tuple_id)
return cjson.encode({ok=true, code='RENEWED', agent=next_json})
`

const completeAndOutScriptSource = luaHelpers + `
local agent_key = KEYS[1]
local tuple_id = ARGV[1]
local agent_id = ARGV[2]
local lease_token = ARGV[3]
local result_json = ARGV[4]
local outputs_json = ARGV[5]
local timestamp = ARGV[6]
local write_events = ARGV[7]
local slim_events = ARGV[8]

local current, _ = load_agent(agent_key)
if not current then
  return cjson.encode({ok=false, code='NOT_FOUND'})
end
if tostring(current.status or '') ~= 'IN_PROGRESS' then
  return cjson.encode({ok=false, code='INVALID_STATUS'})
end
if tostring(current.owner or '') ~= agent_id then
  return cjson.encode({ok=false, code='OWNER_MISMATCH'})
end
if tostring(current.lease_token or '') ~= lease_token then
  return cjson.encode({ok=false, code='LEASE_TOKEN_MISMATCH'})
end

remove_indexes(current, tuple_id)
local decoded = cjson.decode(result_json)
current.payload = ensure_object(current.payload)
current.payload['result'] = decoded
current.status = 'COMPLETED'
current.owner = ''
current.owner_time = nil
current.lease_token = ''
current.lease_until = nil
current.updated_at = timestamp
current.version = (current.version or 0) + 1
current.metadata = ensure_object(current.metadata)
current.metadata['_completed_by'] = agent_id
current.metadata['_completed_by_token'] = lease_token
current.metadata['_completed_at'] = timestamp
current.metadata['__lease_until_ms'] = nil
local completed_json = cjson.encode(current)
completed_json = persist_agent(agent_key, current, tuple_id, 0)
add_indexes(current, tuple_id)
if write_events_enabled(write_events) then
  append_event(tuple_id, current.kind, 'COMPLETED', timestamp, agent_id, current.trace_id or '', current.version or 0, result_json, namespace_value(current))
end

local created = {}
local outputs = cjson.decode(outputs_json)
for _, output in ipairs(outputs) do
  output = normalize_agent(output)
  local output_key = 'agent:' .. part(output.id)
  local old = redis.call('GET', output_key)
  if old then
    local old_agent = normalize_agent(cjson.decode(old))
    remove_indexes(old_agent, output.id)
  end
  local output_json = cjson.encode(output)
  output_json = persist_agent(output_key, output, output.id, 0)
  add_indexes(output, output.id)
  local event_data = nil
  if slim_events == '1' or slim_events == 'true' then
    event_data = cjson.encode({tuple_id=output.id, kind=output.kind, status=output.status, parent_id=tuple_id})
  else
    event_data = cjson.encode({agent=output, parent_id=tuple_id})
  end
  if write_events_enabled(write_events) then
    append_event(output.id, output.kind, 'CREATED', timestamp, agent_id, output.trace_id or '', output.version or 0, event_data, namespace_value(output))
  end
  table.insert(created, output_json)
end

return cjson.encode({ok=true, code='COMPLETED_AND_OUT', agent=completed_json, created=created})
`

const reapScriptSource = luaHelpers + `
local timestamp = ARGV[1]
local now_ms = tonumber(ARGV[2])
local batch = tonumber(ARGV[3])
local write_events = ARGV[4]

local expired = redis.call('ZRANGEBYSCORE', 'lease:by_until', '-inf', tostring(now_ms), 'LIMIT', 0, batch)
local count = 0
for _, tuple_id in ipairs(expired) do
  local agent_key = 'agent:' .. part(tuple_id)
  local current, _ = load_agent(agent_key)
  if current and tostring(current.status or '') == 'IN_PROGRESS' then
    local previous_owner = tostring(current.owner or '')
    remove_indexes(current, tuple_id)
    current.status = 'NEW'
    current.owner = ''
    current.owner_time = nil
    current.lease_token = ''
    current.lease_until = nil
    current.updated_at = timestamp
    current.version = (current.version or 0) + 1
    current.metadata = ensure_object(current.metadata)
    current.metadata['__lease_until_ms'] = nil
    persist_agent(agent_key, current, tuple_id, 0)
    add_indexes(current, tuple_id)
    if write_events_enabled(write_events) then
      append_event(tuple_id, current.kind, 'RELEASED', timestamp, 'reaper', current.trace_id or '', current.version or 0, cjson.encode({reap_reason='lease_expired', previous_owner=previous_owner}), namespace_value(current))
    end
    count = count + 1
  else
    redis.call('ZREM', 'lease:by_until', tuple_id)
  end
end
return cjson.encode({ok=true, count=count})
`
