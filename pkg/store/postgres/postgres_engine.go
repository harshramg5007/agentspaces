package postgres

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/store/internal/common"
	"github.com/urobora-ai/agentspaces/pkg/store/internal/core"
)

const defaultPostgresPollInterval = 100 * time.Millisecond
const defaultPostgresLeaseReapInterval = 2 * time.Second
const postgresWakeupChannel = "agent_wakeup"

const agentSelectColumns = `
	id, kind, status, payload, tags, owner, parent_id, trace_id, namespace_id, shard_id,
	ttl_ms, created_at, updated_at, owner_time, lease_token, lease_until, version, metadata
`

const eventSelectColumns = `
	id, tuple_id, type, kind, timestamp, agent_id, trace_id, version, data, namespace_id
`

const telemetrySelectColumns = `
	trace_id, span_id, parent_span_id, tuple_id, kind, name, ts_start, ts_end, status,
	attrs, metric_key, metric_value_bool, metric_value_num, metric_value_str, namespace_id
`

// pendingEvent holds an event to be written asynchronously.
type pendingEvent struct {
	event       *agent.Event
	namespaceID string
	shardID     string
}

// PostgresStore implements the AgentSpace interface using PostgreSQL.
type PostgresStore struct {
	pool              *pgxpool.Pool
	logger            *zap.Logger
	ctx               context.Context
	cancel            context.CancelFunc
	subscriptions     *common.SubscriptionRegistry
	runtime           *core.StoreRuntime
	pollInterval      time.Duration
	leaseDuration     time.Duration
	leaseReapInterval time.Duration
	schemaMode        string
	slimEvents        bool
	queueClaimMode    QueueClaimMode
	shardID           string
	connString        string
	listenerConn      *pgx.Conn
	listenerDone      chan struct{}
	wakeupMu          sync.RWMutex
	wakeupCh          chan struct{}
	deferEvents       bool
	eventQueue        chan *pendingEvent
	eventDone         chan struct{}
	prefetch          *prefetchRegistry
}

func NewPostgresStore(ctx context.Context, cfg *PostgresConfig, logger *zap.Logger) (*PostgresStore, error) {
	if cfg == nil {
		return nil, fmt.Errorf("postgres config is nil")
	}

	queueClaimMode, err := ParseQueueClaimMode(cfg.QueueClaimMode)
	if err != nil {
		return nil, err
	}

	connStr, err := buildPostgresConnString(cfg)
	if err != nil {
		return nil, err
	}

	poolCfg, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to parse postgres config: %w", err)
	}
	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = int32(cfg.MaxConns)
	}
	if cfg.MinConns > 0 {
		poolCfg.MinConns = int32(cfg.MinConns)
	}

	if cfg.MaxConnAge != "" {
		if d, err := time.ParseDuration(cfg.MaxConnAge); err == nil {
			poolCfg.MaxConnLifetime = d
		}
	}
	if cfg.MaxIdleTime != "" {
		if d, err := time.ParseDuration(cfg.MaxIdleTime); err == nil {
			poolCfg.MaxConnIdleTime = d
		}
	}
	poolCfg.HealthCheckPeriod = 30 * time.Second
	poolCfg.MaxConnLifetimeJitter = 5 * time.Minute

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create postgres pool: %w", err)
	}

	eventBatchSize := cfg.EventBatchSize
	if eventBatchSize <= 0 {
		eventBatchSize = 100
	}

	storeCtx, cancel := context.WithCancel(ctx)
	store := &PostgresStore{
		pool:              pool,
		logger:            logger,
		ctx:               storeCtx,
		cancel:            cancel,
		subscriptions:     common.NewSubscriptionRegistry(),
		pollInterval:      defaultPostgresPollInterval,
		leaseDuration:     agent.DefaultLeaseOptions().LeaseDuration,
		leaseReapInterval: defaultPostgresLeaseReapInterval,
		schemaMode:        cfg.SchemaMode,
		slimEvents:        cfg.SlimEvents,
		queueClaimMode:    queueClaimMode,
		shardID:           cfg.ShardID,
		connString:        connStr,
		listenerDone:      make(chan struct{}),
		wakeupCh:          make(chan struct{}),
		deferEvents:       cfg.DeferEvents,
		eventQueue:        make(chan *pendingEvent, 10000),
		eventDone:         make(chan struct{}),
	}
	store.runtime = core.NewStoreRuntime(store)

	if err := store.initSchema(storeCtx); err != nil {
		pool.Close()
		return nil, err
	}
	if err := store.startWakeupListener(storeCtx); err != nil {
		pool.Close()
		return nil, err
	}

	// Start lease reaper
	go store.leaseReaperLoop()

	// Start deferred event writer
	if store.deferEvents {
		go store.eventWriterLoop(eventBatchSize)
	}

	// Start prefetch registry for parallel mode
	if cfg.PrefetchEnabled && queueClaimMode == QueueClaimModeParallel {
		store.prefetch = newPrefetchRegistry(storeCtx, pool, logger, cfg.PrefetchBufferSize, cfg.PrefetchBatchSize)
	}

	return store, nil
}

// Close closes the store.
func (s *PostgresStore) Close() error {
	s.cancel()
	if s.prefetch != nil {
		s.prefetch.close()
	}
	if s.deferEvents {
		<-s.eventDone // wait for event writer to drain
	}
	if s.listenerConn != nil {
		<-s.listenerDone
		_ = s.listenerConn.Close(context.Background())
	}
	s.pool.Close()
	return nil
}

// enqueueEvent sends an event to the deferred writer. If the queue is full,
// the event is written synchronously as a fallback.
func (s *PostgresStore) enqueueEvent(ev *agent.Event, namespaceID string) {
	pe := &pendingEvent{event: ev, namespaceID: namespaceID, shardID: s.shardID}
	select {
	case s.eventQueue <- pe:
	default:
		// Queue full — write synchronously so we don't lose the event.
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		batch := &pgx.Batch{}
		if err := queueInsertEvent(batch, ev, namespaceID, s.shardID); err != nil {
			s.logger.Warn("deferred event sync fallback marshal error", zap.Error(err))
			return
		}
		results := s.pool.SendBatch(ctx, batch)
		if _, err := results.Exec(); err != nil {
			s.logger.Warn("deferred event sync fallback write error", zap.Error(err))
		}
		results.Close()
	}
}

// eventWriterLoop drains the event queue in batches.
func (s *PostgresStore) eventWriterLoop(batchSize int) {
	defer close(s.eventDone)
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	buf := make([]*pendingEvent, 0, batchSize)
	for {
		select {
		case pe, ok := <-s.eventQueue:
			if !ok {
				// Channel closed (shouldn't happen, but be safe).
				s.flushEvents(buf)
				return
			}
			buf = append(buf, pe)
			if len(buf) >= batchSize {
				s.flushEvents(buf)
				buf = buf[:0]
			}
		case <-ticker.C:
			if len(buf) > 0 {
				s.flushEvents(buf)
				buf = buf[:0]
			}
		case <-s.ctx.Done():
			// Drain remaining events before exit.
		drain:
			for {
				select {
				case pe := <-s.eventQueue:
					buf = append(buf, pe)
				default:
					break drain
				}
			}
			s.flushEvents(buf)
			return
		}
	}
}

// flushEvents writes a batch of pending events to Postgres.
func (s *PostgresStore) flushEvents(pending []*pendingEvent) {
	if len(pending) == 0 {
		return
	}
	batch := &pgx.Batch{}
	for _, pe := range pending {
		if err := queueInsertEvent(batch, pe.event, pe.namespaceID, pe.shardID); err != nil {
			s.logger.Warn("deferred event marshal error", zap.Error(err))
			continue
		}
	}
	if batch.Len() == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	results := s.pool.SendBatch(ctx, batch)
	for i := 0; i < batch.Len(); i++ {
		if _, err := results.Exec(); err != nil {
			s.logger.Warn("deferred event batch write error", zap.Error(err))
		}
	}
	results.Close()
}

// Health checks whether the Postgres backend is reachable.
func (s *PostgresStore) Health(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	var one int
	if err := s.pool.QueryRow(ctx, "select 1").Scan(&one); err != nil {
		return err
	}
	if one != 1 {
		return fmt.Errorf("unexpected health query result: %d", one)
	}
	return nil
}

// Out writes a agent to the space.

func buildPostgresConnString(cfg *PostgresConfig) (string, error) {
	if cfg.Host == "" {
		return "", fmt.Errorf("postgres host is required")
	}
	if cfg.Port == 0 {
		return "", fmt.Errorf("postgres port is required")
	}
	if cfg.User == "" {
		return "", fmt.Errorf("postgres user is required")
	}
	if cfg.Database == "" {
		return "", fmt.Errorf("postgres database is required")
	}
	sslMode := cfg.SSLMode
	if sslMode == "" {
		sslMode = "disable"
	}

	u := &url.URL{
		Scheme: "postgres",
		User:   url.UserPassword(cfg.User, cfg.Password),
		Host:   fmt.Sprintf("%s:%d", cfg.Host, cfg.Port),
		Path:   cfg.Database,
	}
	q := u.Query()
	q.Set("sslmode", sslMode)
	u.RawQuery = q.Encode()
	return u.String(), nil
}
