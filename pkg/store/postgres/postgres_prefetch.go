package postgres

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

const (
	defaultPrefetchBufferSize = 64
	defaultPrefetchBatchSize  = 32
	prefetchPollInterval      = 10 * time.Millisecond
	prefetchIdleTimeout       = 30 * time.Second
	prefetchReaperInterval    = 10 * time.Second
)

// queuePrefetcher pre-selects candidate agent IDs for a single queue.
// Workers receive IDs from the candidates channel and attempt targeted claims.
type queuePrefetcher struct {
	namespaceID string
	queueName   string
	candidates  chan string  // buffered channel of pre-fetched agent IDs
	lastAccess  atomic.Int64 // unix nanos of last Take request
	cancel      context.CancelFunc
	done        chan struct{}
}

// prefetchRegistry manages per-queue prefetcher lifecycle.
type prefetchRegistry struct {
	mu          sync.Mutex
	prefetchers map[string]*queuePrefetcher
	pool        *pgxpool.Pool
	logger      *zap.Logger
	ctx         context.Context
	cancel      context.CancelFunc
	done        chan struct{}
	bufferSize  int
	batchSize   int
}

func newPrefetchRegistry(ctx context.Context, pool *pgxpool.Pool, logger *zap.Logger, bufferSize, batchSize int) *prefetchRegistry {
	if bufferSize <= 0 {
		bufferSize = defaultPrefetchBufferSize
	}
	if batchSize <= 0 {
		batchSize = defaultPrefetchBatchSize
	}
	regCtx, regCancel := context.WithCancel(ctx)
	r := &prefetchRegistry{
		prefetchers: make(map[string]*queuePrefetcher),
		pool:        pool,
		logger:      logger,
		ctx:         regCtx,
		cancel:      regCancel,
		done:        make(chan struct{}),
		bufferSize:  bufferSize,
		batchSize:   batchSize,
	}
	go r.reaperLoop()
	return r
}

func prefetchKey(namespaceID, queue string) string {
	return namespaceID + "|" + queue
}

// getOrStart returns the prefetcher for the given queue, starting one if needed.
func (r *prefetchRegistry) getOrStart(namespaceID, queue string) *queuePrefetcher {
	key := prefetchKey(namespaceID, queue)

	r.mu.Lock()
	defer r.mu.Unlock()

	if pf, ok := r.prefetchers[key]; ok {
		pf.lastAccess.Store(time.Now().UnixNano())
		return pf
	}

	pfCtx, pfCancel := context.WithCancel(r.ctx)
	pf := &queuePrefetcher{
		namespaceID: namespaceID,
		queueName:   queue,
		candidates:  make(chan string, r.bufferSize),
		cancel:      pfCancel,
		done:        make(chan struct{}),
	}
	pf.lastAccess.Store(time.Now().UnixNano())
	r.prefetchers[key] = pf
	go pf.run(pfCtx, r.pool, r.batchSize, r.logger)
	return pf
}

// tryGet returns a candidate ID from the prefetcher buffer, or "" if empty/unavailable.
func (r *prefetchRegistry) tryGet(namespaceID, queue string) string {
	pf := r.getOrStart(namespaceID, queue)
	select {
	case id, ok := <-pf.candidates:
		if !ok {
			return ""
		}
		return id
	default:
		return ""
	}
}

// run is the per-queue prefetch goroutine.
func (pf *queuePrefetcher) run(ctx context.Context, pool *pgxpool.Pool, batchSize int, logger *zap.Logger) {
	defer close(pf.done)
	ticker := time.NewTicker(prefetchPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Skip if buffer is more than half full.
			if len(pf.candidates) >= cap(pf.candidates)/2 {
				continue
			}
			ids, err := pf.fetchCandidateIDs(ctx, pool, batchSize)
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				logger.Debug("prefetch query error",
					zap.String("namespace", pf.namespaceID),
					zap.String("queue", pf.queueName),
					zap.Error(err),
				)
				continue
			}
			for _, id := range ids {
				select {
				case pf.candidates <- id:
				case <-ctx.Done():
					return
				}
			}
		}
	}
}

// fetchCandidateIDs runs a plain SELECT (no locks) to get candidate agent IDs.
func (pf *queuePrefetcher) fetchCandidateIDs(ctx context.Context, pool *pgxpool.Pool, limit int) ([]string, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	rows, err := pool.Query(queryCtx, `
		SELECT id FROM agents
		WHERE status = $1
		  AND namespace_id = $2
		  AND queue = $3
		  AND `+ttlClause()+`
		ORDER BY created_at ASC, id ASC
		LIMIT $4
	`,
		string(agent.StatusNew),
		pf.namespaceID,
		pf.queueName,
		limit,
	)
	if err != nil {
		return nil, fmt.Errorf("prefetch query: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return ids, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

// reaperLoop evicts idle prefetchers.
func (r *prefetchRegistry) reaperLoop() {
	defer close(r.done)
	ticker := time.NewTicker(prefetchReaperInterval)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			r.evictAll()
			return
		case <-ticker.C:
			r.evictIdle()
		}
	}
}

func (r *prefetchRegistry) evictIdle() {
	now := time.Now().UnixNano()
	threshold := prefetchIdleTimeout.Nanoseconds()

	r.mu.Lock()
	defer r.mu.Unlock()

	for key, pf := range r.prefetchers {
		if now-pf.lastAccess.Load() > threshold {
			pf.cancel()
			delete(r.prefetchers, key)
		}
	}
}

func (r *prefetchRegistry) evictAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	for key, pf := range r.prefetchers {
		pf.cancel()
		delete(r.prefetchers, key)
	}
}

func (r *prefetchRegistry) close() {
	r.cancel()
	<-r.done
}
