package postgres

import (
	"context"
	"time"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

type contextBoundStore struct {
	store *PostgresStore
	ctx   context.Context
}

func defaultContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (s *PostgresStore) BindContext(ctx context.Context) agent.AgentSpace {
	return &contextBoundStore{
		store: s,
		ctx:   defaultContext(ctx),
	}
}

func (b *contextBoundStore) Get(id string) (*agent.Agent, error) {
	return b.store.get(b.ctx, id)
}

func (b *contextBoundStore) Out(t *agent.Agent) error {
	return b.store.out(b.ctx, t)
}

func (b *contextBoundStore) In(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return b.store.in(b.ctx, query, timeout)
}

func (b *contextBoundStore) Read(query *agent.Query, timeout time.Duration) (*agent.Agent, error) {
	return b.store.read(b.ctx, query, timeout)
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
	_, err := b.store.completeAndGet(b.ctx, id, agentID, leaseToken, result)
	return err
}

func (b *contextBoundStore) CompleteAndOut(id string, agentID string, leaseToken string, result map[string]interface{}, outputs []*agent.Agent) (*agent.Agent, []*agent.Agent, error) {
	return b.store.CompleteAndOut(id, agentID, leaseToken, result, outputs)
}

func (b *contextBoundStore) Release(id string, agentID string, leaseToken string, reason string) error {
	_, err := b.store.releaseAndGet(b.ctx, id, agentID, leaseToken, reason)
	return err
}

func (b *contextBoundStore) RenewLease(id string, agentID string, leaseToken string, leaseDuration time.Duration) (*agent.Agent, error) {
	return b.store.renewLease(b.ctx, id, agentID, leaseToken, leaseDuration)
}

func (b *contextBoundStore) Subscribe(filter *agent.EventFilter, handler agent.EventHandler) error {
	return b.store.Subscribe(filter, handler)
}

func (b *contextBoundStore) GetEvents(tupleID string) ([]*agent.Event, error) {
	return b.store.getEvents(b.ctx, tupleID)
}

func (b *contextBoundStore) GetGlobalEvents(limit, offset int) ([]*agent.Event, error) {
	return b.store.getGlobalEvents(b.ctx, limit, offset)
}

func (b *contextBoundStore) QueryEvents(query *agent.EventQuery) (*agent.EventPage, error) {
	return b.store.queryEvents(b.ctx, query)
}

func (b *contextBoundStore) GetDAG(rootID string) (*agent.DAG, error) {
	return b.store.getDAG(b.ctx, rootID)
}

func (b *contextBoundStore) Count(query *agent.Query) (int, error) {
	return b.store.count(b.ctx, query)
}

func (b *contextBoundStore) CountByStatus(query *agent.Query) (map[agent.Status]int, error) {
	return b.store.countByStatus(b.ctx, query)
}

func (b *contextBoundStore) Health(ctx context.Context) error {
	if ctx == nil {
		ctx = b.ctx
	}
	return b.store.Health(ctx)
}
