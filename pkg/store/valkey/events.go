package valkey

import (
	"context"
	"fmt"
	"strconv"
	"time"

	json "github.com/goccy/go-json"
	vk "github.com/valkey-io/valkey-go"
	"go.uber.org/zap"

	"github.com/urobora-ai/agentspaces/pkg/agent"
	"github.com/urobora-ai/agentspaces/pkg/metrics"
)

type pendingValkeyEvent struct {
	event       *agent.Event
	namespaceID string
	shardID     string
}

func createdEventData(t *agent.Agent, slim bool) map[string]interface{} {
	if t == nil {
		return map[string]interface{}{}
	}
	if slim {
		return map[string]interface{}{
			"tuple_id": t.ID,
			"kind":     t.Kind,
			"status":   t.Status,
		}
	}
	return map[string]interface{}{
		"agent": t,
	}
}

func (s *ValkeyStore) enqueueEvent(ev *agent.Event, namespaceID string, shardID string) {
	if ev == nil {
		return
	}
	pending := &pendingValkeyEvent{
		event:       ev,
		namespaceID: namespaceID,
		shardID:     shardID,
	}
	select {
	case s.eventQueue <- pending:
	default:
		metrics.RecordRouteActivity("valkey_event_sync_fallback", s.storeLabel())
		shard, ok := s.byShardID[shardID]
		if !ok {
			return
		}
		if err := s.appendEventToStreams(context.Background(), shard, ev, namespaceID); err != nil {
			metrics.RecordStoreError(s.storeLabel(), "event_write")
			s.logger.Warn("valkey deferred event sync fallback failed", zap.Error(err))
		}
	}
}

func (s *ValkeyStore) eventWriterLoop(batchSize int) {
	defer close(s.eventDone)
	if batchSize <= 0 {
		batchSize = 100
	}
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	buffer := make([]*pendingValkeyEvent, 0, batchSize)
	flush := func() {
		if len(buffer) == 0 {
			return
		}
		s.flushDeferredEvents(buffer)
		buffer = buffer[:0]
	}

	for {
		select {
		case <-s.ctx.Done():
			for {
				select {
				case pending := <-s.eventQueue:
					if pending != nil {
						buffer = append(buffer, pending)
					}
				default:
					flush()
					return
				}
			}
		case pending := <-s.eventQueue:
			if pending == nil {
				continue
			}
			buffer = append(buffer, pending)
			if len(buffer) >= batchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

func (s *ValkeyStore) flushDeferredEvents(events []*pendingValkeyEvent) {
	for _, pending := range events {
		if pending == nil || pending.event == nil {
			continue
		}
		shard, ok := s.byShardID[pending.shardID]
		if !ok {
			continue
		}
		if err := s.appendEventToStreams(context.Background(), shard, pending.event, pending.namespaceID); err != nil {
			metrics.RecordStoreError(s.storeLabel(), "event_write")
			s.logger.Warn("valkey deferred event write failed", zap.Error(err))
		}
	}
}

func (s *ValkeyStore) appendEventToStreams(ctx context.Context, shard *shardHandle, ev *agent.Event, namespaceID string) error {
	if shard == nil || ev == nil {
		return nil
	}
	dataJSON, err := json.Marshal(ev.Data)
	if err != nil {
		return err
	}
	fields := []string{
		"MAXLEN", "~", "500000", "*",
		"tuple_id", ev.TupleID,
		"type", string(ev.Type),
		"kind", ev.Kind,
		"timestamp", ev.Timestamp.UTC().Format(time.RFC3339Nano),
		"agent_id", ev.AgentID,
		"trace_id", ev.TraceID,
		"version", strconv.FormatInt(ev.Version, 10),
		"data", string(dataJSON),
		"namespace_id", namespaceID,
	}
	cmds := []vk.Completed{
		shard.client.B().Arbitrary("XADD").Keys(globalEventsKey()).Args(fields...).Build(),
		shard.client.B().Arbitrary("XADD").Keys(agentEventsStreamKey(ev.TupleID)).Args(
			append([]string{"MAXLEN", "~", "200000", "*"}, fields[4:]...)...,
		).Build(),
	}
	if ev.TraceID != "" {
		cmds = append(cmds,
			shard.client.B().Arbitrary("XADD").Keys(traceEventsStreamKey(ev.TraceID)).Args(
				append([]string{"MAXLEN", "~", "200000", "*"}, fields[4:]...)...,
			).Build(),
		)
	}
	for _, resp := range shard.client.DoMulti(defaultContext(ctx), cmds...) {
		if err := resp.Error(); err != nil {
			return fmt.Errorf("append event: %w", err)
		}
	}
	return nil
}
