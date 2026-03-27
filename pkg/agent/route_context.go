package agent

import "context"

type routeContextKey string

const shardIDContextKey routeContextKey = "agent.shard_id"

// ContextWithShardID annotates request context with a shard routing hint.
func ContextWithShardID(ctx context.Context, shardID string) context.Context {
	if ctx == nil || shardID == "" {
		return ctx
	}
	return context.WithValue(ctx, shardIDContextKey, shardID)
}

// ShardIDFromContext returns the shard routing hint when present.
func ShardIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if shardID, ok := ctx.Value(shardIDContextKey).(string); ok {
		return shardID
	}
	return ""
}
