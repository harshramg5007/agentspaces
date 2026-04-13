package store

import "github.com/urobora-ai/agentspaces/pkg/agent"

var (
	_ agent.UpdateAndGetProvider   = (*PostgresStore)(nil)
	_ agent.CompleteAndGetProvider = (*PostgresStore)(nil)
	_ agent.ReleaseAndGetProvider  = (*PostgresStore)(nil)

	_ agent.UpdateAndGetProvider   = (*SQLiteStore)(nil)
	_ agent.CompleteAndGetProvider = (*SQLiteStore)(nil)
	_ agent.ReleaseAndGetProvider  = (*SQLiteStore)(nil)

	_ agent.UpdateAndGetProvider   = (*ValkeyStore)(nil)
	_ agent.CompleteAndGetProvider = (*ValkeyStore)(nil)
	_ agent.ReleaseAndGetProvider  = (*ValkeyStore)(nil)
)
