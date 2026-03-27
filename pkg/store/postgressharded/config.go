package postgressharded

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"sort"
	"strings"

	postgresbackend "github.com/urobora-ai/agentspaces/pkg/store/postgres"
)

type Config struct {
	ShardMapFile          string `json:"shard_map_file"`
	NodeID                string `json:"node_id"`
	AdvertiseAddr         string `json:"advertise_addr"`
	ShardMapRefreshPeriod string `json:"shard_map_refresh_period"`
	SchemaMode            string `json:"schema_mode"`
	MaxConns              int    `json:"max_conns"`
	MinConns              int    `json:"min_conns"`
	MaxConnAge            string `json:"max_conn_age"`
	MaxIdleTime           string `json:"max_idle_time"`
	SlimEvents            bool   `json:"slim_events"`
	QueueClaimMode        string `json:"queue_claim_mode"`
	DeferEvents           bool   `json:"defer_events"`
	EventBatchSize        int    `json:"event_batch_size"`
	PrefetchEnabled       bool   `json:"prefetch_enabled"`
	PrefetchBufferSize    int    `json:"prefetch_buffer_size"`
	PrefetchBatchSize     int    `json:"prefetch_batch_size"`
}

type ShardMap struct {
	Version int64       `json:"version"`
	Epoch   int64       `json:"epoch,omitempty"`
	Shards  []ShardSpec `json:"shards"`
}

type ShardSpec struct {
	ShardID       string   `json:"shard_id"`
	WriterDSN     string   `json:"writer_dsn"`
	ReplicaDSNs   []string `json:"replica_dsns,omitempty"`
	OwnerNodeID   string   `json:"owner_node_id,omitempty"`
	AdvertiseAddr string   `json:"advertise_addr,omitempty"`
}

func (m *ShardMap) normalize() error {
	if m == nil {
		return fmt.Errorf("shard map is nil")
	}
	if len(m.Shards) == 0 {
		return fmt.Errorf("shard map contains no shards")
	}
	seen := make(map[string]struct{}, len(m.Shards))
	for i := range m.Shards {
		m.Shards[i].ShardID = strings.TrimSpace(m.Shards[i].ShardID)
		m.Shards[i].WriterDSN = strings.TrimSpace(m.Shards[i].WriterDSN)
		m.Shards[i].OwnerNodeID = strings.TrimSpace(m.Shards[i].OwnerNodeID)
		m.Shards[i].AdvertiseAddr = strings.TrimSpace(m.Shards[i].AdvertiseAddr)
		if m.Shards[i].ShardID == "" {
			return fmt.Errorf("shard[%d] missing shard_id", i)
		}
		if m.Shards[i].WriterDSN == "" {
			return fmt.Errorf("shard %q missing writer_dsn", m.Shards[i].ShardID)
		}
		if _, ok := seen[m.Shards[i].ShardID]; ok {
			return fmt.Errorf("duplicate shard_id %q", m.Shards[i].ShardID)
		}
		seen[m.Shards[i].ShardID] = struct{}{}
	}
	sort.SliceStable(m.Shards, func(i, j int) bool {
		return m.Shards[i].ShardID < m.Shards[j].ShardID
	})
	return nil
}

func loadShardMap(path string) (*ShardMap, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read shard map %q: %w", path, err)
	}
	var shardMap ShardMap
	if err := json.Unmarshal(raw, &shardMap); err != nil {
		return nil, fmt.Errorf("decode shard map %q: %w", path, err)
	}
	if err := shardMap.normalize(); err != nil {
		return nil, fmt.Errorf("invalid shard map %q: %w", path, err)
	}
	return &shardMap, nil
}

func postgresConfigFromShardSpec(cfg *Config, spec ShardSpec) (*postgresbackend.PostgresConfig, error) {
	u, err := url.Parse(spec.WriterDSN)
	if err != nil {
		return nil, fmt.Errorf("parse writer_dsn for shard %q: %w", spec.ShardID, err)
	}
	password, _ := u.User.Password()
	port := 5432
	if rawPort := u.Port(); rawPort != "" {
		fmt.Sscanf(rawPort, "%d", &port)
	}
	sslMode := u.Query().Get("sslmode")
	if sslMode == "" {
		sslMode = "disable"
	}
	database := strings.TrimPrefix(u.Path, "/")
	if database == "" {
		return nil, fmt.Errorf("shard %q writer_dsn missing database name", spec.ShardID)
	}
	user := ""
	if u.User != nil {
		user = u.User.Username()
	}
	if user == "" {
		return nil, fmt.Errorf("shard %q writer_dsn missing user", spec.ShardID)
	}
	if u.Hostname() == "" {
		return nil, fmt.Errorf("shard %q writer_dsn missing hostname", spec.ShardID)
	}
	return &postgresbackend.PostgresConfig{
		Host:           u.Hostname(),
		Port:           port,
		User:           user,
		Password:       password,
		Database:       database,
		ShardID:        spec.ShardID,
		SSLMode:        sslMode,
		SchemaMode:     cfg.SchemaMode,
		MaxConns:       cfg.MaxConns,
		MinConns:       cfg.MinConns,
		MaxConnAge:     cfg.MaxConnAge,
		MaxIdleTime:    cfg.MaxIdleTime,
		SlimEvents:     cfg.SlimEvents,
		QueueClaimMode: cfg.QueueClaimMode,
		DeferEvents:        cfg.DeferEvents,
		EventBatchSize:     cfg.EventBatchSize,
		PrefetchEnabled:    cfg.PrefetchEnabled,
		PrefetchBufferSize: cfg.PrefetchBufferSize,
		PrefetchBatchSize:  cfg.PrefetchBatchSize,
	}, nil
}

func redactDSN(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return ""
	}

	u, err := url.Parse(raw)
	if err != nil {
		return "<redacted>"
	}

	u.User = nil
	query := url.Values{}
	if sslMode := strings.TrimSpace(u.Query().Get("sslmode")); sslMode != "" {
		query.Set("sslmode", sslMode)
	}
	u.RawQuery = query.Encode()
	return u.String()
}
