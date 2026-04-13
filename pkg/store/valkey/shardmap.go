package valkey

import (
	"fmt"
	"os"
	"strings"

	json "github.com/goccy/go-json"
)

type shardMap struct {
	Version int         `json:"version"`
	Epoch   int         `json:"epoch"`
	Shards  []shardSpec `json:"shards"`
}

func loadShardMap(path string) (*shardMap, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return nil, fmt.Errorf("valkey shard map path is empty")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m shardMap
	if err := json.Unmarshal(body, &m); err != nil {
		return nil, err
	}
	if len(m.Shards) == 0 {
		return nil, fmt.Errorf("valkey shard map %q has no shards", path)
	}
	for i := range m.Shards {
		m.Shards[i].ShardID = strings.TrimSpace(m.Shards[i].ShardID)
		m.Shards[i].Addr = strings.TrimSpace(m.Shards[i].Addr)
		m.Shards[i].OwnerNodeID = strings.TrimSpace(m.Shards[i].OwnerNodeID)
		m.Shards[i].AdvertiseAddr = strings.TrimSpace(m.Shards[i].AdvertiseAddr)
		if m.Shards[i].ShardID == "" {
			return nil, fmt.Errorf("valkey shard map %q contains shard with empty shard_id", path)
		}
		if m.Shards[i].Addr == "" {
			return nil, fmt.Errorf("valkey shard map %q contains shard %q with empty addr", path, m.Shards[i].ShardID)
		}
	}
	return &m, nil
}
