package valkey

// ValkeyConfig holds Valkey-specific configuration.
type ValkeyConfig struct {
	Addr              string   `json:"addr"`
	Shards            []string `json:"shards"`
	PublishPubSub     bool     `json:"publish_pubsub"`
	LeaseReapInterval string   `json:"lease_reap_interval"`
	LeaseReapBatch    int      `json:"lease_reap_batch"`
	SlimEvents        bool     `json:"slim_events"`
	DeferEvents       bool     `json:"defer_events"`
	EventBatchSize    int      `json:"event_batch_size"`
	ShardMapFile      string   `json:"shard_map_file"`
	NodeID            string   `json:"node_id"`
	AdvertiseAddr     string   `json:"advertise_addr"`
}

// DefaultValkeyConfig returns default Valkey configuration.
func DefaultValkeyConfig() *ValkeyConfig {
	return &ValkeyConfig{
		Addr:              "localhost:6379",
		Shards:            nil,
		PublishPubSub:     false,
		LeaseReapInterval: "1s",
		LeaseReapBatch:    200,
		SlimEvents:        false,
		DeferEvents:       false,
		EventBatchSize:    100,
		ShardMapFile:      "",
		NodeID:            "",
		AdvertiseAddr:     "",
	}
}
