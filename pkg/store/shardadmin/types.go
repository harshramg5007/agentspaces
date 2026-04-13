package shardadmin

type ShardHealth struct {
	ShardID       string `json:"shard_id"`
	OwnerNodeID   string `json:"owner_node_id,omitempty"`
	AdvertiseAddr string `json:"advertise_addr,omitempty"`
	Ready         bool   `json:"ready"`
	Error         string `json:"error,omitempty"`
}
