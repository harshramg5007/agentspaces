package migrations

// AppliedStep summarizes one applied or rolled-back migration step.
type AppliedStep struct {
	Version   int64  `json:"version"`
	Name      string `json:"name"`
	Direction string `json:"direction"`
	Checksum  string `json:"checksum"`
}

// Result captures migration command outcomes for CI/rollout jobs.
type Result struct {
	Backend            string       `json:"backend"`
	Action             string       `json:"action"`
	RequestedVersion   int64        `json:"requested_version"`
	PreviousVersion    int64        `json:"previous_version"`
	CurrentVersion     int64        `json:"current_version"`
	TargetVersion      int64        `json:"target_version"`
	MinCompatVersion   int64        `json:"min_compat_version"`
	CompatibilityRange [2]int64     `json:"compatibility_range"`
	Compatible         bool         `json:"compatible"`
	Applied            []AppliedStep `json:"applied"`
	RolledBack         []AppliedStep `json:"rolled_back"`
}
