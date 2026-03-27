package postgres

import (
	"strings"
	"testing"
	"time"
)

func TestBuildDailyPartitionDDL(t *testing.T) {
	from := time.Date(2026, 3, 18, 0, 0, 0, 0, time.UTC)
	until := from.Add(24 * time.Hour)

	got := buildDailyPartitionDDL("agent_events", "agent_events_20260318", from, until)

	if strings.Contains(got, "$1") || strings.Contains(got, "$2") {
		t.Fatalf("expected rendered DDL without bind placeholders, got %q", got)
	}
	if !strings.Contains(got, `CREATE TABLE IF NOT EXISTS "agent_events_20260318" PARTITION OF "agent_events"`) {
		t.Fatalf("expected quoted identifiers in DDL, got %q", got)
	}
	if !strings.Contains(got, `TIMESTAMPTZ '2026-03-18 00:00:00+00:00'`) {
		t.Fatalf("expected UTC lower bound in DDL, got %q", got)
	}
	if !strings.Contains(got, `TIMESTAMPTZ '2026-03-19 00:00:00+00:00'`) {
		t.Fatalf("expected UTC upper bound in DDL, got %q", got)
	}
}

func TestFormatPartitionTimestampUsesUTC(t *testing.T) {
	input := time.Date(2026, 3, 18, 0, 30, 0, 0, time.FixedZone("PDT", -7*60*60))
	got := formatPartitionTimestamp(input)
	if got != "2026-03-18 07:30:00+00:00" {
		t.Fatalf("formatPartitionTimestamp() = %q, want %q", got, "2026-03-18 07:30:00+00:00")
	}
}

func TestTruncateToUTCDate(t *testing.T) {
	input := time.Date(2026, 3, 18, 23, 45, 0, 0, time.FixedZone("PDT", -7*60*60))
	got := truncateToUTCDate(input)
	want := time.Date(2026, 3, 19, 0, 0, 0, 0, time.UTC)
	if !got.Equal(want) {
		t.Fatalf("truncateToUTCDate() = %s, want %s", got.Format(time.RFC3339), want.Format(time.RFC3339))
	}
}

func TestShouldDropDailyPartition(t *testing.T) {
	cutoff := time.Date(2026, 3, 18, 0, 0, 0, 0, time.UTC)
	tests := []struct {
		name   string
		table  string
		prefix string
		want   bool
	}{
		{name: "older partition drops", table: "telemetry_events_20260317", prefix: "telemetry_events_", want: true},
		{name: "same day partition stays", table: "telemetry_events_20260318", prefix: "telemetry_events_", want: false},
		{name: "newer partition stays", table: "telemetry_events_20260319", prefix: "telemetry_events_", want: false},
		{name: "wrong prefix stays", table: "agent_events_20260317", prefix: "telemetry_events_", want: false},
		{name: "bad suffix stays", table: "telemetry_events_bad", prefix: "telemetry_events_", want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := shouldDropDailyPartition(tt.table, tt.prefix, cutoff); got != tt.want {
				t.Fatalf("shouldDropDailyPartition(%q, %q) = %v, want %v", tt.table, tt.prefix, got, tt.want)
			}
		})
	}
}
