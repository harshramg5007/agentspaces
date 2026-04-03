package main

import (
	"fmt"
	"testing"

	"github.com/urobora-ai/agentspaces/pkg/agent"
)

type mockStatusCounter struct {
	global map[agent.Status]int
	byKind map[string]map[agent.Status]int
}

func (m mockStatusCounter) CountByStatus(query *agent.Query) (map[agent.Status]int, error) {
	if query != nil && query.Kind != "" {
		counts := m.byKind[query.Kind]
		return cloneStatusCounts(counts), nil
	}
	return cloneStatusCounts(m.global), nil
}

type mockCounter struct {
	counts map[string]int
}

func (m mockCounter) Count(query *agent.Query) (int, error) {
	if query == nil {
		return 0, fmt.Errorf("query is required")
	}
	key := string(query.Status) + "|" + query.Kind
	return m.counts[key], nil
}

func cloneStatusCounts(in map[agent.Status]int) map[agent.Status]int {
	out := make(map[agent.Status]int, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}

func TestSplitCommaListDedup(t *testing.T) {
	got := splitCommaListDedup("fault_task, other , fault_task,other,third")
	want := []string{"fault_task", "other", "third"}
	if len(got) != len(want) {
		t.Fatalf("unexpected len: got=%d want=%d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("unexpected kinds[%d]: got=%q want=%q", i, got[i], want[i])
		}
	}
}

func TestCollectAgentMetricsStatusCounter(t *testing.T) {
	statusCounter := mockStatusCounter{
		global: map[agent.Status]int{
			agent.StatusNew:        10,
			agent.StatusInProgress: 3,
			agent.StatusCompleted:  7,
			agent.StatusFailed:     1,
		},
		byKind: map[string]map[agent.Status]int{
			"fault_task": {
				agent.StatusNew:        6,
				agent.StatusInProgress: 2,
				agent.StatusCompleted:  3,
			},
			"other": {
				agent.StatusNew:        4,
				agent.StatusInProgress: 1,
				agent.StatusCompleted:  4,
				agent.StatusFailed:     1,
			},
		},
	}

	byStatus, byKind, backlogByKind, err := collectAgentMetrics(statusCounter, nil, []string{"fault_task", "other"})
	if err != nil {
		t.Fatalf("collectAgentMetrics returned error: %v", err)
	}

	if byStatus[string(agent.StatusNew)] != 10 || byStatus[string(agent.StatusInProgress)] != 3 {
		t.Fatalf("unexpected byStatus values: %#v", byStatus)
	}
	if byKind["fault_task"] != 11 {
		t.Fatalf("unexpected fault_task total: got=%d want=11", byKind["fault_task"])
	}
	if backlogByKind["fault_task"] != 8 {
		t.Fatalf("unexpected fault_task backlog: got=%d want=8", backlogByKind["fault_task"])
	}
	if backlogByKind["other"] != 5 {
		t.Fatalf("unexpected other backlog: got=%d want=5", backlogByKind["other"])
	}
}

func TestCollectAgentMetricsCounterFallback(t *testing.T) {
	counter := mockCounter{counts: map[string]int{
		string(agent.StatusNew) + "|":                  9,
		string(agent.StatusInProgress) + "|":           2,
		string(agent.StatusCompleted) + "|":            5,
		string(agent.StatusFailed) + "|":               1,
		string(agent.StatusCancelled) + "|":            0,
		string(agent.StatusNew) + "|fault_task":        7,
		string(agent.StatusInProgress) + "|fault_task": 1,
		string(agent.StatusCompleted) + "|fault_task":  4,
		string(agent.StatusFailed) + "|fault_task":     0,
		string(agent.StatusCancelled) + "|fault_task":  0,
	}}

	byStatus, byKind, backlogByKind, err := collectAgentMetrics(nil, counter, []string{"fault_task"})
	if err != nil {
		t.Fatalf("collectAgentMetrics returned error: %v", err)
	}

	if byStatus[string(agent.StatusCompleted)] != 5 {
		t.Fatalf("unexpected byStatus completed: got=%d want=5", byStatus[string(agent.StatusCompleted)])
	}
	if byKind["fault_task"] != 12 {
		t.Fatalf("unexpected byKind total: got=%d want=12", byKind["fault_task"])
	}
	if backlogByKind["fault_task"] != 8 {
		t.Fatalf("unexpected backlog: got=%d want=8", backlogByKind["fault_task"])
	}
}

func TestCollectAgentMetricsNoCounterSupport(t *testing.T) {
	_, _, _, err := collectAgentMetrics(nil, nil, []string{"fault_task"})
	if err == nil {
		t.Fatal("expected error when no counter interfaces are available")
	}
}
