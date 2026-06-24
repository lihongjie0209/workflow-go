package engine

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage/memstore"
	"github.com/lihongjie/workflow-go/storage/sqlstore"
)

func TestEngine_LinearFlow(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID:      "linear",
		Name:    "Linear Flow",
		Key:     "linear",
		Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start", Name: "Start"},
			"task1": &spec.UserTask{ID: "task1", Name: "Task A"},
			"end":   &spec.EndEvent{ID: "end", Name: "End"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "task1"},
			{ID: "sf2", SourceRef: "task1", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	pi, err := e.StartProcessInstance(ctx, "linear", nil)
	if err != nil {
		t.Fatalf("StartProcessInstance: %v", err)
	}
	if pi.State != instance.ProcessInstanceStateRunning {
		t.Errorf("instance state = %q, want running", pi.State)
	}

	// Find the active UserTask activity.
	activities, err := s.ListActiveActivities(ctx, pi.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activities) != 1 {
		t.Fatalf("expected 1 active activity, got %d", len(activities))
	}
	if activities[0].ActivityID != "task1" {
		t.Errorf("expected activity task1, got %s", activities[0].ActivityID)
	}

	// Complete the task.
	if err := e.CompleteTask(ctx, activities[0].ID, nil); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}

	// Verify process completed.
	pi, err = s.GetProcessInstance(ctx, pi.ID)
	if err != nil {
		t.Fatal(err)
	}
	if pi.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("instance state after completion = %q, want completed", pi.State)
	}

	// Verify all activities.
	allActivities, err := s.ListActivitiesByProcessInstance(ctx, pi.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(allActivities) != 3 {
		t.Fatalf("expected 3 activities (start, task, end), got %d", len(allActivities))
	}
}

func TestEngine_ExclusiveGateway_XOR(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	condHigh := "${amount > 100}"
	condLow := "${amount <= 100}"

	def := &spec.ProcessDefinition{
		ID:      "xor",
		Name:    "XOR Gateway",
		Key:     "xor",
		Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"xor":   &spec.ExclusiveGateway{ID: "xor", Name: "Amount Check", DefaultFlowID: "sf_default"},
			"task_high": &spec.UserTask{ID: "task_high", Name: "High Amount"},
			"task_low":  &spec.UserTask{ID: "task_low", Name: "Low Amount"},
			"end":       &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf_start", SourceRef: "start", TargetRef: "xor"},
			{ID: "sf_high", SourceRef: "xor", TargetRef: "task_high", ConditionExpression: &condHigh},
			{ID: "sf_low", SourceRef: "xor", TargetRef: "task_low", ConditionExpression: &condLow},
			{ID: "sf_default", SourceRef: "xor", TargetRef: "task_low"},
			{ID: "sf_high_end", SourceRef: "task_high", TargetRef: "end"},
			{ID: "sf_low_end", SourceRef: "task_low", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	t.Run("takes high amount path", func(t *testing.T) {
		pi, err := e.StartProcessInstance(ctx, "xor", map[string]any{"amount": 500})
		if err != nil {
			t.Fatalf("StartProcessInstance: %v", err)
		}
		activities, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(activities) != 1 || activities[0].ActivityID != "task_high" {
			t.Errorf("expected active at task_high, got %v", activityIDs(activities))
		}
	})

	t.Run("takes low amount path", func(t *testing.T) {
		pi, err := e.StartProcessInstance(ctx, "xor", map[string]any{"amount": 50})
		if err != nil {
			t.Fatalf("StartProcessInstance: %v", err)
		}
		activities, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(activities) != 1 || activities[0].ActivityID != "task_low" {
			t.Errorf("expected active at task_low, got %v", activityIDs(activities))
		}
	})
}

func TestEngine_ParallelGateway_AND(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID:      "and",
		Name:    "Parallel Gateway",
		Key:     "and",
		Version: 1,
		Elements: map[string]spec.FlowElement{
			"start":  &spec.StartEvent{ID: "start"},
			"fork":   &spec.ParallelGateway{ID: "fork", Name: "Fork"},
			"task_a": &spec.UserTask{ID: "task_a", Name: "Task A"},
			"task_b": &spec.UserTask{ID: "task_b", Name: "Task B"},
			"join":   &spec.ParallelGateway{ID: "join", Name: "Join"},
			"end":    &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf_start", SourceRef: "start", TargetRef: "fork"},
			{ID: "sf_a", SourceRef: "fork", TargetRef: "task_a"},
			{ID: "sf_b", SourceRef: "fork", TargetRef: "task_b"},
			{ID: "sf_a_join", SourceRef: "task_a", TargetRef: "join"},
			{ID: "sf_b_join", SourceRef: "task_b", TargetRef: "join"},
			{ID: "sf_join_end", SourceRef: "join", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	pi, err := e.StartProcessInstance(ctx, "and", nil)
	if err != nil {
		t.Fatalf("StartProcessInstance: %v", err)
	}

	// Both tasks should be active after fork.
	activities, err := s.ListActiveActivities(ctx, pi.ID)
	if err != nil {
		t.Fatal(err)
	}
	if len(activities) != 2 {
		t.Fatalf("expected 2 active activities after fork, got %d: %v", len(activities), activityIDs(activities))
	}

	// Complete both tasks.
	taskAComplete := false
	taskBComplete := false

	for _, ai := range activities {
		if ai.ActivityID == "task_a" {
			if err := e.CompleteTask(ctx, ai.ID, nil); err != nil {
				t.Fatalf("CompleteTask task_a: %v", err)
			}
			taskAComplete = true
		}
		if ai.ActivityID == "task_b" {
			if err := e.CompleteTask(ctx, ai.ID, nil); err != nil {
				t.Fatalf("CompleteTask task_b: %v", err)
			}
			taskBComplete = true
		}
	}

	if !taskAComplete || !taskBComplete {
		t.Fatal("did not complete both tasks")
	}

	// Process should still be running after first task completes.
	pi, err = s.GetProcessInstance(ctx, pi.ID)
	if err != nil {
		t.Fatal(err)
	}
	if pi.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("expected completed after both tasks complete, got %q", pi.State)
	}
}

func TestEngine_InclusiveGateway_OR(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	// Use a definition where ALL outgoing flows have conditions,
	// so only matching conditions activate.
	condA := "${category == \"a\"}"
	condB := "${category == \"b\"}"

	def := &spec.ProcessDefinition{
		ID:      "or_all_conditional",
		Name:    "Inclusive Gateway",
		Key:     "or",
		Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"or_gw": &spec.InclusiveGateway{ID: "or_gw", Name: "OR Split"},
			"task_a": &spec.UserTask{ID: "task_a", Name: "Task A"},
			"task_b": &spec.UserTask{ID: "task_b", Name: "Task B"},
			"task_c": &spec.UserTask{ID: "task_c", Name: "Task C"},
			"end":    &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf_start", SourceRef: "start", TargetRef: "or_gw"},
			{ID: "sf_a", SourceRef: "or_gw", TargetRef: "task_a", ConditionExpression: &condA},
			{ID: "sf_b", SourceRef: "or_gw", TargetRef: "task_b", ConditionExpression: &condB},
			{ID: "sf_c", SourceRef: "or_gw", TargetRef: "task_c", ConditionExpression: &condA},
			{ID: "sf_a_end", SourceRef: "task_a", TargetRef: "end"},
			{ID: "sf_b_end", SourceRef: "task_b", TargetRef: "end"},
			{ID: "sf_c_end", SourceRef: "task_c", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	t.Run("activates all matching conditions", func(t *testing.T) {
		// condA matches task_a and task_c, condB doesn't match
		pi, err := e.StartProcessInstance(ctx, "or_all_conditional", map[string]any{"category": "a"})
		if err != nil {
			t.Fatalf("StartProcessInstance: %v", err)
		}
		activities, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(activities) != 2 {
			t.Fatalf("expected 2 active tasks (a and c), got %d: %v", len(activities), activityIDs(activities))
		}
		hasA, hasC := false, false
		for _, a := range activities {
			if a.ActivityID == "task_a" { hasA = true }
			if a.ActivityID == "task_c" { hasC = true }
		}
		if !hasA || !hasC {
			t.Errorf("expected task_a and task_c, got %v", activityIDs(activities))
		}
	})

	t.Run("activates only matching condition", func(t *testing.T) {
		// Only condB matches (category == "b")
		pi, err := e.StartProcessInstance(ctx, "or_all_conditional", map[string]any{"category": "b"})
		if err != nil {
			t.Fatalf("StartProcessInstance: %v", err)
		}
		activities, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(activities) != 1 || activities[0].ActivityID != "task_b" {
			t.Errorf("expected only task_b active, got %v", activityIDs(activities))
		}
	})

	t.Run("no matching conditions without default", func(t *testing.T) {
		// No condition matches (category is neither "a" nor "b")
		_, err := e.StartProcessInstance(ctx, "or_all_conditional", map[string]any{"category": "unknown"})
		if err == nil {
			t.Error("expected error when no condition matches and no default flow")
		}
	})
}

func TestEngine_ServiceTask(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID:      "svc",
		Name:    "Service Task Flow",
		Key:     "svc",
		Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"svc1":  &spec.ServiceTask{ID: "svc1", Name: "Auto Task"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "svc1"},
			{ID: "sf2", SourceRef: "svc1", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	pi, err := e.StartProcessInstance(ctx, "svc", nil)
	if err != nil {
		t.Fatalf("StartProcessInstance: %v", err)
	}
	if pi.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("expected completed (service task auto-completes), got %q", pi.State)
	}
}

func TestEngine_CompleteTask_WithVariables(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID:      "varflow",
		Name:    "Variable Flow",
		Key:     "varflow",
		Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task1": &spec.UserTask{ID: "task1"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "task1"},
			{ID: "sf2", SourceRef: "task1", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	pi, err := e.StartProcessInstance(ctx, "varflow", map[string]any{"init": true})
	if err != nil {
		t.Fatalf("StartProcessInstance: %v", err)
	}

	activities, _ := s.ListActiveActivities(ctx, pi.ID)
	if err := e.CompleteTask(ctx, activities[0].ID, map[string]any{"approved": true}); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}

	// Verify variables were stored.
	approved, err := s.GetVariable(ctx, pi.ID, "approved")
	if err != nil {
		t.Fatalf("GetVariable: %v", err)
	}
	if approved != true {
		t.Errorf("approved = %v, want true", approved)
	}

	initVal, err := s.GetVariable(ctx, pi.ID, "init")
	if err != nil {
		t.Fatalf("GetVariable: %v", err)
	}
	if initVal != true {
		t.Errorf("init = %v, want true", initVal)
	}
}

func TestEngine_ErrorCases(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	t.Run("unknown definition", func(t *testing.T) {
		_, err := e.StartProcessInstance(ctx, "nonexistent", nil)
		if err == nil {
			t.Error("expected error for unknown definition")
		}
	})

	t.Run("complete nonexistent activity", func(t *testing.T) {
		err := e.CompleteTask(ctx, "nonexistent", nil)
		if err == nil {
			t.Error("expected error for nonexistent activity")
		}
	})

	t.Run("complete already completed task", func(t *testing.T) {
		def := &spec.ProcessDefinition{
			ID:      "dbl",
			Name:    "Double Complete",
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"task1": &spec.UserTask{ID: "task1"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "sf1", SourceRef: "start", TargetRef: "task1"},
				{ID: "sf2", SourceRef: "task1", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		if err := s.CreateProcessDefinition(ctx, def); err != nil {
			t.Fatal(err)
		}
		pi, _ := e.StartProcessInstance(ctx, "dbl", nil)
		activities, _ := s.ListActiveActivities(ctx, pi.ID)

		// First complete should succeed.
		if err := e.CompleteTask(ctx, activities[0].ID, nil); err != nil {
			t.Fatalf("first CompleteTask: %v", err)
		}

		// Second complete should fail.
		if err := e.CompleteTask(ctx, activities[0].ID, nil); err == nil {
			t.Error("expected error on second CompleteTask")
		}
	})
}

func activityIDs(activities []*instance.ActivityInstance) []string {
	ids := make([]string, len(activities))
	for i, a := range activities {
		ids[i] = a.ActivityID
	}
	return ids
}

// TestEngine_WithSQLite runs key flow tests against SQLite storage to verify
// engine behavior is consistent across storage backends.
func TestEngine_WithSQLite(t *testing.T) {
	ctx := context.Background()
	s := sqlstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	// Linear flow
	def := &spec.ProcessDefinition{
		ID:      "linear",
		Key:     "linear",
		Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task1": &spec.UserTask{ID: "task1"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "task1"},
			{ID: "sf2", SourceRef: "task1", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	pi, err := e.StartProcessInstance(ctx, "linear", nil)
	if err != nil {
		t.Fatalf("StartProcessInstance: %v", err)
	}
	activities, _ := s.ListActiveActivities(ctx, pi.ID)
	if len(activities) != 1 {
		t.Fatalf("expected 1 active activity, got %d", len(activities))
	}
	if err := e.CompleteTask(ctx, activities[0].ID, nil); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}
	pi, _ = s.GetProcessInstance(ctx, pi.ID)
	if pi.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("expected completed, got %q", pi.State)
	}

	// XOR gateway
	condHigh := "${amount > 100}"
	xorDef := &spec.ProcessDefinition{
		ID:      "xor",
		Key:     "xor",
		Version: 1,
		Elements: map[string]spec.FlowElement{
			"start":    &spec.StartEvent{ID: "start"},
			"xor":      &spec.ExclusiveGateway{ID: "xor", DefaultFlowID: "sf_low"},
			"task_high": &spec.UserTask{ID: "task_high"},
			"task_low":  &spec.UserTask{ID: "task_low"},
			"end":       &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf_start", SourceRef: "start", TargetRef: "xor"},
			{ID: "sf_high", SourceRef: "xor", TargetRef: "task_high", ConditionExpression: &condHigh},
			{ID: "sf_low", SourceRef: "xor", TargetRef: "task_low"},
			{ID: "sf_he", SourceRef: "task_high", TargetRef: "end"},
			{ID: "sf_le", SourceRef: "task_low", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, xorDef); err != nil {
		t.Fatal(err)
	}

	pi, err = e.StartProcessInstance(ctx, "xor", map[string]any{"amount": 500})
	if err != nil {
		t.Fatalf("XOR StartProcessInstance: %v", err)
	}
	activities, _ = s.ListActiveActivities(ctx, pi.ID)
	if len(activities) != 1 || activities[0].ActivityID != "task_high" {
		t.Errorf("expected task_high active, got %v", activityIDs(activities))
	}
}
