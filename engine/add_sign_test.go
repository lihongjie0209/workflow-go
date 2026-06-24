package engine

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
	"github.com/lihongjie/workflow-go/storage/memstore"
)

func runForwardOR(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "f-or", Key: "f-or", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "task": &spec.UserTask{ID: "task"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"}, {ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "f-or", nil)
	orig, _ := s.ListActiveActivities(ctx, pi.ID)
	if err := e.AddSign(ctx, orig[0].ID, SignForward, StrategyOR, []string{"B", "C"}); err != nil {
		t.Fatalf("AddSign: %v", err)
	}
	after, _ := s.ListActiveActivities(ctx, pi.ID)
	if len(after) != 3 {
		t.Fatalf("expected 3 activities, got %d", len(after))
	}
	for _, a := range after {
		if a.AdhocParentID == orig[0].ID && a.Assignee == "B" {
			e.CompleteTask(ctx, a.ID, map[string]any{"approved": true})
		}
	}
	rem, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range rem {
		if a.AdhocParentID == "" {
			e.CompleteTask(ctx, a.ID, nil)
		}
	}
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q", p2.State)
	}
}

func TestAddSign_Forward_OR(t *testing.T) {
	s := memstore.New(); t.Cleanup(func() { s.Close() }); runForwardOR(t, s)
}

func runBackwardOR(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "b-or", Key: "b-or", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "task": &spec.UserTask{ID: "task"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"}, {ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "b-or", nil)
	orig, _ := s.ListActiveActivities(ctx, pi.ID)
	e.AddSign(ctx, orig[0].ID, SignBackward, StrategyOR, []string{"B"})
	aAfter, _ := s.GetActivityInstance(ctx, orig[0].ID)
	if aAfter.State != instance.ActivityStateCompleted {
		t.Errorf("A should be completed, got %s", aAfter.State)
	}
	signs, _ := s.ListActiveActivities(ctx, pi.ID)
	if len(signs) != 1 || signs[0].Assignee != "B" {
		t.Fatalf("expected B active")
	}
	e.CompleteTask(ctx, signs[0].ID, map[string]any{"approved": true})
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q (tokens=%d)", p2.State, len(getActiveIDs(ctx, s, pi.ID)))
	}
}

func TestAddSign_Backward_OR(t *testing.T) {
	s := memstore.New(); t.Cleanup(func() { s.Close() }); runBackwardOR(t, s)
}

func TestAddSign_Forward_AND(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "f-and", Key: "f-and", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "task": &spec.UserTask{ID: "task"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"}, {ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "f-and", nil)
	orig, _ := s.ListActiveActivities(ctx, pi.ID)
	e.AddSign(ctx, orig[0].ID, SignForward, StrategyAND, []string{"B", "C"})
	all, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range all {
		if a.AdhocParentID == orig[0].ID && a.Assignee == "B" {
			e.CompleteTask(ctx, a.ID, map[string]any{"approved": true})
		}
	}
	rem, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range rem {
		if a.AdhocParentID == orig[0].ID {
			e.CompleteTask(ctx, a.ID, map[string]any{"approved": true})
		}
	}
	rem2, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range rem2 {
		e.CompleteTask(ctx, a.ID, nil)
	}
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q", p2.State)
	}
}

func TestAddSign_Parallel_AND(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "p-and", Key: "p-and", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "task": &spec.UserTask{ID: "task"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"}, {ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "p-and", nil)
	orig, _ := s.ListActiveActivities(ctx, pi.ID)
	e.AddSign(ctx, orig[0].ID, SignParallel, StrategyAND, []string{"B"})
	all, _ := s.ListActiveActivities(ctx, pi.ID)
	if len(all) != 2 {
		t.Fatalf("expected 2, got %d", len(all))
	}
	for _, a := range all {
		if a.AdhocParentID == "" {
			e.CompleteTask(ctx, a.ID, nil)
		}
	}
	aAfter, _ := s.GetActivityInstance(ctx, orig[0].ID)
	if aAfter.State != instance.ActivityStateCompleted {
		t.Errorf("A should be completed, got %s", aAfter.State)
	}
	signs, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range signs {
		e.CompleteTask(ctx, a.ID, map[string]any{"approved": true})
	}
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q", p2.State)
	}
}

func TestAddSign_DynamicAssignee(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "dyn", Key: "dyn", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "task": &spec.UserTask{ID: "task"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"}, {ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "dyn", map[string]any{"referee": "陈七"})
	orig, _ := s.ListActiveActivities(ctx, pi.ID)
	e.AddSign(ctx, orig[0].ID, SignForward, StrategyOR, []string{"${referee}"})
	signs, _ := s.ListActiveActivities(ctx, pi.ID)
	found := false
	for _, a := range signs {
		if a.AdhocParentID == orig[0].ID && a.Assignee == "陈七" {
			found = true
		}
	}
	if !found {
		t.Error("sign assignee was not rendered as 陈七")
	}
}

func TestAddSign_Errors(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "err", Key: "err", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "task": &spec.UserTask{ID: "task"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"}, {ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	t.Run("empty assignees", func(t *testing.T) {
		pi, _ := e.StartProcessInstance(ctx, "err", nil)
		ai, _ := s.ListActiveActivities(ctx, pi.ID)
		if err := e.AddSign(ctx, ai[0].ID, SignForward, StrategyOR, nil); err == nil {
			t.Error("expected error")
		}
	})
	t.Run("completed activity", func(t *testing.T) {
		pi, _ := e.StartProcessInstance(ctx, "err", nil)
		ai, _ := s.ListActiveActivities(ctx, pi.ID)
		e.CompleteTask(ctx, ai[0].ID, nil)
		if err := e.AddSign(ctx, ai[0].ID, SignForward, StrategyOR, []string{"X"}); err == nil {
			t.Error("expected error")
		}
	})
}
