package engine

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
	"github.com/lihongjie/workflow-go/storage/memstore"
)

// 委派: A→DelegateTask(B)→B审批完成→回到A→A通过→End
func TestDelegate_Basic(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "dlg", Key: "dlg", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T", Assignee: "A"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "dlg", nil)
		acts, _ := s.ListActiveActivities(ctx, pi.ID)

		// A delegates to B
		if err := e.DelegateTask(ctx, acts[0].ID, "B"); err != nil {
			t.Fatalf("DelegateTask: %v", err)
		}
		ai, _ := s.GetActivityInstance(ctx, acts[0].ID)
		if ai.Assignee != "B" { t.Fatalf("assignee=%q, want B", ai.Assignee) }

		// B completes → should return to A, not forward
		completeFirstActive(ctx, t, e, s, pi.ID)

		// A should be active again
		remaining := getActiveIDs(ctx, s, pi.ID)
		if len(remaining) != 1 {
			t.Fatalf("expected A to be active, got %v", remaining)
		}
		returnedAI, _ := s.ListActiveActivities(ctx, pi.ID)
		if returnedAI[0].Assignee != "A" {
			t.Errorf("returned to %q, want A", returnedAI[0].Assignee)
		}

		// A completes → forward to end
		completeFirstActive(ctx, t, e, s, pi.ID)
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 委派错误场景
func TestDelegate_Errors(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "dlge", Key: "dlge", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		t.Run("已完成活动", func(t *testing.T) {
			pi, _ := e.StartProcessInstance(ctx, "dlge", nil)
			acts, _ := s.ListActiveActivities(ctx, pi.ID)
			completeFirstActive(ctx, t, e, s, pi.ID)
			if err := e.DelegateTask(ctx, acts[0].ID, "X"); err == nil { t.Error("expected error") }
		})
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}
