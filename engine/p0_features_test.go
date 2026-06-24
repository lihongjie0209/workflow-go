package engine

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
	"github.com/lihongjie/workflow-go/storage/memstore"
	"github.com/lihongjie/workflow-go/storage/sqlstore"
)

// 转办: A → TransferTask(B) → B审批 → End
func TestTransferTask_Basic(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "tf", Key: "tf", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "A": &spec.UserTask{ID: "A", Assignee: "A"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "A"}, {ID: "s2", SourceRef: "A", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "tf", map[string]any{})
		acts, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(acts) != 1 || acts[0].Assignee != "A" { t.Fatalf("expected A, got %v", acts[0].Assignee) }

		// Transfer to B
		e.TransferTask(ctx, acts[0].ID, "B")
		ai, _ := s.GetActivityInstance(ctx, acts[0].ID)
		if ai.Assignee != "B" { t.Fatalf("assignee=%q, want B", ai.Assignee) }

		// B completes
		completeFirstActive(ctx, t, e, s, pi.ID)
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted { t.Errorf("state=%q", p2.State) }
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
	t.Run("sqlite", func(t *testing.T) { s := sqlstore.New(sqlstore.WithMemory()); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 转办错误场景
func TestTransferTask_Errors(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "tfe", Key: "tfe", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		t.Run("已完成活动", func(t *testing.T) {
			pi, _ := e.StartProcessInstance(ctx, "tfe", nil)
			acts, _ := s.ListActiveActivities(ctx, pi.ID)
			completeFirstActive(ctx, t, e, s, pi.ID)
			if err := e.TransferTask(ctx, acts[0].ID, "X"); err == nil { t.Error("expected error") }
		})
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 减签: Start → A → B(forward sign C) → RemoveSign(C) → A完成 → End
func TestRemoveSign_Basic(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "rs", Key: "rs", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "A": &spec.UserTask{ID: "A"}, "B": &spec.UserTask{ID: "B"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "A"}, {ID: "s2", SourceRef: "A", TargetRef: "B"}, {ID: "s3", SourceRef: "B", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "rs", nil)
		completeFirstActive(ctx, t, e, s, pi.ID) // A

		// At B, add forward sign for C, D
		bActs, _ := s.ListActiveActivities(ctx, pi.ID)
		e.AddSign(ctx, bActs[0].ID, SignForward, StrategyOR, []string{"C", "D"})

		// Remove sign C
		e.RemoveSign(ctx, bActs[0].ID, "C")

		// D should still be active
		allActs := getActiveIDs(ctx, s, pi.ID)
		t.Logf("after remove C: %v", allActs)
		hasC := false
		for _, id := range allActs {
			if id == "C" { hasC = true }
		}
		if hasC { t.Error("C should have been removed") }
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
	t.Run("sqlite", func(t *testing.T) { s := sqlstore.New(sqlstore.WithMemory()); t.Cleanup(func() { s.Close() }); run(t, s) })
}
