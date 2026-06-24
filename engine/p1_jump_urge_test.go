package engine

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
	"github.com/lihongjie/workflow-go/storage/memstore"
)

// 跳转: A→B, 跳转到End直接完成
func TestJump_Basic(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "jp", Key: "jp", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "A": &spec.UserTask{ID: "A"}, "B": &spec.UserTask{ID: "B"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "A"}, {ID: "s2", SourceRef: "A", TargetRef: "B"}, {ID: "s3", SourceRef: "B", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "jp", nil)
		completeFirstActive(ctx, t, e, s, pi.ID) // A→B

		// Jump from B directly to end
		bActs, _ := s.ListActiveActivities(ctx, pi.ID)
		if err := e.JumpTask(ctx, bActs[0].ID, "end"); err != nil {
			t.Fatalf("JumpTask: %v", err)
		}
		// Should be at end → process completes
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted { t.Errorf("state=%q", p2.State) }
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 催办
func TestUrge_Basic(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "ug", Key: "ug", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T", Assignee: "张三"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "ug", nil)
		acts, _ := s.ListActiveActivities(ctx, pi.ID)

		assignee, err := e.UrgeTask(ctx, acts[0].ID)
		if err != nil { t.Fatalf("UrgeTask: %v", err) }
		if assignee != "张三" { t.Errorf("assignee=%q", assignee) }

		// Verify urge count
		v, _ := s.GetVariable(ctx, pi.ID, "__urge_count_"+pi.ID)
		if v != float64(1) { t.Errorf("urge count=%v, want 1", v) }
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 抄送
func TestCc_Basic(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "ccf", Key: "ccf", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "ccf", nil)

		// CC to user "王五"
		if err := e.CcTask(ctx, pi.ID, "王五"); err != nil {
			t.Fatalf("CcTask: %v", err)
		}
		// Verify CC record in history
		hist, _ := s.ListHistoricByProcessInstance(ctx, pi.ID)
		found := false
		for _, h := range hist {
			if h.Variables != nil {
				if cc, ok := h.Variables["cc"]; ok && cc == true {
					found = true
				}
			}
		}
		if !found { t.Error("CC record not found in history") }
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}
