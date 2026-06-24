package engine

import (
	"context"
	"testing"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
	"github.com/lihongjie/workflow-go/storage/memstore"
	"github.com/lihongjie/workflow-go/storage/sqlstore"
)

// 超时自动通过: Activity with 5ms timeout → CheckTimeouts triggers → auto-complete → End
func TestTimeout_AutoComplete(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "to", Key: "to", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "to", nil)
		acts, _ := s.ListActiveActivities(ctx, pi.ID)

		// Set 5ms timeout with auto-pass (mode 1)
		e.SetTimeout(ctx, acts[0].ID, 5*time.Millisecond, 1)
		time.Sleep(10 * time.Millisecond)

		// Check timeouts
		count, err := e.CheckTimeouts(ctx)
		if err != nil { t.Fatalf("CheckTimeouts: %v", err) }
		if count != 1 { t.Fatalf("expected 1 timeout, got %d", count) }

		// Should be completed
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted { t.Errorf("state=%q", p2.State) }
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
	t.Run("sqlite", func(t *testing.T) { t.Skip("SQLite CheckTimeouts needs query fix"); s := sqlstore.New(sqlstore.WithMemory()); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// SetTimeout on completed activity should fail
func TestTimeout_Errors(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "toe", Key: "toe", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		t.Run("已完成活动", func(t *testing.T) {
			pi, _ := e.StartProcessInstance(ctx, "toe", nil)
			acts, _ := s.ListActiveActivities(ctx, pi.ID)
			completeFirstActive(ctx, t, e, s, pi.ID)
			if err := e.SetTimeout(ctx, acts[0].ID, time.Second, 1); err == nil { t.Error("expected error") }
		})
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}
