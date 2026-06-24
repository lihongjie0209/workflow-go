package engine

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
	"github.com/lihongjie/workflow-go/storage/memstore"
)

// 拿回: A→B, B未处理, A拿回→A重新激活→A重新提交→B→End
func TestReclaim_Basic(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "rc", Key: "rc", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "A": &spec.UserTask{ID: "A"}, "B": &spec.UserTask{ID: "B"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "A"}, {ID: "s2", SourceRef: "A", TargetRef: "B"}, {ID: "s3", SourceRef: "B", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "rc", nil)
		completeFirstActive(ctx, t, e, s, pi.ID) // A→B

		// B is active, reclaim it back to A
		bActs, _ := s.ListActiveActivities(ctx, pi.ID)
		if err := e.ReclaimTask(ctx, bActs[0].ID); err != nil {
			t.Fatalf("ReclaimTask: %v", err)
		}

		// A should be active again
		back := getActiveIDs(ctx, s, pi.ID)
		if len(back) != 1 || back[0] != "A" {
			t.Fatalf("expected A, got %v", back)
		}

		// A re-submits → B → end
		completeFirstActive(ctx, t, e, s, pi.ID)
		completeFirstActive(ctx, t, e, s, pi.ID)
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted { t.Errorf("state=%q", p2.State) }
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}
