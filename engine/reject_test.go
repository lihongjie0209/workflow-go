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

// 驳回至上一步: Start → A → B → C(驳回→B) → B通过 → End
func TestReject_Previous(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "prev", Key: "prev", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"A":     &spec.UserTask{ID: "A", Name: "A"},
				"B":     &spec.UserTask{ID: "B", Name: "B"},
				"C":     &spec.UserTask{ID: "C", Name: "C"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "A"},
				{ID: "s2", SourceRef: "A", TargetRef: "B"},
				{ID: "s3", SourceRef: "B", TargetRef: "C"},
				{ID: "s4", SourceRef: "C", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)

		pi, _ := e.StartProcessInstance(ctx, "prev", nil)
		completeFirstActive(ctx, t, e, s, pi.ID) // A
		completeFirstActive(ctx, t, e, s, pi.ID) // B
		// Now at C

		cActs, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(cActs) != 1 || cActs[0].ActivityID != "C" {
			t.Fatalf("expected at C, got %v", getActiveIDs(ctx, s, pi.ID))
		}

		// 驳回 C 到上一步 B
		if err := e.RejectTask(ctx, cActs[0].ID, RejectPrevious, "需要修改", ""); err != nil {
			t.Fatalf("RejectTask: %v", err)
		}

		// B 应该重新激活
		acts := getActiveIDs(ctx, s, pi.ID)
		if len(acts) != 1 || acts[0] != "B" {
			t.Fatalf("expected B reactivated, got %v", acts)
		}

		// B 通过后流程应该继续到 C
		completeFirstActive(ctx, t, e, s, pi.ID)
		acts = getActiveIDs(ctx, s, pi.ID)
		if len(acts) != 1 || acts[0] != "C" {
			t.Fatalf("expected C after B, got %v", acts)
		}

		completeFirstActive(ctx, t, e, s, pi.ID)
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted {
			t.Errorf("state=%q", p2.State)
		}
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
	t.Run("sqlite", func(t *testing.T) { s := sqlstore.New(sqlstore.WithMemory()); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 驳回到发起人: Start → A → B(驳回→A) → A重新提交 → B → End
func TestReject_Initiator(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "init", Key: "init", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"apply": &spec.UserTask{ID: "apply", Name: "申请"},
				"audit": &spec.UserTask{ID: "audit", Name: "审核"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "apply"},
				{ID: "s2", SourceRef: "apply", TargetRef: "audit"},
				{ID: "s3", SourceRef: "audit", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)

		pi, _ := e.StartProcessInstance(ctx, "init", nil)
		completeFirstActive(ctx, t, e, s, pi.ID) // apply

		// Now at audit, reject to initiator
		auditActs, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(auditActs) != 1 || auditActs[0].ActivityID != "audit" {
			t.Fatalf("expected at audit, got %v", getActiveIDs(ctx, s, pi.ID))
		}

		if err := e.RejectTask(ctx, auditActs[0].ID, RejectInitiator, "请修改", ""); err != nil {
			t.Fatalf("RejectTask: %v", err)
		}

		// Should be back at "apply" (the first UserTask after start)
		acts := getActiveIDs(ctx, s, pi.ID)
		if len(acts) != 1 || acts[0] != "apply" {
			t.Fatalf("expected apply reactivated, got %v", acts)
		}

		// 发起人重新提交: 通过 apply → 流转到 audit
		completeFirstActive(ctx, t, e, s, pi.ID)
		acts = getActiveIDs(ctx, s, pi.ID)
		if len(acts) != 1 || acts[0] != "audit" {
			t.Fatalf("expected audit after resubmit, got %v", acts)
		}

		completeFirstActive(ctx, t, e, s, pi.ID)
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted {
			t.Errorf("state=%q", p2.State)
		}
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
	t.Run("sqlite", func(t *testing.T) { s := sqlstore.New(sqlstore.WithMemory()); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 驳回到指定节点: Start → A → B → C(驳回指定到A) → A通过 → B → C → End
func TestReject_Specific(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "spec", Key: "spec", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"A":     &spec.UserTask{ID: "A"},
				"B":     &spec.UserTask{ID: "B"},
				"C":     &spec.UserTask{ID: "C"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "A"},
				{ID: "s2", SourceRef: "A", TargetRef: "B"},
				{ID: "s3", SourceRef: "B", TargetRef: "C"},
				{ID: "s4", SourceRef: "C", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)

		pi, _ := e.StartProcessInstance(ctx, "spec", nil)
		completeFirstActive(ctx, t, e, s, pi.ID) // A
		completeFirstActive(ctx, t, e, s, pi.ID) // B
		// Now at C, reject to A

		cActs, _ := s.ListActiveActivities(ctx, pi.ID)
		if err := e.RejectTask(ctx, cActs[0].ID, RejectSpecific, "跳过B直接改A", "A"); err != nil {
			t.Fatalf("RejectTask: %v", err)
		}

		acts := getActiveIDs(ctx, s, pi.ID)
		if len(acts) != 1 || acts[0] != "A" {
			t.Fatalf("expected A, got %v", acts)
		}

		// A → B → C → End
		completeFirstActive(ctx, t, e, s, pi.ID)
		completeFirstActive(ctx, t, e, s, pi.ID)
		completeFirstActive(ctx, t, e, s, pi.ID)
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted {
			t.Errorf("state=%q", p2.State)
		}
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
	t.Run("sqlite", func(t *testing.T) { s := sqlstore.New(sqlstore.WithMemory()); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 终止流程: Start → A(终止流程) → state=rejected
func TestReject_Terminate(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "term", Key: "term", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"A":     &spec.UserTask{ID: "A"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "A"},
				{ID: "s2", SourceRef: "A", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)

		pi, _ := e.StartProcessInstance(ctx, "term", nil)
		aActs, _ := s.ListActiveActivities(ctx, pi.ID)

		if err := e.RejectTask(ctx, aActs[0].ID, RejectTerminate, "严重违规", ""); err != nil {
			t.Fatalf("RejectTask: %v", err)
		}

		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateRejected {
			t.Errorf("state=%q, want rejected", p2.State)
		}
		// No active activities
		left := getActiveIDs(ctx, s, pi.ID)
		if len(left) != 0 {
			t.Errorf("expected no active activities, got %v", left)
		}
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
	t.Run("sqlite", func(t *testing.T) { s := sqlstore.New(sqlstore.WithMemory()); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 驳回次数限制
func TestReject_Limit(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "limit", Key: "limit", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"A":     &spec.UserTask{ID: "A"},
				"B":     &spec.UserTask{ID: "B"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "A"},
				{ID: "s2", SourceRef: "A", TargetRef: "B"},
				{ID: "s3", SourceRef: "B", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "limit", nil)
		completeFirstActive(ctx, t, e, s, pi.ID)

		bActs,_ := s.ListActiveActivities(ctx, pi.ID)
		e.RejectTask(ctx, bActs[0].ID, RejectPrevious, "改1", "")

		// Complete A → B
		completeFirstActive(ctx, t, e, s, pi.ID)

		// Set max rejection to 1 after the first reject happened
		s.SetVariable(ctx, pi.ID, "__maxRejectionCount", float64(1))

		bActs2,_ := s.ListActiveActivities(ctx, pi.ID)
		err := e.RejectTask(ctx, bActs2[0].ID, RejectPrevious, "改2", "")
		if err == nil {
			t.Error("expected rejection limit error but got nil")
		}
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 含网关的驳回: Start → XOR → A → B → C(驳回上一步→B) 跳过XOR
func TestReject_Previous_WithGateway(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "gw-prev", Key: "gw-prev", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"xor":   &spec.ExclusiveGateway{ID: "xor", DefaultFlowID: "sf_a"},
				"A":     &spec.UserTask{ID: "A"},
				"B":     &spec.UserTask{ID: "B"},
				"C":     &spec.UserTask{ID: "C"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "xor"},
				{ID: "sf_a", SourceRef: "xor", TargetRef: "A"},
				{ID: "s2", SourceRef: "A", TargetRef: "B"},
				{ID: "s3", SourceRef: "B", TargetRef: "C"},
				{ID: "s4", SourceRef: "C", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)

		pi, _ := e.StartProcessInstance(ctx, "gw-prev", nil)
		completeFirstActive(ctx, t, e, s, pi.ID) // A
		completeFirstActive(ctx, t, e, s, pi.ID) // B

		cActs, _ := s.ListActiveActivities(ctx, pi.ID)
		if err := e.RejectTask(ctx, cActs[0].ID, RejectPrevious, "退回", ""); err != nil {
			t.Fatalf("RejectTask: %v", err)
		}

		// 应该回到 B（跳过 XOR gateway）
		acts := getActiveIDs(ctx, s, pi.ID)
		if len(acts) != 1 || acts[0] != "B" {
			t.Fatalf("expected B through gateway, got %v", acts)
		}
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 驳回后重提的变量记录
func TestReject_VariableTracking(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "vartrack", Key: "vartrack", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"X":     &spec.UserTask{ID: "X"},
				"Y":     &spec.UserTask{ID: "Y"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "X"},
				{ID: "s2", SourceRef: "X", TargetRef: "Y"},
				{ID: "s3", SourceRef: "Y", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)

		pi, _ := e.StartProcessInstance(ctx, "vartrack", nil)
		completeFirstActive(ctx, t, e, s, pi.ID) // X

		yActs, _ := s.ListActiveActivities(ctx, pi.ID)
		e.RejectTask(ctx, yActs[0].ID, RejectPrevious, "缺少附件", "")

		// Verify rejection counter tracked
		count, _ := s.GetVariable(ctx, pi.ID, "__rejectionCount")
		if count != float64(1) {
			t.Errorf("rejectionCount=%v, want 1", count)
		}
		reason, _ := s.GetVariable(ctx, pi.ID, "__rejectionReason")
		if reason != "缺少附件" {
			t.Errorf("rejectionReason=%v, want 缺少附件", reason)
		}
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 错误场景
func TestReject_ErrorCases(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)
		def := &spec.ProcessDefinition{
			ID: "err", Key: "err", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"T":     &spec.UserTask{ID: "T"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "T"},
				{ID: "s2", SourceRef: "T", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)

		t.Run("已完成活动", func(t *testing.T) {
			pi, _ := e.StartProcessInstance(ctx, "err", nil)
			acts, _ := s.ListActiveActivities(ctx, pi.ID)
			e.CompleteTask(ctx, acts[0].ID, nil)
			if err := e.RejectTask(ctx, acts[0].ID, RejectPrevious, "", ""); err == nil {
				t.Error("expected error")
			}
		})
		t.Run("驳回指定节点不存在", func(t *testing.T) {
			pi, _ := e.StartProcessInstance(ctx, "err", nil)
			acts, _ := s.ListActiveActivities(ctx, pi.ID)
			if err := e.RejectTask(ctx, acts[0].ID, RejectSpecific, "", "NONEXIST"); err == nil {
				t.Error("expected error")
			}
		})
	}
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
}
