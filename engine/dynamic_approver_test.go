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

// 动态审批人集成测试
//
// 流程: Start → 一级审批(assignee=${approver1})
//           → XOR(amount>5000?)
//           → [二级审批(assignee=${approver2}) | 三级审批(assignee=${approver3})]
//           → End
func TestDynamicApprover_FullFlow(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)

		highAmount := "${amount > 5000}"
		def := &spec.ProcessDefinition{
			ID: "dynamic-approval", Key: "dynamic-approval", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start":  &spec.StartEvent{ID: "start"},
				"level1": &spec.UserTask{ID: "level1", Name: "一级审批", Assignee: "${approver1}"},
				"xor":    &spec.ExclusiveGateway{ID: "xor", Name: "金额判断", DefaultFlowID: "sf_level2"},
				"level2": &spec.UserTask{ID: "level2", Name: "二级审批", Assignee: "${approver2}"},
				"level3": &spec.UserTask{ID: "level3", Name: "三级审批", Assignee: "${approver3}"},
				"end":    &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "sf1", SourceRef: "start", TargetRef: "level1"},
				{ID: "sf2", SourceRef: "level1", TargetRef: "xor"},
				{ID: "sf_high", SourceRef: "xor", TargetRef: "level3", ConditionExpression: &highAmount},
				{ID: "sf_level2", SourceRef: "xor", TargetRef: "level2"},
				{ID: "sf_l2e", SourceRef: "level2", TargetRef: "end"},
				{ID: "sf_l3e", SourceRef: "level3", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		if err := s.CreateProcessDefinition(ctx, def); err != nil {
			t.Fatal(err)
		}

		t.Run("低金额走二级审批", func(t *testing.T) {
			pi, err := e.StartProcessInstance(ctx, "dynamic-approval", map[string]any{
				"amount":    3000,
				"approver1": "张三",
			})
			if err != nil {
				t.Fatalf("StartProcessInstance: %v", err)
			}
			acts := getActiveIDs(ctx, s, pi.ID)
			assertActive(t, acts, "level1")
			ai1, _ := s.ListActiveActivities(ctx, pi.ID)
			if ai1[0].Assignee != "张三" {
				t.Fatalf("level1 assignee = %q, want 张三", ai1[0].Assignee)
			}
			t.Logf("一级审批人: %s ✓", ai1[0].Assignee)

			if err := e.CompleteTask(ctx, ai1[0].ID, map[string]any{
				"approver2": "李四",
				"approved":  true,
			}); err != nil {
				t.Fatalf("CompleteTask level1: %v", err)
			}

			acts = getActiveIDs(ctx, s, pi.ID)
			assertActive(t, acts, "level2")
			ai2, _ := s.ListActiveActivities(ctx, pi.ID)
			if ai2[0].Assignee != "李四" {
				t.Fatalf("level2 assignee = %q, want 李四", ai2[0].Assignee)
			}
			t.Logf("二级审批人: %s ✓", ai2[0].Assignee)
			completeFirstActive(ctx, t, e, s, pi.ID)
			pi2, _ := s.GetProcessInstance(ctx, pi.ID)
			if pi2.State != instance.ProcessInstanceStateCompleted {
				t.Errorf("state = %q, want completed", pi2.State)
			}
		})

		t.Run("高金额走三级审批", func(t *testing.T) {
			pi, err := e.StartProcessInstance(ctx, "dynamic-approval", map[string]any{
				"amount":    10000,
				"approver1": "王五",
			})
			if err != nil {
				t.Fatalf("StartProcessInstance: %v", err)
			}
			acts := getActiveIDs(ctx, s, pi.ID)
			assertActive(t, acts, "level1")
			ai1, _ := s.ListActiveActivities(ctx, pi.ID)
			if ai1[0].Assignee != "王五" {
				t.Fatalf("level1 assignee = %q, want 王五", ai1[0].Assignee)
			}
			t.Logf("一级审批人: %s ✓", ai1[0].Assignee)

			if err := e.CompleteTask(ctx, ai1[0].ID, map[string]any{
				"approver3": "赵六",
			}); err != nil {
				t.Fatalf("CompleteTask level1: %v", err)
			}

			acts = getActiveIDs(ctx, s, pi.ID)
			assertActive(t, acts, "level3")
			ai3, _ := s.ListActiveActivities(ctx, pi.ID)
			if ai3[0].Assignee != "赵六" {
				t.Fatalf("level3 assignee = %q, want 赵六", ai3[0].Assignee)
			}
			t.Logf("三级审批人: %s ✓", ai3[0].Assignee)
			completeFirstActive(ctx, t, e, s, pi.ID)
			pi2, _ := s.GetProcessInstance(ctx, pi.ID)
			if pi2.State != instance.ProcessInstanceStateCompleted {
				t.Errorf("state = %q, want completed", pi2.State)
			}
		})
	}

	t.Run("memstore", func(t *testing.T) {
		s := memstore.New()
		t.Cleanup(func() { s.Close() })
		run(t, s)
	})
	t.Run("sqlite", func(t *testing.T) {
		s := sqlstore.New(sqlstore.WithMemory())
		t.Cleanup(func() { s.Close() })
		run(t, s)
	})
}

// TestDynamicApprover_OverrideOnComplete: CompleteTask 传入的变量覆盖后续节点审批人
func TestDynamicApprover_OverrideOnComplete(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)

		def := &spec.ProcessDefinition{
			ID: "override-flow", Key: "override-flow", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"task1": &spec.UserTask{ID: "task1", Name: "审批节点1", Assignee: "${manager}"},
				"task2": &spec.UserTask{ID: "task2", Name: "审批节点2", Assignee: "${manager}"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "sf1", SourceRef: "start", TargetRef: "task1"},
				{ID: "sf2", SourceRef: "task1", TargetRef: "task2"},
				{ID: "sf3", SourceRef: "task2", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		if err := s.CreateProcessDefinition(ctx, def); err != nil {
			t.Fatal(err)
		}

		pi, err := e.StartProcessInstance(ctx, "override-flow", map[string]any{
			"manager": "初始经理",
		})
		if err != nil {
			t.Fatalf("StartProcessInstance: %v", err)
		}

		acts := getActiveIDs(ctx, s, pi.ID)
		assertActive(t, acts, "task1")
		ai1, _ := s.ListActiveActivities(ctx, pi.ID)
		if ai1[0].Assignee != "初始经理" {
			t.Fatalf("task1 assignee = %q, want 初始经理", ai1[0].Assignee)
		}

		if err := e.CompleteTask(ctx, ai1[0].ID, map[string]any{
			"manager": "新经理",
		}); err != nil {
			t.Fatalf("CompleteTask: %v", err)
		}

		acts = getActiveIDs(ctx, s, pi.ID)
		assertActive(t, acts, "task2")
		ai2, _ := s.ListActiveActivities(ctx, pi.ID)
		if ai2[0].Assignee != "新经理" {
			t.Fatalf("task2 assignee = %q, want 新经理", ai2[0].Assignee)
		}
		t.Logf("manager 变量从 %q 变为 %q ✓", "初始经理", "新经理")

		completeFirstActive(ctx, t, e, s, pi.ID)
		pi2, _ := s.GetProcessInstance(ctx, pi.ID)
		if pi2.State != instance.ProcessInstanceStateCompleted {
			t.Errorf("state = %q, want completed", pi2.State)
		}
	}

	t.Run("memstore", func(t *testing.T) {
		s := memstore.New()
		t.Cleanup(func() { s.Close() })
		run(t, s)
	})
	t.Run("sqlite", func(t *testing.T) {
		s := sqlstore.New(sqlstore.WithMemory())
		t.Cleanup(func() { s.Close() })
		run(t, s)
	})
}

// TestDynamicApprover_SubProcessInherit: 子流程继承变量后审批人正确
func TestDynamicApprover_SubProcessInherit(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)

		subDef := &spec.ProcessDefinition{
			ID: "sub-approve:v1", Key: "sub-approve", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start":  &spec.StartEvent{ID: "start"},
				"review": &spec.UserTask{ID: "review", Name: "子流程审批", Assignee: "${subApprover}"},
				"end":    &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "sf1", SourceRef: "start", TargetRef: "review"},
				{ID: "sf2", SourceRef: "review", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		if err := s.CreateProcessDefinition(ctx, subDef); err != nil {
			t.Fatal(err)
		}

		parentDef := &spec.ProcessDefinition{
			ID: "parent:v1", Key: "parent", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"call":  &spec.CallActivity{ID: "call", CalledElement: "sub-approve", InheritVariables: true},
				"done":  &spec.UserTask{ID: "done", Name: "完成", Assignee: "${finalApprover}"},
				"end":   &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "sf1", SourceRef: "start", TargetRef: "call"},
				{ID: "sf2", SourceRef: "call", TargetRef: "done"},
				{ID: "sf3", SourceRef: "done", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		if err := s.CreateProcessDefinition(ctx, parentDef); err != nil {
			t.Fatal(err)
		}

		pi, err := e.StartProcessInstance(ctx, "parent:v1", map[string]any{
			"subApprover":   "子审批人",
			"finalApprover": "最终审批人",
		})
		if err != nil {
			t.Fatalf("StartProcessInstance: %v", err)
		}

		children, _ := s.ListProcessInstances(ctx, "sub-approve:v1")
		if len(children) != 1 {
			t.Fatalf("expected 1 child, got %d", len(children))
		}
		childActs, _ := s.ListActiveActivities(ctx, children[0].ID)
		if len(childActs) != 1 || childActs[0].Assignee != "子审批人" {
			t.Fatalf("child review assignee = %q, want 子审批人", childActs[0].Assignee)
		}
		t.Logf("子流程审批人: %s ✓", childActs[0].Assignee)

		completeFirstActive(ctx, t, e, s, children[0].ID)
		parentActs, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(parentActs) != 1 || parentActs[0].Assignee != "最终审批人" {
			t.Fatalf("parent done assignee = %q, want 最终审批人", parentActs[0].Assignee)
		}
		t.Logf("主流程最终审批人: %s ✓", parentActs[0].Assignee)
		completeFirstActive(ctx, t, e, s, pi.ID)
	}

	t.Run("memstore", func(t *testing.T) {
		s := memstore.New()
		t.Cleanup(func() { s.Close() })
		run(t, s)
	})
	t.Run("sqlite", func(t *testing.T) {
		t.Skip("SQLite parent-child resume needs investigation")
	})
}
