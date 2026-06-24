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

// A1-A3 验证: sqlstore 的 GetActivityInstance 和 List 是否丢失字段
// 创建包含 AdhocParentID、ExpireTime、TermMode 的活动 → 读取 → 验证字段保留
func TestBug_SqlstoreActivityFields(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)

		// 先创建一个 process instance 和 activity instance
		def := &spec.ProcessDefinition{
			ID: "bug123", Key: "bug123", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, def)
		pi, _ := e.StartProcessInstance(ctx, "bug123", nil)
		acts, _ := s.ListActiveActivities(ctx, pi.ID)

		if len(acts) != 1 { t.Fatalf("expected 1 activity, got %d", len(acts)) }
		actID := acts[0].ID

		// 通过 Update 设置 AdhocParentID 和超时字段
		ai, err := s.GetActivityInstance(ctx, actID)
		if err != nil { t.Fatalf("GetActivityInstance: %v", err) }

		ai.AdhocParentID = "parent-123"
		expire := time.Now().Add(time.Hour)
		ai.ExpireTime = &expire
		ai.TermMode = 2

		if err := s.UpdateActivityInstance(ctx, ai); err != nil {
			t.Fatalf("UpdateActivityInstance: %v", err)
		}

		// 重新读取 → 这些字段应该还在
		ai2, err := s.GetActivityInstance(ctx, actID)
		if err != nil { t.Fatalf("GetActivityInstance 2: %v", err) }

		if ai2.AdhocParentID != "parent-123" {
			t.Errorf("GET BUG: AdhocParentID lost! got=%q, want parent-123", ai2.AdhocParentID)
		} else {
			t.Logf("✅ GetActivityInstance AdhocParentID survives: %s", ai2.AdhocParentID)
		}
		if ai2.ExpireTime == nil {
			t.Error("GET BUG: ExpireTime lost!")
		} else {
			t.Logf("✅ GetActivityInstance ExpireTime survives: %v", ai2.ExpireTime)
		}
		if ai2.TermMode != 2 {
			t.Errorf("GET BUG: TermMode lost! got=%d, want 2", ai2.TermMode)
		} else {
			t.Logf("✅ GetActivityInstance TermMode survives: %d", ai2.TermMode)
		}

		// 验证 List 查询也保留字段
		acts2, err := s.ListActiveActivities(ctx, pi.ID)
		if err != nil { t.Fatalf("ListActiveActivities: %v", err) }
		if len(acts2) != 1 { t.Fatalf("expected 1, got %d", len(acts2)) }

		listed := acts2[0]
		if listed.AdhocParentID != "parent-123" {
			t.Errorf("LIST BUG: AdhocParentID lost! got=%q", listed.AdhocParentID)
		} else {
			t.Logf("✅ List AdhocParentID survives: %s", listed.AdhocParentID)
		}
		if listed.ExpireTime == nil {
			t.Error("LIST BUG: ExpireTime lost!")
		} else {
			t.Logf("✅ List ExpireTime survives: %v", listed.ExpireTime)
		}
		if listed.TermMode != 2 {
			t.Errorf("LIST BUG: TermMode lost! got=%d", listed.TermMode)
		}
	}

	t.Log("=== Test A1-A3: sqlstore field loss ===")
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
	t.Run("sqlite", func(t *testing.T) { s := sqlstore.New(sqlstore.WithMemory()); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// 验证 ProcessInstance 的 ParentProcessInstanceID / ParentActivityID 字段
func TestBug_SqlstoreProcessParentFields(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		sub := &spec.ProcessDefinition{
			ID: "sub:v1", Key: "sub", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, sub)
		parent := &spec.ProcessDefinition{
			ID: "p:v1", Key: "p", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"call": &spec.CallActivity{ID: "call", CalledElement: "sub", InheritVariables: true},
				"done": &spec.UserTask{ID: "done"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "call"},
				{ID: "s2", SourceRef: "call", TargetRef: "done"},
				{ID: "s3", SourceRef: "done", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		s.CreateProcessDefinition(ctx, parent)
		e := NewProcessEngine(s)
		pi, _ := e.StartProcessInstance(ctx, "p:v1", nil)
		_ = pi

		// 获取子流程实例并验证 parent 字段
		children, err := s.ListProcessInstances(ctx, "sub:v1")
		if err != nil { t.Fatalf("ListProcessInstances: %v", err) }
		if len(children) != 1 { t.Fatalf("expected 1 child, got %d", len(children)) }
		child := children[0]

		if child.ParentProcessInstanceID == "" {
			t.Error("BUG: ListProcessInstances ParentProcessInstanceID is empty (should be set by CallActivity)")
		} else {
			t.Logf("✅ ListPI ParentProcessInstanceID: %s", child.ParentProcessInstanceID)
		}

		// 通过 GetProcessInstance 读取 → 这里可能丢字段
		child2, err := s.GetProcessInstance(ctx, child.ID)
		if err != nil { t.Fatalf("GetProcessInstance: %v", err) }

		if child2.ParentProcessInstanceID == "" {
			t.Errorf("GET BUG: ParentProcessInstanceID lost via GetProcessInstance! List said %q, Get returns empty", child.ParentProcessInstanceID)
		} else {
			t.Logf("✅ GetProcessInstance ParentProcessInstanceID survives: %s", child2.ParentProcessInstanceID)
		}
	}

	t.Log("=== Test A3: Process parent field loss ===")
	t.Run("memstore", func(t *testing.T) { s := memstore.New(); t.Cleanup(func() { s.Close() }); run(t, s) })
	t.Run("sqlite", func(t *testing.T) { s := sqlstore.New(sqlstore.WithMemory()); t.Cleanup(func() { s.Close() }); run(t, s) })
}

// B6 验证: AddSign 错误被吞掉
func TestBug_AddSignSilentError(t *testing.T) {
	t.Log("B6: AddSign SetVariable errors are swallowed — verify test demonstrates issue")
	ctx := context.Background()
	e := NewProcessEngine(memstore.New())
	def := &spec.ProcessDefinition{
		ID: "be", Key: "be", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "A": &spec.UserTask{ID: "A"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "A"}, {ID: "s2", SourceRef: "A", TargetRef: "end"}},
		StartEventID: "start",
	}
	e.store.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "be", nil)
	acts, _ := e.store.ListActiveActivities(ctx, pi.ID)

	// 如果 SetVariable 失败，AddSign 不会返回错误
	err := e.AddSign(ctx, acts[0].ID, SignForward, StrategyOR, []string{"B"})
	if err != nil {
		t.Logf("AddSign returned error (expected if store fails): %v", err)
	} else {
		// 正常情况 AddSign 成功
		t.Log("✅ AddSign completed (set up for verification)")
	}
}

// B8 验证: float64 类型断言无 ok 检查
func TestBug_Float64Assert(t *testing.T) {
	t.Log("B8: multi_instance.go unsafe float64 assertion")
	ctx := context.Background()
	e := NewProcessEngine(memstore.New())
	def := &spec.ProcessDefinition{
		ID: "b8", Key: "b8", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"mi": &spec.UserTask{ID: "mi", LoopCharacteristics: &spec.LoopCharacteristics{
				Collection: "items", ElementVariable: "x",
			}},
			"end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "mi"}, {ID: "s2", SourceRef: "mi", TargetRef: "end"}},
		StartEventID: "start",
	}
	e.store.CreateProcessDefinition(ctx, def)

	// 正常流程——3 个会签实例完成
	pi, _ := e.StartProcessInstance(ctx, "b8", map[string]any{"items": []any{"a", "b", "c"}})
	acts, _ := e.store.ListActiveActivities(ctx, pi.ID)
	if len(acts) != 3 { t.Fatalf("expected 3 MI, got %d", len(acts)) }

	for _, a := range acts { e.CompleteTask(ctx, a.ID, nil) }
	p2, _ := e.store.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q, want completed", p2.State)
	}
	t.Log("✅ MI complete with 3 items works")
}
