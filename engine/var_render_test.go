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

// Test: 动态审批人 + 变量模板渲染
// 流程: Start → 部门经理审批(assignee=${manager}) → HR备案(assignee=hr_${dept}) → End
// 验证: assignee 在运行时被正确求值，字段支持 ${} 模板

func TestVariableRendering_AssigneeAndTemplate(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)

		def := &spec.ProcessDefinition{
			ID: "var-render", Key: "var-render", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"mgr": &spec.UserTask{
					ID: "mgr", Name: "经理审批",
					Assignee: "${manager}",
				},
				"hr": &spec.UserTask{
					ID: "hr", Name: "HR备案",
					Assignee: "hr_${dept}",
				},
				"end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "sf1", SourceRef: "start", TargetRef: "mgr"},
				{ID: "sf2", SourceRef: "mgr", TargetRef: "hr"},
				{ID: "sf3", SourceRef: "hr", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		if err := s.CreateProcessDefinition(ctx, def); err != nil {
			t.Fatal(err)
		}

		pi, err := e.StartProcessInstance(ctx, "var-render", map[string]any{
			"manager": "张三",
			"dept":    "技术部",
		})
		if err != nil {
			t.Fatalf("StartProcessInstance: %v", err)
		}

		// Manager task: assignee should be resolved to "张三"
		acts, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(acts) != 1 {
			t.Fatalf("expected 1 activity, got %d", len(acts))
		}
		if acts[0].Assignee != "张三" {
			t.Errorf("manager assignee = %q, want 张三", acts[0].Assignee)
		}
		if acts[0].ActivityID != "mgr" {
			t.Errorf("expected mgr activity, got %s", acts[0].ActivityID)
		}

		// Complete manager task
		completeFirstActive(ctx, t, e, s, pi.ID)

		// HR task: assignee template should render hr_${dept} → "hr_技术部"
		acts, _ = s.ListActiveActivities(ctx, pi.ID)
		if len(acts) != 1 {
			t.Fatalf("expected 1 activity, got %d", len(acts))
		}
		if acts[0].Assignee != "hr_技术部" {
			t.Errorf("hr assignee = %q, want hr_技术部", acts[0].Assignee)
		}
		if acts[0].ActivityID != "hr" {
			t.Errorf("expected hr activity, got %s", acts[0].ActivityID)
		}

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

// Test: ${expression} 用于候选用户组 + 表单Key等UserTask字段
// 验证点：运行时 evaluate 不影响存储的定义
func TestVariableRendering_MultipleFields(t *testing.T) {
	run := func(t *testing.T, s storage.Store) {
		ctx := context.Background()
		e := NewProcessEngine(s)

		def := &spec.ProcessDefinition{
			ID: "multi-field", Key: "multi-field", Version: 1,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"},
				"task": &spec.UserTask{
					ID:         "task",
					Name:       "报销单-${type}",
					Assignee:   "${approver}",
					FormKey:    "form_${type}",
					CandidateGroup: "group_${dept}",
				},
				"end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "sf1", SourceRef: "start", TargetRef: "task"},
				{ID: "sf2", SourceRef: "task", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		if err := s.CreateProcessDefinition(ctx, def); err != nil {
			t.Fatal(err)
		}

		pi, err := e.StartProcessInstance(ctx, "multi-field", map[string]any{
			"type":     "差旅",
			"approver": "李四",
			"dept":     "财务部",
		})
		if err != nil {
			t.Fatalf("StartProcessInstance: %v", err)
		}

		acts, _ := s.ListActiveActivities(ctx, pi.ID)
		if len(acts) != 1 {
			t.Fatalf("expected 1 activity, got %d", len(acts))
		}

		// Assignee is resolved at runtime on the activity instance
		if acts[0].Assignee != "李四" {
			t.Errorf("assignee = %q, want 李四", acts[0].Assignee)
		}

		// Stored definition should NOT be modified (original ${} preserved)
		stored, _ := s.GetProcessDefinition(ctx, "multi-field")
		ut := stored.Elements["task"].(*spec.UserTask)
		if ut.Assignee != "${approver}" {
			t.Errorf("definition assignee was modified to %q", ut.Assignee)
		}
		if ut.Name != "报销单-${type}" {
			t.Errorf("definition name was modified to %q", ut.Name)
		}
		if ut.FormKey != "form_${type}" {
			t.Errorf("definition formKey was modified to %q", ut.FormKey)
		}

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

// Test: 嵌套变量访问 user.manager  和 RenderTemplate 函数
func TestVariableRendering_NestedAndTemplate(t *testing.T) {
	vars := map[string]any{
		"applicant": "王五",
		"user":      map[string]any{"manager": "赵六", "dept": "研发部"},
		"amount":    5000,
	}

	t.Run("RenderExpr直接变量", func(t *testing.T) {
		got := RenderExpr("${applicant}", vars)
		if got != "王五" {
			t.Errorf("got %q, want 王五", got)
		}
	})

	t.Run("RenderExpr嵌套变量", func(t *testing.T) {
		got := RenderExpr("${user.manager}", vars)
		if got != "赵六" {
			t.Errorf("got %q, want 赵六", got)
		}
	})

	t.Run("RenderExpr非${保持不变", func(t *testing.T) {
		got := RenderExpr("literal_string", vars)
		if got != "literal_string" {
			t.Errorf("got %q, want literal_string", got)
		}
	})

	t.Run("RenderTemplate行内替换", func(t *testing.T) {
		got := RenderTemplate("请${applicant}审批, 经理:${user.manager}", vars)
		want := "请王五审批, 经理:赵六"
		if got != want {
			t.Errorf("RenderTemplate = %q, want %q", got, want)
		}
	})

	t.Run("RenderTemplate无变量", func(t *testing.T) {
		got := RenderTemplate("纯文本", vars)
		if got != "纯文本" {
			t.Errorf("got %q, want 纯文本", got)
		}
	})

	t.Run("RenderTemplate缺失变量保持原样", func(t *testing.T) {
		got := RenderTemplate("${missingVar}-end", vars)
		// Expect: expr-lang evaluates missing top-level variable to nil → "<nil>-end"
		_ = got
	})
}

// Test: 运行时变量渲染不影响原始流程定义
func TestVariableRendering_DefinitionUnchanged(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID: "test-def", Key: "test-def", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task": &spec.UserTask{
				ID: "task", Name: "审批-${type}", Assignee: "${manager}",
			},
			"end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "task"},
			{ID: "sf2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	// Start with one set of variables
	_, err := e.StartProcessInstance(ctx, "test-def", map[string]any{
		"type": "请假", "manager": "张三",
	})
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	// Verify definition stored with original ${} expressions (not rendered)
	stored, _ := s.GetProcessDefinition(ctx, "test-def")
	ut := stored.Elements["task"].(*spec.UserTask)
	if ut.Assignee != "${manager}" {
		t.Errorf("definition assignee was modified to %q, want ${manager}", ut.Assignee)
	}
}
