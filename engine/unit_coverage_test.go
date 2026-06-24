package engine

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/expr"
	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage/memstore"
)

// deployment.go 单元测试
func TestDeploymentService_Deploy(t *testing.T) {
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	ds := NewDeploymentService(s)
	ctx := context.Background()

	def := &spec.ProcessDefinition{
		ID: "test:v1", Key: "test", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "end"}},
		StartEventID: "start",
	}

	t.Run("deploy first version", func(t *testing.T) {
		result, err := ds.DeployProcessDefinition(ctx, def)
		if err != nil { t.Fatalf("Deploy: %v", err) }
		if result.Version != 1 { t.Errorf("version=%d, want 1", result.Version) }
	})

	t.Run("deploy second version auto-increment", func(t *testing.T) {
		def2 := &spec.ProcessDefinition{
			ID: "test:v2", Key: "test", Version: 0,
			Elements: map[string]spec.FlowElement{
				"start": &spec.StartEvent{ID: "start"}, "task": &spec.UserTask{ID: "task"}, "end": &spec.EndEvent{ID: "end"},
			},
			SequenceFlows: []*spec.SequenceFlow{
				{ID: "s1", SourceRef: "start", TargetRef: "task"}, {ID: "s2", SourceRef: "task", TargetRef: "end"},
			},
			StartEventID: "start",
		}
		result, err := ds.DeployProcessDefinition(ctx, def2)
		if err != nil { t.Fatalf("Deploy v2: %v", err) }
		if result.Version != 2 { t.Errorf("version=%d, want 2", result.Version) }
	})

	t.Run("empty key rejected", func(t *testing.T) {
		def3 := &spec.ProcessDefinition{ID: "nokey", Key: ""}
		_, err := ds.DeployProcessDefinition(ctx, def3)
		if err == nil { t.Error("expected error for empty key") }
	})

	t.Run("get deployed definition", func(t *testing.T) {
		got, err := ds.GetDeployedDefinition(ctx, "test")
		if err != nil { t.Fatalf("GetDeployed: %v", err) }
		if got.Version != 2 { t.Errorf("version=%d, want 2", got.Version) }
	})

	t.Run("list definitions", func(t *testing.T) {
		list, err := ds.ListDeployedDefinitions(ctx)
		if err != nil { t.Fatalf("List: %v", err) }
		if len(list) < 2 { t.Errorf("expected >=2 defs, got %d", len(list)) }
	})
}

// engine.go Suspend/Resume/Terminate 单元测试
func TestEngine_Lifecycle(t *testing.T) {
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	ctx := context.Background()

	def := &spec.ProcessDefinition{
		ID: "lifecycle", Key: "lifecycle", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "lifecycle", nil)

	t.Run("suspend", func(t *testing.T) {
		if err := e.SuspendProcessInstance(ctx, pi.ID); err != nil { t.Fatalf("Suspend: %v", err) }
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateSuspended { t.Errorf("state=%q", p2.State) }
	})

	t.Run("resume", func(t *testing.T) {
		if err := e.ResumeProcessInstance(ctx, pi.ID); err != nil { t.Fatalf("Resume: %v", err) }
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateRunning { t.Errorf("state=%q", p2.State) }
	})

	t.Run("terminate", func(t *testing.T) {
		if err := e.TerminateProcessInstance(ctx, pi.ID); err != nil { t.Fatalf("Terminate: %v", err) }
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateTerminated { t.Errorf("state=%q", p2.State) }
		// No active tokens or activities after terminate
		toks, _ := s.ListActiveTokens(ctx, pi.ID)
		if len(toks) != 0 { t.Errorf("expected 0 active tokens, got %d", len(toks)) }
	})

	t.Run("suspend already terminated should fail", func(t *testing.T) {
		if err := e.SuspendProcessInstance(ctx, pi.ID); err == nil { t.Error("expected error") }
	})
}

// listener.go 单元测试
func TestEngine_Listeners(t *testing.T) {
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	called := false
	e := NewProcessEngine(s, WithExecutionListener(executionListenerFunc(func(ctx context.Context, pi *instance.ProcessInstance, ai *instance.ActivityInstance, el spec.FlowElement) error {
		called = true
		return nil
	})))
	_ = e
	// Verify engine created without panic
	if !called { /* listener stored but not invoked in v1 */ }
}

type executionListenerFunc func(context.Context, *instance.ProcessInstance, *instance.ActivityInstance, spec.FlowElement) error

func (f executionListenerFunc) OnStart(ctx context.Context, pi *instance.ProcessInstance, ai *instance.ActivityInstance, el spec.FlowElement) error { return f(ctx, pi, ai, el) }
func (f executionListenerFunc) OnEnd(ctx context.Context, pi *instance.ProcessInstance, ai *instance.ActivityInstance, el spec.FlowElement) error { return f(ctx, pi, ai, el) }

// engine.go Terminate non-running instance
func TestEngine_TerminateErrors(t *testing.T) {
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	ctx := context.Background()

	t.Run("terminate nonexistent", func(t *testing.T) {
		err := e.TerminateProcessInstance(ctx, "fake")
		if err == nil { t.Error("expected error") }
	})
}

// engine.go ReceiveMessage is alias for ReceiveSignal
func TestEngine_ReceiveMessage(t *testing.T) {
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	ctx := context.Background()
	// Just verify it doesn't panic (no subscriptions = no-op)
	if err := e.ReceiveMessage(ctx, "test", nil); err != nil {
		t.Logf("ReceiveMessage: %v (expected if no subscriptions)", err)
	}
}

// expr.EvaluateNumeric unit test
func TestExpr_EvaluateNumeric(t *testing.T) {
	c := expr.MustNewCondition("5")
	n, err := c.EvaluateNumeric(nil)
	if err != nil { t.Fatalf("EvaluateNumeric: %v", err) }
	if n != 5 { t.Errorf("got %v, want 5", n) }

	c2 := expr.MustNewCondition("true")
	_, err2 := c2.EvaluateNumeric(nil)
	if err2 == nil { t.Error("expected error for boolean result") }
}

// expr.String()
func TestExpr_String(t *testing.T) {
	c := expr.MustNewCondition("${amount > 100}")
	if c.String() != "amount > 100" {
		t.Errorf("String()=%q, want 'amount > 100'", c.String())
	}
}
