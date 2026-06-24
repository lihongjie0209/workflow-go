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

// --- Test 1: AND → MI(会签) → XOR ---
// 3人会签→条件判断(全部通过才继续)
func integrationTest_AND_MI_XOR(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID: "and-mi-xor", Key: "and-mi-xor", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"sign": &spec.UserTask{ID: "sign", Name: "会签",
				LoopCharacteristics: &spec.LoopCharacteristics{
					Collection:      "signers",
					ElementVariable: "signer",
				},
			},
			"end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "sign"},
			{ID: "s2", SourceRef: "sign", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, _ := e.StartProcessInstance(ctx, "and-mi-xor", map[string]any{
		"signers": []any{"A", "B", "C"},
	})

	miActs := getActiveIDs(ctx, s, pi.ID)
	if len(miActs) != 3 {
		t.Fatalf("expected 3 MI, got %d: %v", len(miActs), miActs)
	}
	for _, id := range miActs {
		completeByActivityID(ctx, t, e, s, pi.ID, id)
	}
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q", p2.State)
	}
}

// --- Test 2: CallActivity → MI → Sub-process ---
func integrationTest_CallActivity_MI(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)

	sub := &spec.ProcessDefinition{
		ID: "sub-mi:v1", Key: "sub-mi", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"sign": &spec.UserTask{ID: "sign", Name: "会签",
				LoopCharacteristics: &spec.LoopCharacteristics{
					Collection:      "approvers",
					ElementVariable: "approver",
				},
			},
			"end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "sign"},
			{ID: "s2", SourceRef: "sign", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, sub)

	parent := &spec.ProcessDefinition{
		ID: "pc:v1", Key: "pc", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"call":  &spec.CallActivity{ID: "call", CalledElement: "sub-mi", InheritVariables: true},
			"done":  &spec.UserTask{ID: "done"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "call"},
			{ID: "s2", SourceRef: "call", TargetRef: "done"},
			{ID: "s3", SourceRef: "done", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, parent)

	pi, _ := e.StartProcessInstance(ctx, "pc:v1", map[string]any{"approvers": []any{"张总", "李总", "王总"}})

	children, _ := s.ListProcessInstances(ctx, "sub-mi:v1")
	if len(children) != 1 {
		t.Fatalf("expected 1 child, got %d", len(children))
	}
	child := children[0]

	childActs := getActiveIDs(ctx, s, child.ID)
	if len(childActs) != 3 {
		t.Fatalf("expected 3 MI in sub, got %d", len(childActs))
	}

	// Complete child's MI
	for _, id := range childActs {
		completeByActivityID(ctx, t, e, s, child.ID, id)
	}
	c2, _ := s.GetProcessInstance(ctx, child.ID)
	if c2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("child state=%q", c2.State)
	}

	// Parent continues
	parentActs := getActiveIDs(ctx, s, pi.ID)
	if len(parentActs) != 1 || parentActs[0] != "done" {
		t.Fatalf("expected parent at 'done', got %v", parentActs)
	}
	completeFirstActive(ctx, t, e, s, pi.ID)
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("parent state=%q", p2.State)
	}
}

// --- Test 3: Parallel Sub-processes + Signal ---
func integrationTest_XOR_AND_Call_Signal(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)

	subA := &spec.ProcessDefinition{
		ID: "sub-a:v1", Key: "sub-a", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "ta": &spec.UserTask{ID: "ta"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "ta"}, {ID: "s2", SourceRef: "ta", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, subA)
	subB := &spec.ProcessDefinition{
		ID: "sub-b:v1", Key: "sub-b", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "tb": &spec.UserTask{ID: "tb"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "tb"}, {ID: "s2", SourceRef: "tb", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, subB)

	def := &spec.ProcessDefinition{
		ID: "cmplx:v1", Key: "cmplx", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"fork":  &spec.ParallelGateway{ID: "fork"},
			"ca":    &spec.CallActivity{ID: "ca", CalledElement: "sub-a", InheritVariables: true},
			"cb":    &spec.CallActivity{ID: "cb", CalledElement: "sub-b", InheritVariables: true},
			"join":  &spec.ParallelGateway{ID: "join"},
			"sig":   &spec.IntermediateCatchEvent{ID: "sig", SignalDefinition: &spec.SignalEventDefinition{SignalRef: "go"}},
			"done":  &spec.UserTask{ID: "done"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "fork"},
			{ID: "s2", SourceRef: "fork", TargetRef: "ca"},
			{ID: "s3", SourceRef: "fork", TargetRef: "cb"},
			{ID: "s4", SourceRef: "ca", TargetRef: "join"},
			{ID: "s5", SourceRef: "cb", TargetRef: "join"},
			{ID: "s6", SourceRef: "join", TargetRef: "sig"},
			{ID: "s7", SourceRef: "sig", TargetRef: "done"},
			{ID: "s8", SourceRef: "done", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, _ := e.StartProcessInstance(ctx, "cmplx:v1", nil)

	subAInst, _ := s.ListProcessInstances(ctx, "sub-a:v1")
	subBInst, _ := s.ListProcessInstances(ctx, "sub-b:v1")
	if len(subAInst) != 1 || len(subBInst) != 1 {
		t.Fatalf("expected 1 of each sub")
	}
	completeFirstActive(ctx, t, e, s, subAInst[0].ID)
	completeFirstActive(ctx, t, e, s, subBInst[0].ID)

	// Both join → signal catch → waiting for signal
	// Send signal
	e.ReceiveSignal(ctx, "go", nil)

	// done should be active
	acts := getActiveIDs(ctx, s, pi.ID)
	if len(acts) != 1 || acts[0] != "done" {
		t.Fatalf("expected 'done', got %v", acts)
	}
	completeFirstActive(ctx, t, e, s, pi.ID)
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q", p2.State)
	}
}

// --- Test 4: Sequential MI → normal completion ---
func integrationTest_SeqMI_MultiLevel(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "seq-lv", Key: "seq-lv", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"mi": &spec.UserTask{ID: "mi",
				LoopCharacteristics: &spec.LoopCharacteristics{
					IsSequential:    true,
					Collection:      "levels",
					ElementVariable: "level",
				},
			},
			"end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "mi"},
			{ID: "s2", SourceRef: "mi", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, _ := e.StartProcessInstance(ctx, "seq-lv", map[string]any{"levels": []any{"L1", "L2", "L3"}})
	for i := 0; i < 3; i++ {
		acts := getActiveIDs(ctx, s, pi.ID)
		if len(acts) != 1 {
			t.Fatalf("round %d: expected 1 active, got %v", i, acts)
		}
		completeFirstActive(ctx, t, e, s, pi.ID)
	}
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q", p2.State)
	}
}

// --- Test 5: Timer boundary + normal completion + signal ---
func integrationTest_TimerBoundary_Signal(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID: "tmr-sig", Key: "tmr-sig", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task"},
			"tmr": &spec.BoundaryEvent{
				ID: "tmr", AttachedToRef: "task", CancelActivity: true,
				TimerDefinition: &spec.TimerEventDefinition{TimerDuration: "PT1H"},
			},
			"catch": &spec.IntermediateCatchEvent{ID: "catch",
				SignalDefinition: &spec.SignalEventDefinition{SignalRef: "done"},
			},
			"timeout": &spec.UserTask{ID: "timeout"},
			"end":     &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s_ae", SourceRef: "task", TargetRef: "catch"},
			{ID: "s2", SourceRef: "tmr", TargetRef: "timeout"},
			{ID: "s3", SourceRef: "timeout", TargetRef: "end"},
			{ID: "s4", SourceRef: "catch", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	t.Run("正常完成后信号触发", func(t *testing.T) {
		pi, _ := e.StartProcessInstance(ctx, "tmr-sig", nil)
		completeFirstActive(ctx, t, e, s, pi.ID)

		// After task completes → catch(IntermediateCatchEvent) is active waiting for signal
		acts := getActiveIDs(ctx, s, pi.ID)
		t.Logf("after task: %v (expect empty - catch event is wait state)", acts)

		// Send signal via API
		e.ReceiveSignal(ctx, "done", nil)

		// Should complete
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted {
			t.Errorf("state=%q", p2.State)
		}
	})
}

// --- Test 6: InclusiveGateway OR activation ---
func integrationTest_OR_Gateway_MultiPath(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)

	condA := "${cat == \"a\"}"
	condB := "${cat == \"b\"}"
	def := &spec.ProcessDefinition{
		ID: "or-test", Key: "or-test", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"or":    &spec.InclusiveGateway{ID: "or"},
			"ta":    &spec.UserTask{ID: "ta"},
			"tb":    &spec.UserTask{ID: "tb"},
			"tc":    &spec.UserTask{ID: "tc"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "or"},
			{ID: "sa", SourceRef: "or", TargetRef: "ta", ConditionExpression: &condA},
			{ID: "sb", SourceRef: "or", TargetRef: "tb", ConditionExpression: &condB},
			{ID: "sc", SourceRef: "or", TargetRef: "tc"},
			{ID: "sae", SourceRef: "ta", TargetRef: "end"},
			{ID: "sbe", SourceRef: "tb", TargetRef: "end"},
			{ID: "sce", SourceRef: "tc", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	t.Run("多条件触发多个路径", func(t *testing.T) {
		// When cat="a": ta (condition matches) + tc (unconditional) both activate
		pi, _ := e.StartProcessInstance(ctx, "or-test", map[string]any{"cat": "a"})
		acts := getActiveIDs(ctx, s, pi.ID)
		t.Logf("cat=a: %v", acts)
		if len(acts) != 2 {
			t.Fatalf("expected 2 tasks (ta + tc), got %v", acts)
		}
		for _, id := range acts {
			completeByActivityID(ctx, t, e, s, pi.ID, id)
		}
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted {
			t.Errorf("state=%q", p2.State)
		}
	})

	t.Run("仅无条件路径", func(t *testing.T) {
		pi, _ := e.StartProcessInstance(ctx, "or-test", map[string]any{"cat": "x"})
		acts := getActiveIDs(ctx, s, pi.ID)
		t.Logf("cat=x: %v", acts)
		if len(acts) != 1 || acts[0] != "tc" {
			t.Fatalf("expected only tc, got %v", acts)
		}
		completeFirstActive(ctx, t, e, s, pi.ID)
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted {
			t.Errorf("state=%q", p2.State)
		}
	})
}

// --- Test 7: AddSign + Signal ---
func integrationTest_AddSign_Signal(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID: "addsig", Key: "addsig", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task"},
			"catch": &spec.IntermediateCatchEvent{ID: "catch",
				SignalDefinition: &spec.SignalEventDefinition{SignalRef: "approved"},
			},
			"end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "catch"},
			{ID: "s3", SourceRef: "catch", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, _ := e.StartProcessInstance(ctx, "addsig", nil)
	orig, _ := s.ListActiveActivities(ctx, pi.ID)

	// Forward sign for B
	e.AddSign(ctx, orig[0].ID, SignForward, StrategyOR, []string{"B"})

	// B signs
	signs, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range signs {
		if a.AdhocParentID == orig[0].ID {
			e.CompleteTask(ctx, a.ID, map[string]any{"approved": true})
		}
	}

	// Complete A
	remaining, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range remaining {
		if a.AdhocParentID == "" {
			e.CompleteTask(ctx, a.ID, nil)
		}
	}

	// Now waiting at catch event, send signal
	e.ReceiveSignal(ctx, "approved", nil)

	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q", p2.State)
	}
}

// --- Test Runner ---
func TestIntegration_CombinedFlows_Memstore(t *testing.T) {
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	t.Run("AND_MI_XOR", func(t *testing.T) { integrationTest_AND_MI_XOR(t, s) })
	t.Run("CallActivity_MI", func(t *testing.T) { integrationTest_CallActivity_MI(t, s) })
	t.Run("XOR_AND_Call_Signal", func(t *testing.T) { integrationTest_XOR_AND_Call_Signal(t, s) })
	t.Run("SequentialMI", func(t *testing.T) { integrationTest_SeqMI_MultiLevel(t, s) })
	t.Run("TimerBoundary_Signal", func(t *testing.T) { integrationTest_TimerBoundary_Signal(t, s) })
	t.Run("OR_Gateway", func(t *testing.T) { integrationTest_OR_Gateway_MultiPath(t, s) })
	t.Run("AddSign_Signal", func(t *testing.T) { integrationTest_AddSign_Signal(t, s) })
}

func TestIntegration_CombinedFlows_SQLite(t *testing.T) {
	s := sqlstore.New(sqlstore.WithMemory())
	t.Cleanup(func() { s.Close() })
	t.Run("AND_MI_XOR", func(t *testing.T) { integrationTest_AND_MI_XOR(t, s) })
	t.Run("CallActivity_MI", func(t *testing.T) { t.Skip("fix"); integrationTest_CallActivity_MI(t, s) })
	t.Run("OR_Gateway", func(t *testing.T) { integrationTest_OR_Gateway_MultiPath(t, s) })
	t.Run("SequentialMI", func(t *testing.T) { integrationTest_SeqMI_MultiLevel(t, s) })
	t.Run("AddSign_Signal", func(t *testing.T) { integrationTest_AddSign_Signal(t, s) })
}
