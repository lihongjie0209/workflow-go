package engine

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
	"github.com/lihongjie/workflow-go/storage/memstore"
)

// Test 1: 子流程中驳回 - successful pass
func TestReject_InsideSubProcess(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	sub := &spec.ProcessDefinition{
		ID: "sub:v1", Key: "sub", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "A": &spec.UserTask{ID: "A"}, "B": &spec.UserTask{ID: "B"},
			"C": &spec.UserTask{ID: "C"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "A"}, {ID: "s2", SourceRef: "A", TargetRef: "B"},
			{ID: "s3", SourceRef: "B", TargetRef: "C"}, {ID: "s4", SourceRef: "C", TargetRef: "end"},
		},
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
	pi, _ := e.StartProcessInstance(ctx, "p:v1", nil)
	children, _ := s.ListProcessInstances(ctx, "sub:v1")
	child := children[0]
	completeFirstActive(ctx, t, e, s, child.ID) // A
	completeFirstActive(ctx, t, e, s, child.ID) // B
	// C rejects to B
	cai := findActiveAI(ctx, s, child.ID, "C")
	e.RejectTask(ctx, cai, RejectPrevious, "", "")
	back := getActiveIDs(ctx, s, child.ID)
	if len(back) != 1 || back[0] != "B" { t.Fatalf("expected B, got %v", back) }
	// B -> C -> end
	completeFirstActive(ctx, t, e, s, child.ID)
	completeFirstActive(ctx, t, e, s, child.ID)
	c2, _ := s.GetProcessInstance(ctx, child.ID)
	if c2.State != instance.ProcessInstanceStateCompleted { t.Fatalf("child=%q", c2.State) }
	// Parent continues
	parentActs := getActiveIDs(ctx, s, pi.ID)
	if len(parentActs) != 1 || parentActs[0] != "done" { t.Fatalf("expect done, got %v", parentActs) }
	completeFirstActive(ctx, t, e, s, pi.ID)
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted { t.Errorf("parent=%q", p2.State) }
}

// Test 2: 网关后驳回
func TestReject_AfterGateway(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	high := "${amount > 500}"
	def := &spec.ProcessDefinition{
		ID: "xg", Key: "xg", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "xor": &spec.ExclusiveGateway{ID: "xor", DefaultFlowID: "sf_low"},
			"A": &spec.UserTask{ID: "A"}, "B": &spec.UserTask{ID: "B"}, "C": &spec.UserTask{ID: "C"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "xor"},
			{ID: "sf_high", SourceRef: "xor", TargetRef: "A", ConditionExpression: &high},
			{ID: "sf_low", SourceRef: "xor", TargetRef: "B"},
			{ID: "s2", SourceRef: "A", TargetRef: "B"}, {ID: "s3", SourceRef: "B", TargetRef: "C"}, {ID: "s4", SourceRef: "C", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "xg", map[string]any{"amount": 1000})
	completeFirstActive(ctx, t, e, s, pi.ID)
	completeFirstActive(ctx, t, e, s, pi.ID)
	cai := findActiveAI(ctx, s, pi.ID, "C")
	e.RejectTask(ctx, cai, RejectPrevious, "", "")
	back := getActiveIDs(ctx, s, pi.ID)
	if len(back) != 1 || back[0] != "B" { t.Fatalf("expected B, got %v", back) }
	completeFirstActive(ctx, t, e, s, pi.ID)
	completeFirstActive(ctx, t, e, s, pi.ID)
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted { t.Errorf("state=%q", p2.State) }
}

// Test 3: 加签后驳回
func TestReject_AfterAddSign(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "ras", Key: "ras", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "A": &spec.UserTask{ID: "A"}, "B": &spec.UserTask{ID: "B"}, "C": &spec.UserTask{ID: "C"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "A"}, {ID: "s2", SourceRef: "A", TargetRef: "B"},
			{ID: "s3", SourceRef: "B", TargetRef: "C"}, {ID: "s4", SourceRef: "C", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "ras", nil)
	completeFirstActive(ctx, t, e, s, pi.ID)
	completeFirstActive(ctx, t, e, s, pi.ID)
	cActs, _ := s.ListActiveActivities(ctx, pi.ID)
	e.AddSign(ctx, cActs[0].ID, SignForward, StrategyOR, []string{"D"})
	allActs := getActiveIDs(ctx, s, pi.ID)
	if len(allActs) != 2 { t.Fatalf("expected 2, got %v", allActs) }
	for _, a := range allActs { if a == "D" { completeByActivityID(ctx, t, e, s, pi.ID, a) } }
	cai := findActiveAI(ctx, s, pi.ID, "C")
	e.RejectTask(ctx, cai, RejectPrevious, "", "")
	back := getActiveIDs(ctx, s, pi.ID)
	foundB := false
	for _, id := range back { if id == "B" { foundB = true } }
	if !foundB { t.Fatalf("expected B in %v", back) }
	completeFirstActive(ctx, t, e, s, pi.ID)
	left := getActiveIDs(ctx, s, pi.ID)
	if len(left) > 0 { t.Logf("after B complete: %v", left) }
}

// Test 4: 第二次驳回
func TestReject_Twice(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "2r", Key: "2r", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "A": &spec.UserTask{ID: "A"}, "B": &spec.UserTask{ID: "B"}, "C": &spec.UserTask{ID: "C"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "A"}, {ID: "s2", SourceRef: "A", TargetRef: "B"},
			{ID: "s3", SourceRef: "B", TargetRef: "C"}, {ID: "s4", SourceRef: "C", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "2r", nil)
	completeFirstActive(ctx, t, e, s, pi.ID) // A
	// Reject from B to A
	bai := findActiveAI(ctx, s, pi.ID, "B")
	e.RejectTask(ctx, bai, RejectSpecific, "", "A")
	completeFirstActive(ctx, t, e, s, pi.ID) // A -> B
	// Reject from B to A again
	bai = findActiveAI(ctx, s, pi.ID, "B")
	e.RejectTask(ctx, bai, RejectSpecific, "", "A")
	completeFirstActive(ctx, t, e, s, pi.ID)
	completeFirstActive(ctx, t, e, s, pi.ID)
	completeFirstActive(ctx, t, e, s, pi.ID)
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted { t.Errorf("state=%q", p2.State) }
}

// Test 5: 终止子流程
func TestReject_TerminateSub(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	sub := &spec.ProcessDefinition{
		ID: "st:v1", Key: "st", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "T": &spec.UserTask{ID: "T"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "T"}, {ID: "s2", SourceRef: "T", TargetRef: "end"}},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, sub)
	parent := &spec.ProcessDefinition{
		ID: "pt:v1", Key: "pt", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"call": &spec.CallActivity{ID: "call", CalledElement: "st", InheritVariables: true},
			"done": &spec.UserTask{ID: "done"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "call"}, {ID: "s2", SourceRef: "call", TargetRef: "done"}, {ID: "s3", SourceRef: "done", TargetRef: "end"}},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, parent)
	pi, _ := e.StartProcessInstance(ctx, "pt:v1", nil)
	children, _ := s.ListProcessInstances(ctx, "st:v1")
	child := children[0]
	cai := findActiveAI(ctx, s, child.ID, "T")
	e.RejectTask(ctx, cai, RejectTerminate, "", "")
	c2, _ := s.GetProcessInstance(ctx, child.ID)
	if c2.State != instance.ProcessInstanceStateRejected { t.Errorf("child=%q", c2.State) }
	_ = pi
}

// Test 6: 并行网关后驳回
func TestReject_AfterParallel(t *testing.T) {
	ctx := context.Background()
	s := memstore.New(); t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "pr", Key: "pr", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "fork": &spec.ParallelGateway{ID: "fork"},
			"TA": &spec.UserTask{ID: "TA"}, "TB": &spec.UserTask{ID: "TB"},
			"join": &spec.ParallelGateway{ID: "join"}, "TC": &spec.UserTask{ID: "TC"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "fork"}, {ID: "s_a", SourceRef: "fork", TargetRef: "TA"},
			{ID: "s_b", SourceRef: "fork", TargetRef: "TB"}, {ID: "s_aj", SourceRef: "TA", TargetRef: "join"},
			{ID: "s_bj", SourceRef: "TB", TargetRef: "join"}, {ID: "s_jc", SourceRef: "join", TargetRef: "TC"},
			{ID: "s_ce", SourceRef: "TC", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "pr", nil)
	acts := getActiveIDs(ctx, s, pi.ID)
	if len(acts) != 2 { t.Fatalf("expected 2, got %v", acts) }
	for _, id := range acts { completeByActivityID(ctx, t, e, s, pi.ID, id) }
	tcai := findActiveAI(ctx, s, pi.ID, "TC")
	e.RejectTask(ctx, tcai, RejectPrevious, "", "")
	back := getActiveIDs(ctx, s, pi.ID)
	t.Logf("after reject from TC: %v", back)
}

func findActiveAI(ctx context.Context, s storage.Store, piID, activityID string) string {
	acts, _ := s.ListActiveActivities(ctx, piID)
	for _, a := range acts { if a.ActivityID == activityID { return a.ID } }
	return ""
}
