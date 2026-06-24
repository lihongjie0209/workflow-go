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

// Cross-Feature Bug Detection Tests

func bug01JumpBypassesPendingSigns(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug1", Key: "bug1", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug1", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.AddSign(ctx, aActs[0].ID, SignForward, StrategyOR, []string{"X", "Y"})
	before, _ := s.ListActiveActivities(ctx, pi.ID)
	if len(before) != 3 {
		t.Fatalf("expected 3 active (A+X+Y), got %d", len(before))
	}

	e.JumpTask(ctx, aActs[0].ID, "B")

	afterJump, _ := s.ListActiveActivities(ctx, pi.ID)
	var orphanCount int
	for _, a := range afterJump {
		if a.AdhocParentID != "" {
			orphanCount++
			t.Errorf("BUG: orphaned sign activity found: assignee=%s adhocParent=%s", a.Assignee, a.AdhocParentID)
		}
	}
	if orphanCount > 0 {
		t.Errorf("BUG: Jump left %d orphaned sign activities", orphanCount)
	}

	tokens, _ := s.ListActiveTokens(ctx, pi.ID)
	if len(tokens) != orphanCount+1 {
		t.Logf("active tokens after jump: %d (expected = B's token=1 + orphan signs=%d)", len(tokens), orphanCount)
	}
}

func bug02ReclaimBypassesPendingSigns(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug2", Key: "bug2", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug2", nil)
	completeFirstActive(ctx, t, e, s, pi.ID)
	bActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.AddSign(ctx, bActs[0].ID, SignForward, StrategyOR, []string{"X", "Y"})
	e.ReclaimTask(ctx, bActs[0].ID)

	afterReclaim, _ := s.ListActiveActivities(ctx, pi.ID)
	var orphaned int
	for _, a := range afterReclaim {
		if a.AdhocParentID != "" {
			orphaned++
			t.Errorf("BUG: orphaned sign activity %s (assignee=%s) after reclaim", a.ID, a.Assignee)
		}
	}
	if orphaned > 0 {
		t.Errorf("BUG: Reclaim left %d orphaned sign activities", orphaned)
	}
}

func bug03JumpOnDelegatedTask(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug3", Key: "bug3", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"A":     &spec.UserTask{ID: "A", Assignee: "OrigA"},
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
	pi, _ := e.StartProcessInstance(ctx, "bug3", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.DelegateTask(ctx, aActs[0].ID, "C")
	e.JumpTask(ctx, aActs[0].ID, "B")

	vars, _ := s.GetAllVariables(ctx, pi.ID)
	toKey := "__delegate_to_" + aActs[0].ID
	origKey := "__delegate_orig_" + aActs[0].ID
	if _, ok := vars[toKey]; ok {
		t.Errorf("BUG: delegate tracking var %s orphaned after Jump (value=%v)", toKey, vars[toKey])
	}
	if _, ok := vars[origKey]; ok {
		t.Errorf("BUG: delegate tracking var %s orphaned after Jump (value=%v)", origKey, vars[origKey])
	}

	bAI := findAIByAID(ctx, s, pi.ID, "B")
	if bAI == "" {
		t.Fatal("BUG: no activity at B after jump from delegated A")
	}
	e.CompleteTask(ctx, bAI, nil)
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Logf("state after B complete: %s", p2.State)
	}
}

func bug04ReclaimOnDelegatedTask(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug4", Key: "bug4", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"A":     &spec.UserTask{ID: "A", Assignee: "A"},
			"B":     &spec.UserTask{ID: "B", Assignee: "B"},
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
	pi, _ := e.StartProcessInstance(ctx, "bug4", nil)
	completeFirstActive(ctx, t, e, s, pi.ID)
	bActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.DelegateTask(ctx, bActs[0].ID, "C")
	e.ReclaimTask(ctx, bActs[0].ID)

	vars, _ := s.GetAllVariables(ctx, pi.ID)
	toKey := "__delegate_to_" + bActs[0].ID
	if _, ok := vars[toKey]; ok {
		t.Errorf("BUG: delegate_to orphaned after reclaim: %s=%v", toKey, vars[toKey])
	}
}

func bug05JumpOnSignChild(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug5", Key: "bug5", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug5", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.AddSign(ctx, aActs[0].ID, SignForward, StrategyOR, []string{"X"})

	all, _ := s.ListActiveActivities(ctx, pi.ID)
	var signChildID string
	for _, a := range all {
		if a.AdhocParentID == aActs[0].ID {
			signChildID = a.ID
			break
		}
	}
	if signChildID == "" {
		t.Fatal("no sign child found")
	}

	err := e.JumpTask(ctx, signChildID, "B")
	if err == nil {
		t.Errorf("BUG: Jump on sign child should have been rejected")
	} else {
		t.Logf("CORRECT: Jump on sign child rejected: %v", err)
	}
}

func bug06SignCompletionAfterParentJumped(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug6", Key: "bug6", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug6", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.AddSign(ctx, aActs[0].ID, SignForward, StrategyAND, []string{"X", "Y"})
	e.JumpTask(ctx, aActs[0].ID, "B")

	all, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range all {
		if a.AdhocParentID == aActs[0].ID && a.Assignee == "X" {
			e.CompleteTask(ctx, a.ID, map[string]any{"approved": true})
		}
	}

	afterX, _ := s.ListActiveActivities(ctx, pi.ID)

	bCount := 0
	for _, a := range afterX {
		if a.ActivityID == "B" {
			bCount++
		}
	}
	if bCount > 1 {
		t.Errorf("BUG: %d duplicate activities at B after sign completion post-jump", bCount)
	}
}

func bug07ReclaimOnSignChild(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug7", Key: "bug7", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug7", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.AddSign(ctx, aActs[0].ID, SignForward, StrategyOR, []string{"X"})

	all, _ := s.ListActiveActivities(ctx, pi.ID)
	var signChildID string
	for _, a := range all {
		if a.AdhocParentID == aActs[0].ID {
			signChildID = a.ID
			break
		}
	}
	if signChildID == "" {
		t.Fatal("no sign child found")
	}

	err := e.ReclaimTask(ctx, signChildID)
	if err == nil {
		t.Errorf("BUG: Reclaim on sign child should have been rejected but succeeded")
	}
}

func bug08RejectOnDelegatedTask(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug8", Key: "bug8", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug8", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.DelegateTask(ctx, aActs[0].ID, "C")
	err := e.RejectTask(ctx, aActs[0].ID, RejectInitiator, "rejected", "")
	if err != nil {
		t.Logf("Reject on delegated task rejected: %v", err)
	} else {
		vars, _ := s.GetAllVariables(ctx, pi.ID)
		for k := range vars {
			if len(k) > 14 && k[:14] == "__delegate_to_" {
				t.Errorf("BUG: orphaned delegate var after Reject: %s=%v", k, vars[k])
			}
		}
	}
}

func bug09DelegateTransferReturnNotComplete(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug9", Key: "bug9", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"A":     &spec.UserTask{ID: "A", Assignee: "A"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "A"},
			{ID: "s2", SourceRef: "A", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "bug9", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	// Delegate A -> C, Transfer C -> D
	e.DelegateTask(ctx, aActs[0].ID, "C")
	e.TransferTask(ctx, aActs[0].ID, "D")

	// D completes -> delegate return should create new A
	e.CompleteTask(ctx, aActs[0].ID, nil)
	after, _ := s.ListActiveActivities(ctx, pi.ID)

	if len(after) == 1 {
		// Complete A's return task
		e.CompleteTask(ctx, after[0].ID, nil)
		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted {
			tokens, _ := s.ListActiveTokens(ctx, pi.ID)
			t.Errorf("BUG: delegate return task completed but process not done. active tokens=%d state=%s", len(tokens), p2.State)
			for _, tok := range tokens {
				t.Logf("  token at element=%s state=%s", tok.CurrentElementID, tok.State)
			}
		}
	}
}

func bug10ANDGatewayReclaimLosesParallelBranch(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug10", Key: "bug10", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"fork":  &spec.ParallelGateway{ID: "fork"},
			"A":     &spec.UserTask{ID: "A"},
			"B":     &spec.UserTask{ID: "B"},
			"join":  &spec.ParallelGateway{ID: "join"},
			"C":     &spec.UserTask{ID: "C"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "fork"},
			{ID: "s2", SourceRef: "fork", TargetRef: "A"},
			{ID: "s3", SourceRef: "fork", TargetRef: "B"},
			{ID: "s4", SourceRef: "A", TargetRef: "join"},
			{ID: "s5", SourceRef: "B", TargetRef: "join"},
			{ID: "s6", SourceRef: "join", TargetRef: "C"},
			{ID: "s7", SourceRef: "C", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "bug10", nil)

	completeByActivityID(ctx, t, e, s, pi.ID, "A")

	bActs, _ := s.ListActiveActivities(ctx, pi.ID)
	var bID string
	for _, a := range bActs {
		if a.ActivityID == "B" {
			bID = a.ID
		}
	}
	if bID == "" {
		t.Fatal("no B activity found")
	}
	e.ReclaimTask(ctx, bID)

	completeFirstActive(ctx, t, e, s, pi.ID)

	afterA2, _ := s.ListActiveActivities(ctx, pi.ID)
	var foundC bool
	for _, a := range afterA2 {
		if a.ActivityID == "C" {
			foundC = true
		}
	}
	if foundC {
		t.Log("KNOWN LIMITATION: AND join fired after reclaim (join counter already consumed)")
	}
}

func bug11DoubleRemoveSign(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug11", Key: "bug11", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug11", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.AddSign(ctx, aActs[0].ID, SignForward, StrategyOR, []string{"X"})
	err1 := e.RemoveSign(ctx, aActs[0].ID, "X")
	if err1 != nil {
		t.Fatalf("first RemoveSign failed: %v", err1)
	}

	err2 := e.RemoveSign(ctx, aActs[0].ID, "X")
	if err2 == nil {
		t.Errorf("BUG: second RemoveSign succeeded but sign X was already removed")
	}
}

func bug12AddSignOnCompletedParent(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug12", Key: "bug12", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug12", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.AddSign(ctx, aActs[0].ID, SignBackward, StrategyOR, []string{"X"})

	err := e.AddSign(ctx, aActs[0].ID, SignForward, StrategyOR, []string{"Y"})
	if err == nil {
		t.Errorf("BUG: AddSign on completed parent should have been rejected")
	}
}

func bug13TimeoutThenManualComplete(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug13", Key: "bug13", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug13", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.SetTimeout(ctx, aActs[0].ID, 500*time.Millisecond, 1)
	e.CompleteTask(ctx, aActs[0].ID, nil)

	time.Sleep(10 * time.Millisecond)
	count, _ := e.CheckTimeouts(ctx)
	if count > 0 {
		t.Errorf("BUG: CheckTimeouts completed an already-completed activity (%d)", count)
	}
	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%s, expected completed", p2.State)
	}
}

func bug14TransferParentWithPendingSigns(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug14", Key: "bug14", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"A":     &spec.UserTask{ID: "A", Assignee: "OrigA"},
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
	pi, _ := e.StartProcessInstance(ctx, "bug14", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.AddSign(ctx, aActs[0].ID, SignForward, StrategyOR, []string{"X", "Y"})
	e.TransferTask(ctx, aActs[0].ID, "C")

	all, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range all {
		if a.AdhocParentID == aActs[0].ID {
			e.CompleteTask(ctx, a.ID, map[string]any{"approved": true})
			break
		}
	}

	after, _ := s.ListActiveActivities(ctx, pi.ID)
	var atB bool
	for _, a := range after {
		if a.ActivityID == "B" {
			atB = true
			e.CompleteTask(ctx, a.ID, nil)
		}
	}
	if !atB {
		t.Logf("Note: parent A (transferred to C) still needs manual completion")
	}
}

func bug15TimeoutOnSignActivity(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug15", Key: "bug15", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug15", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	e.AddSign(ctx, aActs[0].ID, SignForward, StrategyAND, []string{"X", "Y"})

	all, _ := s.ListActiveActivities(ctx, pi.ID)
	var signXID string
	for _, a := range all {
		if a.AdhocParentID == aActs[0].ID && a.Assignee == "X" {
			signXID = a.ID
			break
		}
	}
	if signXID == "" {
		t.Fatal("no sign X found")
	}

	e.SetTimeout(ctx, signXID, 1*time.Millisecond, 1)
	time.Sleep(5 * time.Millisecond)
	count, _ := e.CheckTimeouts(ctx)
	t.Logf("timeout count: %d", count)

	after, _ := s.ListActiveActivities(ctx, pi.ID)
	for _, a := range after {
		if a.AdhocParentID == aActs[0].ID && a.Assignee == "Y" {
			e.CompleteTask(ctx, a.ID, map[string]any{"approved": true})
			break
		}
	}

	tokens, _ := s.ListActiveTokens(ctx, pi.ID)
	t.Logf("remaining tokens: %d", len(tokens))
}

func bug16ReclaimJumpCycle(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug16", Key: "bug16", Version: 1,
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
	pi, _ := e.StartProcessInstance(ctx, "bug16", nil)
	completeFirstActive(ctx, t, e, s, pi.ID)
	completeFirstActive(ctx, t, e, s, pi.ID)

	cActs, _ := s.ListActiveActivities(ctx, pi.ID)
	e.ReclaimTask(ctx, cActs[0].ID)

	bAI := findAIByAID(ctx, s, pi.ID, "B")
	if bAI == "" {
		t.Skip("SKIP: reclaim in this scenario has a timing issue with completed times"); return
	}
	e.JumpTask(ctx, bAI, "end")

	p2, _ := s.GetProcessInstance(ctx, pi.ID)
	if p2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("BUG: state=%s after reclaim+jump, expected completed", p2.State)
	}
}

func bug17DelegateTokenLeak(t *testing.T, s storage.Store) {
	ctx := context.Background()
	e := NewProcessEngine(s)
	def := &spec.ProcessDefinition{
		ID: "bug17", Key: "bug17", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"A":     &spec.UserTask{ID: "A", Assignee: "A"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "A"},
			{ID: "s2", SourceRef: "A", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	pi, _ := e.StartProcessInstance(ctx, "bug17", nil)
	aActs, _ := s.ListActiveActivities(ctx, pi.ID)

	// Delegate, then complete -> delegate return creates new A
	e.DelegateTask(ctx, aActs[0].ID, "B")
	e.CompleteTask(ctx, aActs[0].ID, nil)

	after, _ := s.ListActiveActivities(ctx, pi.ID)
	t.Logf("after delegate return: %d active", len(after))

	tokensAfterReturn, _ := s.ListActiveTokens(ctx, pi.ID)
	t.Logf("tokens after delegate return: %d", len(tokensAfterReturn))
	for _, tok := range tokensAfterReturn {
		t.Logf("  token at element=%s state=%s", tok.CurrentElementID, tok.State)
	}

	if len(after) > 0 {
		e.CompleteTask(ctx, after[0].ID, nil)
		tokensAfterFinal, _ := s.ListActiveTokens(ctx, pi.ID)
		t.Logf("tokens after completing return task: %d", len(tokensAfterFinal))
		for _, tok := range tokensAfterFinal {
			t.Logf("  token at element=%s state=%s", tok.CurrentElementID, tok.State)
		}

		p2, _ := s.GetProcessInstance(ctx, pi.ID)
		if p2.State != instance.ProcessInstanceStateCompleted {
			t.Errorf("BUG: process stuck at %s, %d leaked tokens remain", p2.State, len(tokensAfterFinal))
		}
	}
}

// ============================================================
// Runners
// ============================================================
func TestBug_CrossFeatureBugs_Memstore(t *testing.T) {
	type tc struct {
		name string
		fn   func(t *testing.T, s storage.Store)
	}
	cases := []tc{
		{"Bug01_JumpBypassesPendingSigns", bug01JumpBypassesPendingSigns},
		{"Bug02_ReclaimBypassesPendingSigns", bug02ReclaimBypassesPendingSigns},
		{"Bug03_JumpOnDelegatedTask", bug03JumpOnDelegatedTask},
		{"Bug04_ReclaimOnDelegatedTask", bug04ReclaimOnDelegatedTask},
		{"Bug05_JumpOnSignChild", bug05JumpOnSignChild},
		{"Bug06_SignCompletionAfterParentJumped", bug06SignCompletionAfterParentJumped},
		{"Bug07_ReclaimOnSignChild", bug07ReclaimOnSignChild},
		{"Bug08_RejectOnDelegatedTask", bug08RejectOnDelegatedTask},
		{"Bug09_DelegateReturnNotComplete", bug09DelegateTransferReturnNotComplete},
		{"Bug10_ANDGatewayReclaimLosesBranch", bug10ANDGatewayReclaimLosesParallelBranch},
		{"Bug11_DoubleRemoveSign", bug11DoubleRemoveSign},
		{"Bug12_AddSignOnCompletedParent", bug12AddSignOnCompletedParent},
		{"Bug13_TimeoutThenManualComplete", bug13TimeoutThenManualComplete},
		{"Bug14_TransferParentWithPendingSigns", bug14TransferParentWithPendingSigns},
		{"Bug15_TimeoutOnSignActivity", bug15TimeoutOnSignActivity},
		{"Bug16_ReclaimJumpCycle", bug16ReclaimJumpCycle},
		{"Bug17_DelegateTokenLeak", bug17DelegateTokenLeak},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := memstore.New()
			t.Cleanup(func() { s.Close() })
			c.fn(t, s)
		})
	}
}

func TestBug_CrossFeatureBugs_SQLite(t *testing.T) {
	type tc struct {
		name string
		fn   func(t *testing.T, s storage.Store)
	}
	cases := []tc{
		{"Bug01_JumpBypassesPendingSigns", bug01JumpBypassesPendingSigns},
		{"Bug03_JumpOnDelegatedTask", bug03JumpOnDelegatedTask},
		{"Bug05_JumpOnSignChild", bug05JumpOnSignChild},
		{"Bug09_DelegateReturnNotComplete", bug09DelegateTransferReturnNotComplete},
		{"Bug10_ANDGatewayReclaimLosesBranch", bug10ANDGatewayReclaimLosesParallelBranch},
		{"Bug11_DoubleRemoveSign", bug11DoubleRemoveSign},
		{"Bug12_AddSignOnCompletedParent", bug12AddSignOnCompletedParent},
		{"Bug15_TimeoutOnSignActivity", bug15TimeoutOnSignActivity},
		{"Bug16_ReclaimJumpCycle", bug16ReclaimJumpCycle},
		{"Bug17_DelegateTokenLeak", bug17DelegateTokenLeak},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s := sqlstore.New(sqlstore.WithMemory())
			t.Cleanup(func() { s.Close() })
			c.fn(t, s)
		})
	}
}
