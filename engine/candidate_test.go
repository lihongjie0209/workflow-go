package engine

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/identity"
	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
	"github.com/lihongjie/workflow-go/storage/memstore"
	"github.com/lihongjie/workflow-go/storage/sqlstore"
)

func TestCandidate_AssigneeDirect(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID: "cand1", Key: "cand1", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批", Assignee: "${approver}"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, err := e.StartProcessInstance(ctx, "cand1", map[string]any{"approver": "张三"})
	if err != nil {
		t.Fatal(err)
	}

	acts, _ := s.ListActiveActivities(ctx, pi.ID)
	if len(acts) != 1 {
		t.Fatalf("expected 1 active activity, got %d", len(acts))
	}
	if acts[0].State != instance.ActivityStateActive {
		t.Errorf("expected state=active for direct assignee, got %s", acts[0].State)
	}
	if acts[0].Assignee != "张三" {
		t.Errorf("expected assignee=张三, got %s", acts[0].Assignee)
	}
	// Should have no candidates variable
	_, err = s.GetVariable(ctx, pi.ID, "__candidates_"+acts[0].ID)
	if err == nil {
		t.Error("expected error getting candidates variable for direct assignee task")
	}
}

func TestCandidate_ClaimAndComplete(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })

	idSvc := identity.NewMemoryStore()
	idSvc.AddUser(&identity.User{ID: "userA", Name: "Alice"})
	idSvc.AddUser(&identity.User{ID: "userB", Name: "Bob"})

	e := NewProcessEngine(s, WithIdentityService(idSvc))

	def := &spec.ProcessDefinition{
		ID: "cand2", Key: "cand2", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批", CandidateUsers: []string{"userA", "userB"}},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, err := e.StartProcessInstance(ctx, "cand2", nil)
	if err != nil {
		t.Fatal(err)
	}

	// Verify activity is unclaimed
	allActs, _ := s.ListActivitiesByProcessInstance(ctx, pi.ID)
	var ai *instance.ActivityInstance
	for _, a := range allActs {
		if a.ActivityID == "task" {
			ai = a
			break
		}
	}
	if ai == nil {
		t.Fatal("no activity found for task")
	}
	if ai.State != instance.ActivityStateUnclaimed {
		t.Errorf("expected state=unclaimed, got %s", ai.State)
	}
	if ai.Assignee != "" {
		t.Errorf("expected empty assignee for unclaimed task, got %s", ai.Assignee)
	}

	// GetCandidates should return the candidates
	cands := e.GetCandidates(ctx, ai.ID)
	if len(cands) != 2 {
		t.Fatalf("expected 2 candidates, got %d: %v", len(cands), cands)
	}

	// ClaimTask by non-candidate should fail
	err = e.ClaimTask(ctx, ai.ID, "userC")
	if err == nil {
		t.Fatal("expected error claiming by non-candidate")
	}

	// ClaimTask by candidate should succeed
	err = e.ClaimTask(ctx, ai.ID, "userA")
	if err != nil {
		t.Fatalf("ClaimTask: %v", err)
	}

	// Verify claimed state
	claimed, _ := s.GetActivityInstance(ctx, ai.ID)
	if claimed.State != instance.ActivityStateActive {
		t.Errorf("expected state=active after claim, got %s", claimed.State)
	}
	if claimed.Assignee != "userA" {
		t.Errorf("expected assignee=userA, got %s", claimed.Assignee)
	}
	if claimed.ClaimTime == nil {
		t.Error("expected ClaimTime to be set after claim")
	}

	// Trying to claim again should fail
	err = e.ClaimTask(ctx, ai.ID, "userB")
	if err == nil {
		t.Fatal("expected error claiming already claimed task")
	}

	// CompleteTask should now succeed
	err = e.CompleteTask(ctx, ai.ID, nil)
	if err != nil {
		t.Fatalf("CompleteTask after claim: %v", err)
	}

	pi2, _ := s.GetProcessInstance(ctx, pi.ID)
	if pi2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("expected completed, got %s", pi2.State)
	}
}

func TestCandidate_Unclaim(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })

	idSvc := identity.NewMemoryStore()
	idSvc.AddUser(&identity.User{ID: "userA", Name: "Alice"})

	e := NewProcessEngine(s, WithIdentityService(idSvc))

	def := &spec.ProcessDefinition{
		ID: "cand3", Key: "cand3", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批", CandidateUsers: []string{"userA"}},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, err := e.StartProcessInstance(ctx, "cand3", nil)
	if err != nil {
		t.Fatal(err)
	}

	acts, _ := s.ListActivitiesByProcessInstance(ctx, pi.ID)
	ai := findActivityByID(acts, "task")
	if ai == nil {
		t.Fatal("no activity found for task")
	}

	// Claim it
	e.ClaimTask(ctx, ai.ID, "userA")

	// Unclaim it
	err = e.UnclaimTask(ctx, ai.ID)
	if err != nil {
		t.Fatalf("UnclaimTask: %v", err)
	}

	// Verify back to unclaimed
	unclaimed, _ := s.GetActivityInstance(ctx, ai.ID)
	if unclaimed.State != instance.ActivityStateUnclaimed {
		t.Errorf("expected state=unclaimed after unclaim, got %s", unclaimed.State)
	}
	if unclaimed.Assignee != "" {
		t.Errorf("expected empty assignee after unclaim, got %s", unclaimed.Assignee)
	}
	if unclaimed.ClaimTime != nil {
		t.Error("expected ClaimTime to be nil after unclaim")
	}

	// Can't complete while unclaimed
	err = e.CompleteTask(ctx, ai.ID, nil)
	if err == nil {
		t.Fatal("expected error completing unclaimed task")
	}

	// Re-claim and complete should work
	e.ClaimTask(ctx, ai.ID, "userA")
	err = e.CompleteTask(ctx, ai.ID, nil)
	if err != nil {
		t.Fatalf("CompleteTask after re-claim: %v", err)
	}
}

func TestCandidate_GroupResolution(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })

	idSvc := identity.NewMemoryStore()
	idSvc.AddUser(&identity.User{ID: "u1", Name: "Alice"})
	idSvc.AddUser(&identity.User{ID: "u2", Name: "Bob"})
	idSvc.AddUser(&identity.User{ID: "u3", Name: "Charlie"})
	idSvc.AddGroup(&identity.Group{ID: "g1", Name: "HR", Members: []string{"u1", "u2"}})

	e := NewProcessEngine(s, WithIdentityService(idSvc))

	def := &spec.ProcessDefinition{
		ID: "cand4", Key: "cand4", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批", CandidateGroup: "g1"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, err := e.StartProcessInstance(ctx, "cand4", nil)
	if err != nil {
		t.Fatal(err)
	}

	acts2, _ := s.ListActivitiesByProcessInstance(ctx, pi.ID)
	ai2 := findActivityByID(acts2, "task")
	if ai2 == nil {
		t.Fatal("no activity found for task")
	}

	// Should have 2 candidates (u1, u2 from group g1)
	cands := e.GetCandidates(ctx, ai2.ID)
	if len(cands) != 2 {
		t.Fatalf("expected 2 candidates from group, got %d: %v", len(cands), cands)
	}

	// Both group members can claim
	err = e.ClaimTask(ctx, ai2.ID, "u1")
	if err != nil {
		t.Fatalf("ClaimTask by u1: %v", err)
	}

	// unclaim
	e.UnclaimTask(ctx, ai2.ID)

	// u3 is NOT in group g1
	err = e.ClaimTask(ctx, ai2.ID, "u3")
	if err == nil {
		t.Fatal("expected error claiming by non-member")
	}
}

func TestCandidate_NoAssigneeNoCandidates(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })

	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID: "cand5", Key: "cand5", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, err := e.StartProcessInstance(ctx, "cand5", nil)
	if err != nil {
		t.Fatal(err)
	}

	acts3, _ := s.ListActivitiesByProcessInstance(ctx, pi.ID)
	ai3 := findActivityByID(acts3, "task")
	if ai3 == nil {
		t.Fatal("no activity found for task")
	}
	// No assignee, no candidates → active with empty assignee
	if ai3.State != instance.ActivityStateActive {
		t.Errorf("expected state=active for no-assignee-no-candidates task, got %s", ai3.State)
	}
}

// TestCandidate_WithIdentityNil verifies the engine works without identity service.
func TestCandidate_WithoutIdentityService(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })

	e := NewProcessEngine(s) // no identity service

	def := &spec.ProcessDefinition{
		ID: "cand6", Key: "cand6", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批", CandidateUsers: []string{"u1"}},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	_, err := e.StartProcessInstance(ctx, "cand6", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Without identity service, candidates can't be resolved, so the task
	// falls through to active with empty assignee (same as no candidates case)
}

func TestCandidate_UnclaimOnNonCandidateTask(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })

	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID: "cand7", Key: "cand7", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批", Assignee: "张三"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, err := e.StartProcessInstance(ctx, "cand7", nil)
	if err != nil {
		t.Fatal(err)
	}

	acts4, _ := s.ListActivitiesByProcessInstance(ctx, pi.ID)
	ai4 := findActivityByID(acts4, "task")
	if ai4 == nil {
		t.Fatal("no activity found for task")
	}
	// Direct-assignee task can't be unclaimed (no candidates)
	err = e.UnclaimTask(ctx, ai4.ID)
	if err == nil {
		t.Fatal("expected error unclaiming a non-candidate task")
	}
}

// TestCandidate_WithSqlStore runs the candidate scenario against sqlstore.
func TestCandidate_WithSqlStore(t *testing.T) {
	ctx := context.Background()
	s := sqlstore.New(sqlstore.WithMemory())
	t.Cleanup(func() { s.Close() })

	idSvc := identity.NewMemoryStore()
	idSvc.AddUser(&identity.User{ID: "userA", Name: "Alice"})
	idSvc.AddUser(&identity.User{ID: "userB", Name: "Bob"})

	e := NewProcessEngine(s, WithIdentityService(idSvc))

	def := &spec.ProcessDefinition{
		ID: "cand_sql", Key: "cand_sql", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批", CandidateUsers: []string{"userA", "userB"}},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, err := e.StartProcessInstance(ctx, "cand_sql", nil)
	if err != nil {
		t.Fatal(err)
	}

	acts5, _ := s.ListActivitiesByProcessInstance(ctx, pi.ID)
	ai5 := findActivityByID(acts5, "task")
	if ai5 == nil {
		t.Fatal("no activity found for task")
	}
	if ai5.State != instance.ActivityStateUnclaimed {
		t.Errorf("expected unclaimed state, got %s", ai5.State)
	}

	// Claim and complete
	e.ClaimTask(ctx, ai5.ID, "userA")
	e.CompleteTask(ctx, ai5.ID, nil)

	pi2, _ := s.GetProcessInstance(ctx, pi.ID)
	if pi2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("expected completed, got %s", pi2.State)
	}
}

func findActivityByID(acts []*instance.ActivityInstance, activityID string) *instance.ActivityInstance {
	for _, a := range acts {
		if a.ActivityID == activityID {
			return a
		}
	}
	return nil
}

func TestBusinessKey_StartAndQuery(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID: "bk1", Key: "leave", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批", Assignee: "张三"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	// Start with business key
	pi, err := e.StartProcessInstanceWithBusinessKey(ctx, "bk1", "ORDER-001", "", map[string]any{"amount": 100})
	if err != nil {
		t.Fatalf("StartProcessInstanceWithBusinessKey: %v", err)
	}
	if pi.BusinessKey != "ORDER-001" {
		t.Errorf("BusinessKey=%q, want ORDER-001", pi.BusinessKey)
	}

	// Query by business key
	results, total, err := s.QueryProcessInstances(ctx, storage.InstQuery{BusinessKey: "ORDER-001"})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(results) != 1 {
		t.Errorf("expected 1 result, got total=%d len=%d", total, len(results))
	}
	if results[0].ID != pi.ID {
		t.Errorf("ID mismatch: got %q, want %q", results[0].ID, pi.ID)
	}

	// StartProcessInstanceByKey
	pi2, err := e.StartProcessInstanceByKey(ctx, "leave", "ORDER-002", "", nil)
	if err != nil {
		t.Fatalf("StartProcessInstanceByKey: %v", err)
	}
	if pi2.BusinessKey != "ORDER-002" {
		t.Errorf("BusinessKey=%q, want ORDER-002", pi2.BusinessKey)
	}
	if pi2.ProcessDefinitionID != "bk1" {
		t.Errorf("expected defID=bk1, got %q", pi2.ProcessDefinitionID)
	}

	// Backward compatible: StartProcessInstance without business key
	pi3, err := e.StartProcessInstance(ctx, "bk1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if pi3.BusinessKey != "" {
		t.Errorf("expected empty BusinessKey, got %q", pi3.BusinessKey)
	}
}

func TestBusinessKey_WithSqlStore(t *testing.T) {
	ctx := context.Background()
	s := sqlstore.New(sqlstore.WithMemory())
	t.Cleanup(func() { s.Close() })
	e := NewProcessEngine(s)

	def := &spec.ProcessDefinition{
		ID: "bk_sql", Key: "bk_sql", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批", Assignee: "张三"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	pi, err := e.StartProcessInstanceWithBusinessKey(ctx, "bk_sql", "BIZ-999", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if pi.BusinessKey != "BIZ-999" {
		t.Errorf("BusinessKey=%q, want BIZ-999", pi.BusinessKey)
	}

	// Verify via QueryStore
	results, total, err := s.QueryProcessInstances(ctx, storage.InstQuery{BusinessKey: "BIZ-999", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(results) != 1 {
		t.Errorf("expected 1, got total=%d len=%d", total, len(results))
	}
	if results[0].BusinessKey != "BIZ-999" {
		t.Errorf("got BusinessKey=%q", results[0].BusinessKey)
	}
}
