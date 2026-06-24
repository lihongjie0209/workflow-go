// Package storagetest provides a reusable test suite for any storage.Store implementation.
// Run the suite with RunStoreTestSuite to verify that a store implementation
// correctly satisfies the storage.Store contract.
package storagetest

import (
	"context"
	"testing"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
)

// RunStoreTestSuite runs the full storage contract test suite against the given store.
// Call this from each storage implementation's test file:
//
//	func TestStore(t *testing.T) {
//	    s := memstore.New()
//	    storagetest.RunStoreTestSuite(t, s)
//	}
func RunStoreTestSuite(t *testing.T, s storage.Store) {
	t.Run("ProcessDefinitionCRUD", func(t *testing.T) { testProcessDefinitionCRUD(t, s) })
	t.Run("ProcessInstanceCRUD", func(t *testing.T) { testProcessInstanceCRUD(t, s) })
	t.Run("ActivityInstanceCRUD", func(t *testing.T) { testActivityInstanceCRUD(t, s) })
	t.Run("TokenCRUD", func(t *testing.T) { testTokenCRUD(t, s) })
	t.Run("Variables", func(t *testing.T) { testVariables(t, s) })
}

func testProcessDefinitionCRUD(t *testing.T, s storage.Store) {
	ctx := context.Background()
	def := &spec.ProcessDefinition{
		ID:   "def1",
		Name: "Test Process",
		Key:  "test-process",
		Version: 1,
		Elements: map[string]spec.FlowElement{
			"start1": &spec.StartEvent{ID: "start1"},
			"end1":   &spec.EndEvent{ID: "end1"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "sf1", SourceRef: "start1", TargetRef: "end1"},
		},
		StartEventID: "start1",
	}

	// Create
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatalf("CreateProcessDefinition: %v", err)
	}

	// Get
	got, err := s.GetProcessDefinition(ctx, "def1")
	if err != nil {
		t.Fatalf("GetProcessDefinition: %v", err)
	}
	if got.ID != "def1" || got.Name != "Test Process" {
		t.Errorf("got id=%q name=%q, want id=def1 name=Test Process", got.ID, got.Name)
	}

	// Get by key/version
	got, err = s.GetProcessDefinitionByKeyVersion(ctx, "test-process", 1)
	if err != nil {
		t.Fatalf("GetProcessDefinitionByKeyVersion: %v", err)
	}
	if got.ID != "def1" {
		t.Errorf("got id=%q, want def1", got.ID)
	}

	// List
	defs, err := s.ListProcessDefinitions(ctx)
	if err != nil {
		t.Fatalf("ListProcessDefinitions: %v", err)
	}
	if len(defs) != 1 {
		t.Errorf("got %d definitions, want 1", len(defs))
	}

	// Delete
	if err := s.DeleteProcessDefinition(ctx, "def1"); err != nil {
		t.Fatalf("DeleteProcessDefinition: %v", err)
	}
	_, err = s.GetProcessDefinition(ctx, "def1")
	if err == nil {
		t.Error("expected error after deleting definition")
	}

	// Get not found
	_, err = s.GetProcessDefinition(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent definition")
	}
}

func testProcessInstanceCRUD(t *testing.T, s storage.Store) {
	ctx := context.Background()
	pi := instance.NewProcessInstance("pi1", "def1", map[string]any{"key": "value"})

	// Create
	if err := s.CreateProcessInstance(ctx, pi); err != nil {
		t.Fatalf("CreateProcessInstance: %v", err)
	}

	// Get
	got, err := s.GetProcessInstance(ctx, "pi1")
	if err != nil {
		t.Fatalf("GetProcessInstance: %v", err)
	}
	if got.State != instance.ProcessInstanceStateRunning {
		t.Errorf("got state=%q, want running", got.State)
	}
	if got.Variables["key"] != "value" {
		t.Errorf("got Variables[key]=%v, want value", got.Variables["key"])
	}

	// Update
	pi.State = instance.ProcessInstanceStateCompleted
	now := time.Now()
	pi.EndedAt = &now
	if err := s.UpdateProcessInstance(ctx, pi); err != nil {
		t.Fatalf("UpdateProcessInstance: %v", err)
	}
	got, err = s.GetProcessInstance(ctx, "pi1")
	if err != nil {
		t.Fatalf("GetProcessInstance after update: %v", err)
	}
	if got.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("got state=%q, want completed", got.State)
	}
	if got.EndedAt == nil {
		t.Error("EndedAt should not be nil after completion")
	}

	// List
	list, err := s.ListProcessInstances(ctx, "def1")
	if err != nil {
		t.Fatalf("ListProcessInstances: %v", err)
	}
	if len(list) != 1 {
		t.Errorf("got %d instances, want 1", len(list))
	}

	// Not found
	_, err = s.GetProcessInstance(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent instance")
	}
}

func testActivityInstanceCRUD(t *testing.T, s storage.Store) {
	ctx := context.Background()
	// First create a process instance (required for foreign key in sqlstore)
	pi := instance.NewProcessInstance("pi_act", "def1", nil)
	if err := s.CreateProcessInstance(ctx, pi); err != nil {
		t.Fatalf("CreateProcessInstance: %v", err)
	}

	ai := instance.NewActivityInstance("ai1", "pi_act", "task1", spec.ElementTypeUserTask)

	// Create
	if err := s.CreateActivityInstance(ctx, ai); err != nil {
		t.Fatalf("CreateActivityInstance: %v", err)
	}

	// Get
	got, err := s.GetActivityInstance(ctx, "ai1")
	if err != nil {
		t.Fatalf("GetActivityInstance: %v", err)
	}
	if got.State != instance.ActivityStateActive {
		t.Errorf("got state=%q, want active", got.State)
	}

	// Update
	ai.Complete()
	if err := s.UpdateActivityInstance(ctx, ai); err != nil {
		t.Fatalf("UpdateActivityInstance: %v", err)
	}
	got, err = s.GetActivityInstance(ctx, "ai1")
	if err != nil {
		t.Fatalf("GetActivityInstance after update: %v", err)
	}
	if got.State != instance.ActivityStateCompleted {
		t.Errorf("got state=%q, want completed", got.State)
	}

	// List active
	ai2 := instance.NewActivityInstance("ai2", "pi_act", "task2", spec.ElementTypeUserTask)
	if err := s.CreateActivityInstance(ctx, ai2); err != nil {
		t.Fatalf("CreateActivityInstance: %v", err)
	}
	active, err := s.ListActiveActivities(ctx, "pi_act")
	if err != nil {
		t.Fatalf("ListActiveActivities: %v", err)
	}
	if len(active) != 1 {
		t.Errorf("got %d active activities, want 1", len(active))
	}

	// List by process instance
	all, err := s.ListActivitiesByProcessInstance(ctx, "pi_act")
	if err != nil {
		t.Fatalf("ListActivitiesByProcessInstance: %v", err)
	}
	if len(all) != 2 {
		t.Errorf("got %d activities, want 2", len(all))
	}
}

func testTokenCRUD(t *testing.T, s storage.Store) {
	ctx := context.Background()
	pi := instance.NewProcessInstance("pi_tok", "def1", nil)
	if err := s.CreateProcessInstance(ctx, pi); err != nil {
		t.Fatalf("CreateProcessInstance: %v", err)
	}

	tok := instance.NewToken("t1", "pi_tok", "start1")

	// Create
	if err := s.CreateToken(ctx, tok); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}

	// Get
	got, err := s.GetToken(ctx, "t1")
	if err != nil {
		t.Fatalf("GetToken: %v", err)
	}
	if got.State != instance.TokenStateActive {
		t.Errorf("got state=%q, want active", got.State)
	}
	if got.CurrentElementID != "start1" {
		t.Errorf("got CurrentElementID=%q, want start1", got.CurrentElementID)
	}

	// Update
	tok.State = instance.TokenStateConsumed
	if err := s.UpdateToken(ctx, tok); err != nil {
		t.Fatalf("UpdateToken: %v", err)
	}
	got, err = s.GetToken(ctx, "t1")
	if err != nil {
		t.Fatalf("GetToken after update: %v", err)
	}
	if got.State != instance.TokenStateConsumed {
		t.Errorf("got state=%q, want consumed", got.State)
	}

	// List active
	tok2 := instance.NewToken("t2", "pi_tok", "task1")
	if err := s.CreateToken(ctx, tok2); err != nil {
		t.Fatalf("CreateToken: %v", err)
	}
	active, err := s.ListActiveTokens(ctx, "pi_tok")
	if err != nil {
		t.Fatalf("ListActiveTokens: %v", err)
	}
	if len(active) != 1 {
		t.Errorf("got %d active tokens, want 1", len(active))
	}

	// Delete
	if err := s.DeleteToken(ctx, "t1"); err != nil {
		t.Fatalf("DeleteToken: %v", err)
	}
	_, err = s.GetToken(ctx, "t1")
	if err == nil {
		t.Error("expected error after deleting token")
	}
}

func testVariables(t *testing.T, s storage.Store) {
	ctx := context.Background()
	piID := "pi_var"

	// Set variables
	if err := s.SetVariable(ctx, piID, "name", "test"); err != nil {
		t.Fatalf("SetVariable: %v", err)
	}
	if err := s.SetVariable(ctx, piID, "count", 42); err != nil {
		t.Fatalf("SetVariable: %v", err)
	}
	if err := s.SetVariable(ctx, piID, "enabled", true); err != nil {
		t.Fatalf("SetVariable: %v", err)
	}

	// Get single variable
	val, err := s.GetVariable(ctx, piID, "name")
	if err != nil {
		t.Fatalf("GetVariable: %v", err)
	}
	if val != "test" {
		t.Errorf("got name=%v, want test", val)
	}

	// Get all variables
	vars, err := s.GetAllVariables(ctx, piID)
	if err != nil {
		t.Fatalf("GetAllVariables: %v", err)
	}
	if len(vars) != 3 {
		t.Errorf("got %d variables, want 3", len(vars))
	}
	if vars["count"] != float64(42) && vars["count"] != 42 {
		t.Errorf("got count=%v (type %T), want 42", vars["count"], vars["count"])
	}

	// Delete variable
	if err := s.DeleteVariable(ctx, piID, "name"); err != nil {
		t.Fatalf("DeleteVariable: %v", err)
	}
	_, err = s.GetVariable(ctx, piID, "name")
	if err == nil {
		t.Error("expected error after deleting variable")
	}

	// Get all after delete
	vars, err = s.GetAllVariables(ctx, piID)
	if err != nil {
		t.Fatalf("GetAllVariables: %v", err)
	}
	if len(vars) != 2 {
		t.Errorf("got %d variables, want 2", len(vars))
	}

	// Nonexistent variable
	_, err = s.GetVariable(ctx, "nonexistent", "x")
	if err == nil {
		t.Error("expected error for nonexistent variable")
	}
}
