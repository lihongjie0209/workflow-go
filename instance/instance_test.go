package instance

import (
	"testing"
	"time"
)

func TestNewProcessInstance(t *testing.T) {
	pi := NewProcessInstance("pi1", "proc1", map[string]any{"key": "val"})
	if pi.ID != "pi1" {
		t.Errorf("ID = %q, want %q", pi.ID, "pi1")
	}
	if pi.ProcessDefinitionID != "proc1" {
		t.Errorf("ProcessDefinitionID = %q, want %q", pi.ProcessDefinitionID, "proc1")
	}
	if pi.State != ProcessInstanceStateRunning {
		t.Errorf("State = %q, want %q", pi.State, ProcessInstanceStateRunning)
	}
	if pi.Variables["key"] != "val" {
		t.Errorf("Variables[key] = %v, want %v", pi.Variables["key"], "val")
	}
	if pi.StartedAt.IsZero() {
		t.Error("StartedAt should not be zero")
	}
	if pi.EndedAt != nil {
		t.Error("EndedAt should be nil for a new instance")
	}
}

func TestNewProcessInstance_NilVariables(t *testing.T) {
	pi := NewProcessInstance("pi2", "proc1", nil)
	if pi.Variables == nil {
		t.Error("Variables should not be nil when initialized with nil")
	}
	if len(pi.Variables) != 0 {
		t.Errorf("expected empty variables, got %d", len(pi.Variables))
	}
}

func TestNewActivityInstance(t *testing.T) {
	ai := NewActivityInstance("ai1", "pi1", "task1", "userTask")
	if ai.ID != "ai1" {
		t.Errorf("ID = %q, want %q", ai.ID, "ai1")
	}
	if ai.ProcessInstanceID != "pi1" {
		t.Errorf("ProcessInstanceID = %q, want %q", ai.ProcessInstanceID, "pi1")
	}
	if ai.ActivityID != "task1" {
		t.Errorf("ActivityID = %q, want %q", ai.ActivityID, "task1")
	}
	if ai.State != ActivityStateActive {
		t.Errorf("State = %q, want %q", ai.State, ActivityStateActive)
	}
}

func TestActivityInstance_Complete(t *testing.T) {
	ai := NewActivityInstance("ai1", "pi1", "task1", "userTask")
	ai.Complete()

	if ai.State != ActivityStateCompleted {
		t.Errorf("State = %q, want %q", ai.State, ActivityStateCompleted)
	}
	if ai.CompletedTime == nil {
		t.Fatal("CompletedTime should not be nil after complete")
	}
	if time.Since(*ai.CompletedTime) > time.Second {
		t.Errorf("CompletedTime should be recent, got %v", *ai.CompletedTime)
	}
}

func TestNewToken(t *testing.T) {
	tok := NewToken("t1", "pi1", "start1")
	if tok.ID != "t1" {
		t.Errorf("ID = %q, want %q", tok.ID, "t1")
	}
	if tok.ProcessInstanceID != "pi1" {
		t.Errorf("ProcessInstanceID = %q, want %q", tok.ProcessInstanceID, "pi1")
	}
	if tok.CurrentElementID != "start1" {
		t.Errorf("CurrentElementID = %q, want %q", tok.CurrentElementID, "start1")
	}
	if tok.State != TokenStateActive {
		t.Errorf("State = %q, want %q", tok.State, TokenStateActive)
	}
	if tok.CreatedAt.IsZero() {
		t.Error("CreatedAt should not be zero")
	}
}
