// Package instance defines the runtime state models for the workflow engine.
// These represent the live state of running process instances,
// activity execution records, and execution tokens.
package instance

import "time"

// ProcessInstanceState enumerates the lifecycle states of a process instance.
type ProcessInstanceState string

const (
	ProcessInstanceStateRunning    ProcessInstanceState = "running"
	ProcessInstanceStateSuspended  ProcessInstanceState = "suspended"
	ProcessInstanceStateCompleted  ProcessInstanceState = "completed"
	ProcessInstanceStateTerminated ProcessInstanceState = "terminated"
	ProcessInstanceStateRejected   ProcessInstanceState = "rejected"
)

// ProcessInstance represents one execution of a ProcessDefinition.
type ProcessInstance struct {
	ID                  string
	ProcessDefinitionID string
	BusinessKey         string // external business identifier (e.g. order ID, ticket number)
	State               ProcessInstanceState
	Variables           map[string]any
	StartedAt           time.Time
	EndedAt             *time.Time
	ParentProcessInstanceID string // set when this is a sub-process (CallActivity)
	ParentActivityID        string // the CallActivity element ID in the parent
}

// NewProcessInstance creates a new process instance in the running state.
func NewProcessInstance(id, defID string, variables map[string]any) *ProcessInstance {
	now := time.Now()
	vars := variables
	if vars == nil {
		vars = make(map[string]any)
	}
	return &ProcessInstance{
		ID:                  id,
		ProcessDefinitionID: defID,
		State:               ProcessInstanceStateRunning,
		Variables:           vars,
		StartedAt:           now,
	}
}
