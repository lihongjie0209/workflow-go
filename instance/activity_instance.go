package instance

import (
	"time"

	"github.com/lihongjie/workflow-go/spec"
)

// ActivityInstanceState enumerates the lifecycle states of an activity instance.
type ActivityInstanceState string

const (
	ActivityStateActive    ActivityInstanceState = "active"
	ActivityStateUnclaimed ActivityInstanceState = "unclaimed"
	ActivityStateCompleted ActivityInstanceState = "completed"
)

// ActivityInstance records one visit to a flow element during process execution.
// For wait-state elements (UserTask), the instance remains "active" until
// explicitly completed. For automatic elements (StartEvent, EndEvent, gateways,
// ServiceTask), the instance transitions to "completed" immediately.
// For multi-instance activities (会签), MultiInstanceLoopID groups instances
// that belong to the same loop, and LoopCounter tracks the 0-based index.
// Assignee records the resolved user/expression value when the task was created.
type ActivityInstance struct {
	ID                  string
	ProcessInstanceID   string
	ActivityID          string
	ActivityType        spec.ElementType
	State               ActivityInstanceState
	Assignee            string `json:"assignee,omitempty"`
	ClaimTime           *time.Time
	CompletedTime       *time.Time
	MultiInstanceLoopID string `json:"multiInstanceLoopID,omitempty"`
	LoopCounter         int    `json:"loopCounter,omitempty"`
	AdhocParentID       string    `json:"adhocParentID,omitempty"` // 加签: 父活动实例ID
	ExpireTime          *time.Time `json:"expireTime,omitempty"`  // 超时时间
	TermMode            int        `json:"termMode,omitempty"`    // 超时模式: 0=无, 1=自动通过, 2=自动驳回
}

// NewActivityInstance creates a new activity instance in the active state.
func NewActivityInstance(id, processInstanceID string, activityID string, activityType spec.ElementType) *ActivityInstance {
	return &ActivityInstance{
		ID:                id,
		ProcessInstanceID: processInstanceID,
		ActivityID:        activityID,
		ActivityType:      activityType,
		State:             ActivityStateActive,
	}
}

// Complete marks the activity instance as completed at the current time.
func (ai *ActivityInstance) Complete() {
	now := time.Now()
	ai.State = ActivityStateCompleted
	ai.CompletedTime = &now
}
