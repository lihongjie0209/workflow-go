package engine

import (
	"context"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// ExecutionListener hooks into element execution lifecycle events.
// NOTE: As of v1, listeners are defined but NOT yet invoked by the engine.
// TODO: Call OnStart/OnEnd in navigator.startFrom/navigateFrom,
// and OnCreate/OnComplete in engine.CompleteTask.
type ExecutionListener interface {
	// OnStart is called when execution arrives at an element.
	OnStart(ctx context.Context, processInstance *instance.ProcessInstance, activityInstance *instance.ActivityInstance, element spec.FlowElement) error
	// OnEnd is called when execution leaves an element.
	OnEnd(ctx context.Context, processInstance *instance.ProcessInstance, activityInstance *instance.ActivityInstance, element spec.FlowElement) error
}

// TaskListener hooks into human task lifecycle events.
type TaskListener interface {
	// OnCreate is called when a new UserTask activity instance is created.
	OnCreate(ctx context.Context, activityInstance *instance.ActivityInstance) error
	// OnComplete is called when a UserTask is completed.
	OnComplete(ctx context.Context, activityInstance *instance.ActivityInstance, variables map[string]any) error
}

// EngineOption configures the ProcessEngine.
type EngineOption func(*ProcessEngine)

// WithExecutionListener adds an execution listener to the engine.
func WithExecutionListener(l ExecutionListener) EngineOption {
	return func(e *ProcessEngine) {
		e.executionListeners = append(e.executionListeners, l)
	}
}

// WithTaskListener adds a task listener to the engine.
func WithTaskListener(l TaskListener) EngineOption {
	return func(e *ProcessEngine) {
		e.taskListeners = append(e.taskListeners, l)
	}
}
