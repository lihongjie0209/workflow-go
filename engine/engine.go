// Package engine provides the core workflow execution engine.
// It processes process definitions by creating process instances,
// navigating through flow elements, and managing gateway logic.
package engine

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"

	exprlang "github.com/expr-lang/expr"
)

var varPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

// ProcessEngine is the main entry point for workflow execution.
// It is stateless: all runtime state is managed through the storage.Store interface.
// This design supports concurrent access to the same process instances
// from multiple goroutines.
type ProcessEngine struct {
	store              storage.Store
	executionListeners []ExecutionListener
	taskListeners      []TaskListener
}

// NewProcessEngine creates a new workflow engine with the given storage backend.
// Optional listeners can be provided via EngineOption.
func NewProcessEngine(store storage.Store, opts ...EngineOption) *ProcessEngine {
	e := &ProcessEngine{store: store}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

// StartProcessInstance creates a new process instance from the given definition,
// sets the initial variables, and begins execution from the start event.
func (e *ProcessEngine) StartProcessInstance(ctx context.Context, defID string, variables map[string]any) (*instance.ProcessInstance, error) {
	def, err := e.store.GetProcessDefinition(ctx, defID)
	if err != nil {
		return nil, fmt.Errorf("engine: get definition %q: %w", defID, err)
	}

	if err := def.Validate(); err != nil {
		return nil, fmt.Errorf("engine: invalid definition %q: %w", defID, err)
	}

	if variables == nil {
		variables = make(map[string]any)
	}

	pi := instance.NewProcessInstance(newID(), defID, variables)
	if err := e.store.CreateProcessInstance(ctx, pi); err != nil {
		return nil, fmt.Errorf("engine: create instance: %w", err)
	}

	for k, v := range variables {
		if err := e.store.SetVariable(ctx, pi.ID, k, v); err != nil {
			return nil, fmt.Errorf("engine: set variable %q: %w", k, err)
		}
	}

	n := &navigator{store: e.store}
	if err := n.startFrom(ctx, def, pi); err != nil {
		return nil, fmt.Errorf("engine: navigate from start: %w", err)
	}

	return pi, nil
}

// CompleteTask completes an active activity instance and advances the
// process execution. If variables are provided, they are merged into
// the process instance's variables before continuing.
func (e *ProcessEngine) CompleteTask(ctx context.Context, activityInstanceID string, variables map[string]any) error {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get activity instance %q: %w", activityInstanceID, err)
	}

	if ai.State != instance.ActivityStateActive {
		return fmt.Errorf("engine: activity %q is not active, current state: %s", activityInstanceID, ai.State)
	}

	// Check that this is a UserTask
	if ai.ActivityType != spec.ElementTypeUserTask {
		return fmt.Errorf("engine: activity %q is a %s, not a userTask", activityInstanceID, ai.ActivityType)
	}

	ai.Complete()
	if err := e.store.UpdateActivityInstance(ctx, ai); err != nil {
		return fmt.Errorf("engine: update activity instance %q: %w", activityInstanceID, err)
	}
	recordHistory(ctx, e.store, ai)

	// Clean up any timer jobs or signal subscriptions for this activity.
	if err := e.store.DeleteTimerJobsByInstance(ctx, ai.ProcessInstanceID); err != nil {
		return fmt.Errorf("engine: cleanup timer jobs: %w", err)
	}
	if err := e.store.DeleteSubscriptionsByInstance(ctx, ai.ProcessInstanceID); err != nil {
		return fmt.Errorf("engine: cleanup subscriptions: %w", err)
	}

	// Merge variables if provided
	if len(variables) > 0 {
		pi, err := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
		if err != nil {
			return fmt.Errorf("engine: get instance %q: %w", ai.ProcessInstanceID, err)
		}
		if pi.Variables == nil {
			pi.Variables = make(map[string]any)
		}
		for k, v := range variables {
			pi.Variables[k] = v
			if err := e.store.SetVariable(ctx, pi.ID, k, v); err != nil {
				return fmt.Errorf("engine: set variable %q: %w", k, err)
			}
		}
		if err := e.store.UpdateProcessInstance(ctx, pi); err != nil {
			return fmt.Errorf("engine: update instance %q: %w", ai.ProcessInstanceID, err)
		}
	}

	n := &navigator{store: e.store}

	// If this is a sign activity, handle sign completion.
	if ai.AdhocParentID != "" {
		pi2, _ := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
		return n.handleSignCompletion(ctx, pi2, ai, variables)
	}

	// If there are pending forward/parallel signs, block navigation.
	pi2, _ := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
	if hasPendingSigns(ctx, e.store, pi2, ai) {
		return nil // wait for signers to complete
	}

	if err := n.navigateFrom(ctx, ai.ProcessInstanceID, ai.ActivityID); err != nil {
		return fmt.Errorf("engine: navigate from task %q: %w", ai.ActivityID, err)
	}

	return nil
}

// GetStore returns the underlying storage store.
// SignType enumerates the types of ad-hoc sign (加签).
type SignType string

const (
	SignForward  SignType = "forward"  // 前加签: 加签人先审，当前人再审
	SignBackward SignType = "backward" // 后加签: 当前人先审，加签人再审
	SignParallel SignType = "parallel" // 并签: 当前人与加签人并列审批
)

// SignStrategy enumerates the signing strategies.
type SignStrategy string

const (
	StrategyOR  SignStrategy = "or"  // 或签: 任一加签人通过即可
	StrategyAND SignStrategy = "and" // 会签: 所有加签人必须全部通过
)

// AddSign adds ad-hoc signers to an active activity instance.
// This creates additional approval tasks without modifying the process definition.
func (e *ProcessEngine) AddSign(ctx context.Context, activityInstanceID string, signType SignType, strategy SignStrategy, assignees []string) error {
	if len(assignees) == 0 {
		return fmt.Errorf("engine: at least one assignee is required for add sign")
	}
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get activity instance %q: %w", activityInstanceID, err)
	}
	if ai.State != instance.ActivityStateActive {
		return fmt.Errorf("engine: activity %q is not active", activityInstanceID)
	}
	if ai.ActivityType != spec.ElementTypeUserTask {
		return fmt.Errorf("engine: activity %q is not a userTask", activityInstanceID)
	}
	if ai.AdhocParentID != "" {
		return fmt.Errorf("engine: cannot add sign to a sign activity itself")
	}
	pi, err := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
	if err != nil {
		return err
	}
	if pi.State != instance.ProcessInstanceStateRunning {
		return fmt.Errorf("engine: process instance %q is not running", pi.ID)
	}
	signID := newID()
	_ = e.store.SetVariable(ctx, pi.ID, signID+"_type", string(signType))
	_ = e.store.SetVariable(ctx, pi.ID, signID+"_strategy", string(strategy))
	_ = e.store.SetVariable(ctx, pi.ID, signID+"_total", float64(len(assignees)))
	_ = e.store.SetVariable(ctx, pi.ID, signID+"_completed", float64(0))
	_ = e.store.SetVariable(ctx, pi.ID, signID+"_approved", float64(0))
	_ = e.store.SetVariable(ctx, pi.ID, signID+"_parent", ai.ID)
	for _, assignee := range assignees {
		v, _ := e.store.GetAllVariables(ctx, pi.ID)
		resolved := RenderTemplate(assignee, v)
		signAI := instance.NewActivityInstance(newID(), pi.ID, ai.ActivityID, spec.ElementTypeUserTask)
		signAI.Assignee = resolved
		signAI.AdhocParentID = ai.ID
		if err := e.store.CreateActivityInstance(ctx, signAI); err != nil {
			return fmt.Errorf("engine: create sign activity: %w", err)
		}
		signTok := instance.NewToken(newID(), pi.ID, ai.ActivityID)
		if err := e.store.CreateToken(ctx, signTok); err != nil {
			return fmt.Errorf("engine: create sign token: %w", err)
		}
	}
	if signType == SignBackward {
		ai.Complete()
		if err := e.store.UpdateActivityInstance(ctx, ai); err != nil { return err }
		tokens, _ := e.store.ListActiveTokens(ctx, pi.ID)
		for _, tok := range tokens {
			if tok.CurrentElementID == ai.ActivityID && tok.State == instance.TokenStateActive {
				tok.State = instance.TokenStateConsumed
				e.store.UpdateToken(ctx, tok)
				break
			}
		}
	}
	return nil
}

func (e *ProcessEngine) GetStore() storage.Store {
	return e.store
}

// SuspendProcessInstance pauses a running process instance.
// No further navigation occurs until ResumeProcessInstance is called.
func (e *ProcessEngine) SuspendProcessInstance(ctx context.Context, processInstanceID string) error {
	pi, err := e.store.GetProcessInstance(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get instance %q: %w", processInstanceID, err)
	}
	if pi.State != instance.ProcessInstanceStateRunning {
		return fmt.Errorf("engine: instance %q is not running, current state: %s", processInstanceID, pi.State)
	}
	pi.State = instance.ProcessInstanceStateSuspended
	return e.store.UpdateProcessInstance(ctx, pi)
}

// ResumeProcessInstance resumes a suspended process instance.
func (e *ProcessEngine) ResumeProcessInstance(ctx context.Context, processInstanceID string) error {
	pi, err := e.store.GetProcessInstance(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get instance %q: %w", processInstanceID, err)
	}
	if pi.State != instance.ProcessInstanceStateSuspended {
		return fmt.Errorf("engine: instance %q is not suspended, current state: %s", processInstanceID, pi.State)
	}
	pi.State = instance.ProcessInstanceStateRunning
	return e.store.UpdateProcessInstance(ctx, pi)
}

// TerminateProcessInstance forcibly ends a running or suspended process instance.
// All active tokens are consumed and all active activities are completed.
func (e *ProcessEngine) TerminateProcessInstance(ctx context.Context, processInstanceID string) error {
	pi, err := e.store.GetProcessInstance(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get instance %q: %w", processInstanceID, err)
	}
	if pi.State == instance.ProcessInstanceStateCompleted || pi.State == instance.ProcessInstanceStateTerminated {
		return fmt.Errorf("engine: instance %q is already ended, state: %s", processInstanceID, pi.State)
	}

	// Consume all active tokens.
	tokens, err := e.store.ListActiveTokens(ctx, processInstanceID)
	if err != nil {
		return err
	}
	for _, tok := range tokens {
		tok.State = instance.TokenStateConsumed
		if err := e.store.UpdateToken(ctx, tok); err != nil {
			return err
		}
	}

	// Complete all active activities.
	activities, err := e.store.ListActiveActivities(ctx, processInstanceID)
	if err != nil {
		return err
	}
	for _, ai := range activities {
		ai.Complete()
		if err := e.store.UpdateActivityInstance(ctx, ai); err != nil {
			return err
		}
	}

	// Mark instance as terminated.
	pi.State = instance.ProcessInstanceStateTerminated
	now := time.Now()
	pi.EndedAt = &now
	return e.store.UpdateProcessInstance(ctx, pi)
}

// ReceiveSignal sends a signal to all waiting catch events or boundary events.
func (e *ProcessEngine) ReceiveSignal(ctx context.Context, signalRef string, variables map[string]any) error {
	n := &navigator{store: e.store}
	if err := n.fireSignal(ctx, signalRef, variables); err != nil {
		return fmt.Errorf("engine: receive signal %q: %w", signalRef, err)
	}
	return nil
}

// ReceiveMessage sends a message to all waiting catch events.
// In v1, messages use the same subscription mechanism as signals.
func (e *ProcessEngine) ReceiveMessage(ctx context.Context, messageRef string, variables map[string]any) error {
	// For v1, messages are treated like signals with messageRef as the signal name.
	// A full implementation would use a separate MessageSubscription store.
	return e.ReceiveSignal(ctx, messageRef, variables)
}

// RenderExpr evaluates a complete ${expression} string against process variables.
// Returns the evaluated result as a string, or the original expression string on error.
//
//	"${approver}"       → "张三"
//	"${user.manager}"   → "李四"
//	"${nrOfInstances}"  → "3"
func RenderExpr(input string, vars map[string]any) string {
	if input == "" || len(input) < 4 || input[:2] != "${" || input[len(input)-1] != '}' {
		return input
	}
	exprStr := input[2 : len(input)-1]
	program, err := exprlang.Compile(exprStr)
	if err != nil {
		return input
	}
	result, err := exprlang.Run(program, vars)
	if err != nil {
		return input
	}
	return fmt.Sprintf("%v", result)
}

// RenderTemplate substitutes all ${expression} occurrences in a string template
// with their evaluated values against process variables.
//
//	"请${applicant}审批"  → "请张三审批"
//	"${manager}您好"     → "李四您好"
//	"报销单-${type}"     → "报销单-差旅"
//
// Non-resolvable expressions are left as-is in the output.
func RenderTemplate(tmpl string, vars map[string]any) string {
	if tmpl == "" || !strings.Contains(tmpl, "${") {
		return tmpl
	}
	return varPattern.ReplaceAllStringFunc(tmpl, func(match string) string {
		inner := match[2 : len(match)-1]
		program, err := exprlang.Compile(inner)
		if err != nil {
			return match
		}
		result, err := exprlang.Run(program, vars)
		if err != nil {
			return match
		}
		return fmt.Sprintf("%v", result)
	})
}

// RecordHistory saves a historic snapshot of a completed activity instance.
func recordHistory(ctx context.Context, store storage.Store, ai *instance.ActivityInstance) {
	if ai.State != instance.ActivityStateCompleted || ai.CompletedTime == nil {
		return
	}
	vars, err := store.GetAllVariables(ctx, ai.ProcessInstanceID)
	if err != nil {
		return
	}
	hai := &instance.HistoricActivityInstance{
		ID:                ai.ID,
		ProcessInstanceID: ai.ProcessInstanceID,
		ActivityID:        ai.ActivityID,
		ActivityType:      ai.ActivityType,
		Variables:         vars,
	}
	// Find the activity's start time by looking at created activity instances.
	// Since we don't store start time separately, use a reasonable estimate.
	if ai.CompletedTime != nil {
		hai.CompletedAt = *ai.CompletedTime
	}
	_ = store.CreateHistoricActivityInstance(ctx, hai)
}

// Sentinel errors.
var (
	ErrProcessNotFound     = storage.ErrNotFound
	ErrActivityNotFound    = fmt.Errorf("activity not found")
	ErrTaskAlreadyDone     = fmt.Errorf("task already completed")
	ErrInstanceNotRunning  = fmt.Errorf("process instance is not running")
)

// --- UUID generation ---

var (
	idMu     sync.Mutex
	idCounter int64
)

func newID() string {
	idMu.Lock()
	idCounter++
	c := idCounter
	idMu.Unlock()
	ts := time.Now().UnixNano()
	return fmt.Sprintf("%x%x", ts, c)
}
