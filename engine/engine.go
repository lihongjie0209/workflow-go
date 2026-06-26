package engine

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/lihongjie/workflow-go/identity"
	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"

	exprlang "github.com/expr-lang/expr"
)

var varPattern = regexp.MustCompile(`\$\{([^}]+)\}`)

type ProcessEngine struct {
	store              storage.Store
	identity           identity.Service
	executionListeners []ExecutionListener
	taskListeners      []TaskListener
}

func WithIdentityService(svc identity.Service) EngineOption {
	return func(e *ProcessEngine) {
		e.identity = svc
	}
}

func NewProcessEngine(store storage.Store, opts ...EngineOption) *ProcessEngine {
	e := &ProcessEngine{store: store}
	for _, opt := range opts {
		opt(e)
	}
	return e
}

func (e *ProcessEngine) StartProcessInstance(ctx context.Context, defID string, variables map[string]any) (*instance.ProcessInstance, error) {
	return e.StartProcessInstanceWithBusinessKey(ctx, defID, "", "", variables)
}

func (e *ProcessEngine) StartProcessInstanceWithBusinessKey(ctx context.Context, defID, businessKey, tenantID string, variables map[string]any) (*instance.ProcessInstance, error) {
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
	pi.BusinessKey = businessKey
	pi.TenantID = tenantID
	if err := e.store.CreateProcessInstance(ctx, pi); err != nil {
		return nil, fmt.Errorf("engine: create instance: %w", err)
	}
	for k, v := range variables {
		if err := e.store.SetVariable(ctx, pi.ID, k, v); err != nil {
			return nil, fmt.Errorf("engine: set variable %q: %w", k, err)
		}
	}
	n := &navigator{store: e.store, identity: e.identity}
	if err := n.startFrom(ctx, def, pi); err != nil {
		return nil, fmt.Errorf("engine: navigate from start: %w", err)
	}
	return pi, nil
}

// StartProcessInstanceByKey starts a new process instance by looking up the
// latest version of the process definition with the given key.
func (e *ProcessEngine) StartProcessInstanceByKey(ctx context.Context, key, businessKey, tenantID string, variables map[string]any) (*instance.ProcessInstance, error) {
	def, err := e.store.GetLatestProcessDefinitionByKey(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("engine: get definition by key %q: %w", key, err)
	}
	return e.StartProcessInstanceWithBusinessKey(ctx, def.ID, businessKey, tenantID, variables)
}

func (e *ProcessEngine) CompleteTask(ctx context.Context, activityInstanceID string, variables map[string]any) error {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get activity instance %q: %w", activityInstanceID, err)
	}
	if ai.State != instance.ActivityStateActive {
		if ai.State == instance.ActivityStateUnclaimed {
			return fmt.Errorf("engine: activity %q is unclaimed, must be claimed before completion", activityInstanceID)
		}
		return fmt.Errorf("engine: activity %q is not active, current state: %s", activityInstanceID, ai.State)
	}
	if ai.ActivityType != spec.ElementTypeUserTask {
		return fmt.Errorf("engine: activity %q is a %s, not a userTask", activityInstanceID, ai.ActivityType)
	}
	ai.Complete()
	if err := e.store.UpdateActivityInstance(ctx, ai); err != nil {
		return fmt.Errorf("engine: update activity instance %q: %w", activityInstanceID, err)
	}
	recordHistory(ctx, e.store, ai)

	// Merge variables before sign/delegate handling (so they're visible downstream)
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

	n := &navigator{store: e.store, identity: e.identity}

	// 加签
	if ai.AdhocParentID != "" {
		pi2, err := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
		if err != nil {
			return fmt.Errorf("engine: get instance: %w", err)
		}
		return n.handleSignCompletion(ctx, pi2, ai, variables)
	}
	pi2, err := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get instance: %w", err)
	}
	if hasPendingSigns(ctx, e.store, pi2, ai) {
		return nil
	}

	// 委派: delegate completed, return to original assignee
	// Must consume the token first, since handleDelegateCompletion creates a new one at same element
	tokens, _ := e.store.ListActiveTokens(ctx, ai.ProcessInstanceID)
	for _, tok := range tokens {
		if tok.CurrentElementID == ai.ActivityID && tok.State == instance.TokenStateActive {
			tok.State = instance.TokenStateConsumed
			e.store.UpdateToken(ctx, tok)
			break
		}
	}
	if n.handleDelegateCompletion(ctx, pi2, ai) {
		return nil
	}

	// Clean up timer jobs and signal subscriptions for normal flow advancement.
	if err := e.store.DeleteTimerJobsByInstance(ctx, ai.ProcessInstanceID); err != nil {
		return fmt.Errorf("engine: cleanup timer jobs: %w", err)
	}
	if err := e.store.DeleteSubscriptionsByInstance(ctx, ai.ProcessInstanceID); err != nil {
		return fmt.Errorf("engine: cleanup subscriptions: %w", err)
	}

	if err := n.navigateFrom(ctx, ai.ProcessInstanceID, ai.ActivityID); err != nil {
		return fmt.Errorf("engine: navigate from task %q: %w", ai.ActivityID, err)
	}
	return nil
}

type SignType string

const (
	SignForward  SignType = "forward"
	SignBackward SignType = "backward"
	SignParallel SignType = "parallel"
)

type SignStrategy string

const (
	StrategyOR  SignStrategy = "or"
	StrategyAND SignStrategy = "and"
)

func (e *ProcessEngine) AddSign(ctx context.Context, activityInstanceID string, signType SignType, strategy SignStrategy, assignees []string) error {
	if len(assignees) == 0 {
		return fmt.Errorf("engine: at least one assignee is required")
	}
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get activity: %w", err)
	}
	if ai.State != instance.ActivityStateActive {
		return fmt.Errorf("engine: activity %q is not active", activityInstanceID)
	}
	if ai.ActivityType != spec.ElementTypeUserTask {
		return fmt.Errorf("engine: activity %q is not a userTask", activityInstanceID)
	}
	if ai.AdhocParentID != "" {
		return fmt.Errorf("engine: cannot add sign to a sign activity")
	}
	pi, err := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
	if err != nil {
		return err
	}
	if pi.State != instance.ProcessInstanceStateRunning {
		return fmt.Errorf("engine: process instance is not running")
	}
	sid := newID()
	_ = e.store.SetVariable(ctx, pi.ID, sid+"_type", string(signType))
	_ = e.store.SetVariable(ctx, pi.ID, sid+"_strategy", string(strategy))
	_ = e.store.SetVariable(ctx, pi.ID, sid+"_total", float64(len(assignees)))
	_ = e.store.SetVariable(ctx, pi.ID, sid+"_completed", float64(0))
	_ = e.store.SetVariable(ctx, pi.ID, sid+"_approved", float64(0))
	_ = e.store.SetVariable(ctx, pi.ID, sid+"_parent", ai.ID)
	for _, a := range assignees {
		v, _ := e.store.GetAllVariables(ctx, pi.ID)
		r := RenderTemplate(a, v)
		sai := instance.NewActivityInstance(newID(), pi.ID, ai.ActivityID, spec.ElementTypeUserTask)
		sai.Assignee = r
		sai.AdhocParentID = ai.ID
		if err := e.store.CreateActivityInstance(ctx, sai); err != nil {
			return fmt.Errorf("engine: create sign activity: %w", err)
		}
		st := instance.NewToken(newID(), pi.ID, ai.ActivityID)
		if err := e.store.CreateToken(ctx, st); err != nil {
			return fmt.Errorf("engine: create sign token: %w", err)
		}
	}
	if signType == SignBackward {
		ai.Complete()
		_ = e.store.UpdateActivityInstance(ctx, ai)
		tokens, _ := e.store.ListActiveTokens(ctx, pi.ID)
		for _, tok := range tokens {
			if tok.CurrentElementID == ai.ActivityID && tok.State == instance.TokenStateActive {
				tok.State = instance.TokenStateConsumed
				_ = e.store.UpdateToken(ctx, tok)
				break
			}
		}
	}
	return nil
}

func (e *ProcessEngine) GetStore() storage.Store {
	return e.store
}

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

func (e *ProcessEngine) TerminateProcessInstance(ctx context.Context, processInstanceID string) error {
	pi, err := e.store.GetProcessInstance(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get instance %q: %w", processInstanceID, err)
	}
	if pi.State == instance.ProcessInstanceStateCompleted || pi.State == instance.ProcessInstanceStateTerminated {
		return fmt.Errorf("engine: instance %q is already ended, state: %s", processInstanceID, pi.State)
	}
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
	pi.State = instance.ProcessInstanceStateTerminated
	now := time.Now()
	pi.EndedAt = &now
	return e.store.UpdateProcessInstance(ctx, pi)
}



// TransferTask 转办: 将当前活动转交新审批人。
func (e *ProcessEngine) TransferTask(ctx context.Context, activityInstanceID, newAssignee string) error {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil { return fmt.Errorf("engine: get activity %q: %w", activityInstanceID, err) }
	if ai.State != instance.ActivityStateActive { return fmt.Errorf("engine: activity %q is not active", activityInstanceID) }
	if ai.ActivityType != spec.ElementTypeUserTask { return fmt.Errorf("engine: activity %q is not a userTask", activityInstanceID) }
	_ = e.store.SetVariable(ctx, ai.ProcessInstanceID, "__transfer_"+ai.ID, ai.Assignee+"->"+newAssignee)
	ai.Assignee = newAssignee
	return e.store.UpdateActivityInstance(ctx, ai)
}

// RemoveSign 减签: 移除当前活动的指定加签人。
func (e *ProcessEngine) RemoveSign(ctx context.Context, activityInstanceID, assignee string) error {
	parentAI, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil { return fmt.Errorf("engine: get activity %q: %w", activityInstanceID, err) }
	if parentAI.State != instance.ActivityStateActive { return fmt.Errorf("engine: activity %q is not active", activityInstanceID) }
	pi, _ := e.store.GetProcessInstance(ctx, parentAI.ProcessInstanceID)
	vars, _ := e.store.GetAllVariables(ctx, pi.ID)
	signID := findSignID(vars, parentAI.ID)
	allActs, _ := e.store.ListActivitiesByProcessInstance(ctx, pi.ID)
	var found *instance.ActivityInstance
	for _, a := range allActs {
		if a.AdhocParentID == parentAI.ID && a.Assignee == assignee && a.State == instance.ActivityStateActive { found = a; break }
	}
	if found == nil { return fmt.Errorf("engine: no active sign for %q", assignee) }
	found.Complete()
	e.store.UpdateActivityInstance(ctx, found)
	tokens, _ := e.store.ListActiveTokens(ctx, pi.ID)
	for _, tok := range tokens {
		if tok.CurrentElementID == parentAI.ActivityID && tok.State == instance.TokenStateActive {
			tok.State = instance.TokenStateConsumed
			e.store.UpdateToken(ctx, tok)
			break
		}
	}
	if signID != "" {
		nrCompleted, _ := vars[signID+"_completed"].(float64)
		e.store.SetVariable(ctx, pi.ID, signID+"_completed", nrCompleted+1)
	}
	return nil
}

// ClaimTask 签收: 用户签收候选人任务，成为指定审批人。
func (e *ProcessEngine) ClaimTask(ctx context.Context, activityInstanceID, userID string) error {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get activity %q: %w", activityInstanceID, err)
	}
	if ai.State != instance.ActivityStateUnclaimed {
		return fmt.Errorf("engine: activity %q is not unclaimed, current state: %s", activityInstanceID, ai.State)
	}
	if ai.ActivityType != spec.ElementTypeUserTask {
		return fmt.Errorf("engine: activity %q is not a userTask", activityInstanceID)
	}
	// Verify user is a candidate if identity service is configured
	candidates := e.GetCandidates(ctx, activityInstanceID)
	if len(candidates) > 0 {
		found := false
		for _, c := range candidates {
			if c == userID {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("engine: user %q is not a candidate for activity %q", userID, activityInstanceID)
		}
	}
	now := time.Now()
	ai.Assignee = userID
	ai.ClaimTime = &now
	ai.State = instance.ActivityStateActive
	return e.store.UpdateActivityInstance(ctx, ai)
}

// UnclaimTask 归还: 签收人归还任务，回到候选人池。
func (e *ProcessEngine) UnclaimTask(ctx context.Context, activityInstanceID string) error {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get activity %q: %w", activityInstanceID, err)
	}
	if ai.State != instance.ActivityStateActive {
		return fmt.Errorf("engine: activity %q is not claimed, current state: %s", activityInstanceID, ai.State)
	}
	// Only allow unclaim if this task has candidates (was created as unclaimed)
	candidates := e.GetCandidates(ctx, activityInstanceID)
	if len(candidates) == 0 {
		return fmt.Errorf("engine: activity %q has no candidates, cannot unclaim", activityInstanceID)
	}
	ai.Assignee = ""
	ai.ClaimTime = nil
	ai.State = instance.ActivityStateUnclaimed
	return e.store.UpdateActivityInstance(ctx, ai)
}

// GetCandidates returns the list of candidate user IDs for an activity.
func (e *ProcessEngine) GetCandidates(ctx context.Context, activityInstanceID string) []string {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return nil
	}
	raw, err := e.store.GetVariable(ctx, ai.ProcessInstanceID, "__candidates_"+ai.ID)
	if err != nil {
		return nil
	}
	switch v := raw.(type) {
	case []string:
		return v
	case []any:
		result := make([]string, 0, len(v))
		for _, c := range v {
			if s, ok := c.(string); ok {
				result = append(result, s)
			}
		}
		return result
	default:
		return nil
	}
}

func (e *ProcessEngine) ReceiveSignal(ctx context.Context, signalRef string, variables map[string]any) error {
	n := &navigator{store: e.store, identity: e.identity}
	if err := n.fireSignal(ctx, signalRef, variables); err != nil {
		return fmt.Errorf("engine: receive signal %q: %w", signalRef, err)
	}
	return nil
}

func (e *ProcessEngine) ReceiveMessage(ctx context.Context, messageRef string, variables map[string]any) error {
	return e.ReceiveSignal(ctx, messageRef, variables)
}

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
		TenantID:          ai.TenantID,
		ActivityID:        ai.ActivityID,
		ActivityType:      ai.ActivityType,
		Variables:         vars,
	}
	if ai.CompletedTime != nil {
		hai.CompletedAt = *ai.CompletedTime
	}
	_ = store.CreateHistoricActivityInstance(ctx, hai)
}

var (
	ErrProcessNotFound    = storage.ErrNotFound
	ErrActivityNotFound   = fmt.Errorf("activity not found")
	ErrTaskAlreadyDone    = fmt.Errorf("task already completed")
	ErrInstanceNotRunning = fmt.Errorf("process instance is not running")
)

var (
	idMu      sync.Mutex
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
