package api

import (
	"context"
	"time"

	"github.com/lihongjie/workflow-go/engine"
	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/storage"
)

// workflowEngineImpl implements WorkflowEngine by delegating to engine.ProcessEngine.
type workflowEngineImpl struct {
	eng        *engine.ProcessEngine
	store      storage.Store
}

// NewWorkflowEngine creates a WorkflowEngine wrapping an engine.ProcessEngine.
func NewWorkflowEngine(eng *engine.ProcessEngine) WorkflowEngine {
	return &workflowEngineImpl{eng: eng, store: eng.GetStore()}
}

func (w *workflowEngineImpl) StartProcessInstance(ctx context.Context, defID string, variables map[string]any) (*instance.ProcessInstance, error) {
	return w.eng.StartProcessInstance(ctx, defID, variables)
}

func (w *workflowEngineImpl) StartProcessInstanceByKey(ctx context.Context, key, businessKey string, variables map[string]any) (*instance.ProcessInstance, error) {
	return w.eng.StartProcessInstanceByKey(ctx, key, businessKey, variables)
}

func (w *workflowEngineImpl) SuspendProcessInstance(ctx context.Context, processInstanceID string) error {
	return w.eng.SuspendProcessInstance(ctx, processInstanceID)
}

func (w *workflowEngineImpl) ResumeProcessInstance(ctx context.Context, processInstanceID string) error {
	return w.eng.ResumeProcessInstance(ctx, processInstanceID)
}

func (w *workflowEngineImpl) TerminateProcessInstance(ctx context.Context, processInstanceID string) error {
	return w.eng.TerminateProcessInstance(ctx, processInstanceID)
}

func (w *workflowEngineImpl) CompleteTask(ctx context.Context, activityInstanceID string, variables map[string]any) error {
	return w.eng.CompleteTask(ctx, activityInstanceID, variables)
}

func (w *workflowEngineImpl) ClaimTask(ctx context.Context, activityInstanceID, userID string) error {
	return w.eng.ClaimTask(ctx, activityInstanceID, userID)
}

func (w *workflowEngineImpl) UnclaimTask(ctx context.Context, activityInstanceID string) error {
	return w.eng.UnclaimTask(ctx, activityInstanceID)
}

func (w *workflowEngineImpl) TransferTask(ctx context.Context, activityInstanceID, newAssignee string) error {
	return w.eng.TransferTask(ctx, activityInstanceID, newAssignee)
}

func (w *workflowEngineImpl) DelegateTask(ctx context.Context, activityInstanceID, delegateAssignee string) error {
	return w.eng.DelegateTask(ctx, activityInstanceID, delegateAssignee)
}

func (w *workflowEngineImpl) ReclaimTask(ctx context.Context, currentActivityID string) error {
	return w.eng.ReclaimTask(ctx, currentActivityID)
}

func (w *workflowEngineImpl) RejectTask(ctx context.Context, activityInstanceID string, rejectType RejectType, reason string, targetNodeID string) error {
	// Map API RejectType to engine RejectType
	rt := engine.RejectType(rejectType)
	return w.eng.RejectTask(ctx, activityInstanceID, rt, reason, targetNodeID)
}

func (w *workflowEngineImpl) JumpTask(ctx context.Context, activityInstanceID, targetNodeID string) error {
	return w.eng.JumpTask(ctx, activityInstanceID, targetNodeID)
}

func (w *workflowEngineImpl) UrgeTask(ctx context.Context, activityInstanceID string) (string, error) {
	return w.eng.UrgeTask(ctx, activityInstanceID)
}

func (w *workflowEngineImpl) CcTask(ctx context.Context, processInstanceID, ccUser string) error {
	return w.eng.CcTask(ctx, processInstanceID, ccUser)
}

func (w *workflowEngineImpl) AddSign(ctx context.Context, activityInstanceID string, signType SignType, strategy SignStrategy, assignees []string) error {
	st := engine.SignType(signType)
	ss := engine.SignStrategy(strategy)
	return w.eng.AddSign(ctx, activityInstanceID, st, ss, assignees)
}

func (w *workflowEngineImpl) RemoveSign(ctx context.Context, activityInstanceID, assignee string) error {
	return w.eng.RemoveSign(ctx, activityInstanceID, assignee)
}

func (w *workflowEngineImpl) SetTimeout(ctx context.Context, activityInstanceID string, duration time.Duration, termMode int) error {
	return w.eng.SetTimeout(ctx, activityInstanceID, duration, termMode)
}

func (w *workflowEngineImpl) CheckTimeouts(ctx context.Context) (int, error) {
	return w.eng.CheckTimeouts(ctx)
}

func (w *workflowEngineImpl) ReceiveSignal(ctx context.Context, signalRef string, variables map[string]any) error {
	return w.eng.ReceiveSignal(ctx, signalRef, variables)
}

func (w *workflowEngineImpl) ReceiveMessage(ctx context.Context, messageRef string, variables map[string]any) error {
	return w.eng.ReceiveMessage(ctx, messageRef, variables)
}

// Ensure compile-time check.
var _ WorkflowEngine = (*workflowEngineImpl)(nil)
