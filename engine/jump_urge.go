package engine

import (
	"context"
	"fmt"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// JumpTask 跳转: 将流程实例跳转到指定的节点。
// 当前活动的 Task 被完成，目标节点创建新活动和新 Token。
func (e *ProcessEngine) JumpTask(ctx context.Context, activityInstanceID, targetNodeID string) error {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get activity %q: %w", activityInstanceID, err)
	}
	if ai.State != instance.ActivityStateActive {
		return fmt.Errorf("engine: activity %q is not active", activityInstanceID)
	}
	if ai.ActivityType != spec.ElementTypeUserTask {
		return fmt.Errorf("engine: activity %q is not a userTask", activityInstanceID)
	}
	if ai.AdhocParentID != "" {
		return fmt.Errorf("engine: cannot jump a sign activity")
	}

	pi, err := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
	if err != nil {
		return err
	}
	def, err := e.store.GetProcessDefinition(ctx, pi.ProcessDefinitionID)
	if err != nil {
		return err
	}
	if _, ok := def.Elements[targetNodeID]; !ok {
		return fmt.Errorf("engine: target node %q not found", targetNodeID)
	}

	// Complete current activity
	ai.Complete()
	if err := e.store.UpdateActivityInstance(ctx, ai); err != nil {
		return fmt.Errorf("engine: jump complete activity: %w", err)
	}

	// Consume its token
	tokens, err := e.store.ListActiveTokens(ctx, pi.ID)
	if err != nil {
		return fmt.Errorf("engine: list tokens: %w", err)
	}
	for _, tok := range tokens {
		if tok.CurrentElementID == ai.ActivityID && tok.State == instance.TokenStateActive {
			tok.State = instance.TokenStateConsumed
			if err := e.store.UpdateToken(ctx, tok); err != nil {
				return fmt.Errorf("engine: consume token: %w", err)
			}
			break
		}
	}

	// Clean up orphaned sign activities and sign session variables
	allActs, _ := e.store.ListActivitiesByProcessInstance(ctx, pi.ID)
	for _, a := range allActs {
		if a.AdhocParentID == ai.ID && a.State == instance.ActivityStateActive {
			a.Complete()
			e.store.UpdateActivityInstance(ctx, a)
		}
	}
	vars2, _ := e.store.GetAllVariables(ctx, pi.ID)
	for k := range vars2 {
		if len(k) > 7 && k[len(k)-7:] == "_parent" {
			if pid, ok := vars2[k].(string); ok && pid == ai.ID {
				prefix := k[:len(k)-7]
				for k2 := range vars2 {
					if len(k2) > len(prefix) && k2[:len(prefix)] == prefix && k2[len(prefix):] != "" && k2[len(prefix):][0] == '_' {
						e.store.DeleteVariable(ctx, pi.ID, k2)
					}
				}
			}
		}
	}

	// Clean up delegate tracking vars
	for _, key := range []string{"__delegate_to_", "__delegate_orig_", "__delegate_return_"} {
		e.store.DeleteVariable(ctx, pi.ID, key+ai.ID)
	}

	// Create new activity at target node
	targetEl := def.Elements[targetNodeID]
	if targetEl == nil {
		return fmt.Errorf("engine: target node %q not found in elements", targetNodeID)
	}

	newTok := instance.NewToken(newID(), pi.ID, targetNodeID)
	if err := e.store.CreateToken(ctx, newTok); err != nil {
		return fmt.Errorf("engine: create token for target: %w", err)
	}

	// If jumping to EndEvent, just let checkComplete handle it
	if targetEl.GetType() == spec.ElementTypeEndEvent {
		// Consume the token and check completion
		n := &navigator{store: e.store, identity: e.identity}
		return n.navigateFrom(ctx, pi.ID, targetNodeID)
	}

	// For UserTask targets, create a new activity
	if targetEl.GetType() == spec.ElementTypeUserTask {
		newAI := instance.NewActivityInstance(newID(), pi.ID, targetNodeID, spec.ElementTypeUserTask)
		if ut, ok := targetEl.(*spec.UserTask); ok {
			vars, _ := e.store.GetAllVariables(ctx, pi.ID)
			newAI.Assignee = RenderTemplate(ut.Assignee, vars)
		}
		if err := e.store.CreateActivityInstance(ctx, newAI); err != nil { return fmt.Errorf("engine: create activity: %w", err) }
	}

	_ = e.store.SetVariable(ctx, pi.ID, "__jump_from_"+ai.ID, targetNodeID)
	return nil
}

// UrgeTask 催办: 记录催办次数, 返回当前处理人和活动的信息用于发送通知。
// 调用者应在回调中处理实际的通知发送（邮件、短信、钉钉等）。
func (e *ProcessEngine) UrgeTask(ctx context.Context, activityInstanceID string) (assignee string, err error) {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return "", fmt.Errorf("engine: get activity %q: %w", activityInstanceID, err)
	}
	if ai.State != instance.ActivityStateActive {
		return "", fmt.Errorf("engine: activity %q is not active", activityInstanceID)
	}
	// Record urge count
	urgeKey := "__urge_count_" + ai.ProcessInstanceID
	pi, _ := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
	vars, _ := e.store.GetAllVariables(ctx, pi.ID)
	count, _ := vars[urgeKey].(float64)
	count++
	_ = e.store.SetVariable(ctx, pi.ID, urgeKey, count)
	return ai.Assignee, nil
}

// CcTask 抄送: 创建一条只读的抄送记录，不参与审批流程。
// 抄送记录写入历史表，不创建 Token，不影响流程流转。
func (e *ProcessEngine) CcTask(ctx context.Context, processInstanceID, ccUser string) error {
	pi, err := e.store.GetProcessInstance(ctx, processInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get instance %q: %w", processInstanceID, err)
	}
	// Create a historic activity instance as CC record
	hai := &instance.HistoricActivityInstance{
		ID:                newID(),
		ProcessInstanceID: pi.ID,
		ActivityID:        "cc_" + ccUser,
		ActivityType:      spec.ElementTypeUserTask,
		Variables:         map[string]any{"cc": true, "ccUser": ccUser},
	}
	_ = e.store.CreateHistoricActivityInstance(ctx, hai)
	return nil
}
