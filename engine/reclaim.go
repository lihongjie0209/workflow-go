package engine

import (
	"context"
	"fmt"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// ReclaimTask 拿回: 在下一节点处理前, 上一节点拿回已提交的任务。
// currentActivityID: 当前活动 (下一节点, 尚未处理)
// returns: 上一个节点被重新激活
func (e *ProcessEngine) ReclaimTask(ctx context.Context, currentActivityID string) error {
	ai, err := e.store.GetActivityInstance(ctx, currentActivityID)
	if err != nil {
		return fmt.Errorf("engine: get activity %q: %w", currentActivityID, err)
	}
	if ai.State != instance.ActivityStateActive {
		return fmt.Errorf("engine: activity %q is not active", currentActivityID)
	}

	pi, err := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
	if err != nil {
		return err
	}

	def, err := e.store.GetProcessDefinition(ctx, pi.ProcessDefinitionID)
	if err != nil {
		return err
	}

	// Find the previous active/completed activity at this element
	allActs, _ := e.store.ListActivitiesByProcessInstance(ctx, pi.ID)

	// Find the most recently completed activity that is not at the current element
	var prevActivityID string
	for i := len(allActs) - 1; i >= 0; i-- {
		a := allActs[i]
		if a.State == instance.ActivityStateCompleted && a.ActivityID != ai.ActivityID {
			prevActivityID = a.ActivityID
			break
		}
	}
	if prevActivityID == "" {
		return fmt.Errorf("engine: no previous activity found to reclaim to")
	}

	// Complete the current activity
	ai.Complete()
	e.store.UpdateActivityInstance(ctx, ai)

	// Consume its token
	tokens, _ := e.store.ListActiveTokens(ctx, pi.ID)
	for _, tok := range tokens {
		if tok.CurrentElementID == ai.ActivityID && tok.State == instance.TokenStateActive {
			tok.State = instance.TokenStateConsumed
			e.store.UpdateToken(ctx, tok)
			break
		}
	}

	// Create new activity at the previous element
	newTok := instance.NewToken(newID(), pi.ID, prevActivityID)
	e.store.CreateToken(ctx, newTok)

	// Try to get assignee from the definition
	newAI := instance.NewActivityInstance(newID(), pi.ID, prevActivityID, ai.ActivityType)
	if ut, ok := def.Elements[prevActivityID].(*spec.UserTask); ok {
		vars, _ := e.store.GetAllVariables(ctx, pi.ID)
		newAI.Assignee = RenderTemplate(ut.Assignee, vars)
	}
	e.store.CreateActivityInstance(ctx, newAI)

	_ = e.store.SetVariable(ctx, pi.ID, "__reclaimed_"+ai.ID, prevActivityID)
	return nil
}
