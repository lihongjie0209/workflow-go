package engine

import (
	"context"
	"fmt"
	"time"

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
	if ai.AdhocParentID != "" {
		return fmt.Errorf("engine: cannot reclaim a sign activity")
	}

	pi, err := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
	if err != nil {
		return err
	}

	def, err := e.store.GetProcessDefinition(ctx, pi.ProcessDefinitionID)
	if err != nil {
		return err
	}

	// Find the previous completed UserTask (not StartEvent or gates)
	allActs, _ := e.store.ListActivitiesByProcessInstance(ctx, pi.ID)

	var prevActivityID string
	var latestCompleted time.Time
	for _, a := range allActs {
		if a.State == instance.ActivityStateCompleted && a.CompletedTime != nil &&
			a.ActivityType == spec.ElementTypeUserTask && a.ActivityID != ai.ActivityID {
			if a.CompletedTime.After(latestCompleted) {
				latestCompleted = *a.CompletedTime
				prevActivityID = a.ActivityID
			}
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

	// Clean up sign activities that were associated with the reclaimed element
	allActs2, _ := e.store.ListActivitiesByProcessInstance(ctx, pi.ID)
	for _, a := range allActs2 {
		if a.AdhocParentID == ai.ID && a.State == instance.ActivityStateActive {
			a.Complete()
			e.store.UpdateActivityInstance(ctx, a)
		}
	}

	// Clean up sign session variables
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

	_ = e.store.SetVariable(ctx, pi.ID, "__reclaimed_"+ai.ID, prevActivityID)
	return nil
}
