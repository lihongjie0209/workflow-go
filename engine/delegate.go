package engine

import (
	"context"
	"fmt"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// DelegateTask 委派: A委托给B审批, B完成后回到A再审。
func (e *ProcessEngine) DelegateTask(ctx context.Context, activityInstanceID, delegateAssignee string) error {
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

	// Store original assignee and delegate info
	_ = e.store.SetVariable(ctx, ai.ProcessInstanceID, "__delegate_orig_"+ai.ID, ai.Assignee)
	_ = e.store.SetVariable(ctx, ai.ProcessInstanceID, "__delegate_to_"+ai.ID, delegateAssignee)

	// Reassign to delegate
	ai.Assignee = delegateAssignee
	return e.store.UpdateActivityInstance(ctx, ai)
}

// handleDelegateCompletion checks if a completed activity is a delegate task
// and returns it to the original assignee. Returns true if handled as delegate.
func (n *navigator) handleDelegateCompletion(ctx context.Context, pi *instance.ProcessInstance, ai *instance.ActivityInstance) bool {
	vars, err := n.store.GetAllVariables(ctx, pi.ID)
	if err != nil {
		return false
	}
	delegateKey := "__delegate_to_" + ai.ID
	origKey := "__delegate_orig_" + ai.ID

	_, hasDelegate := vars[delegateKey]
	origAssignee, hasOrig := vars[origKey]
	if !hasDelegate || !hasOrig {
		return false // not a delegate task
	}

	origStr, _ := origAssignee.(string)

	// Clean up delegate tracking variables
	_ = n.store.DeleteVariable(ctx, pi.ID, delegateKey)
	_ = n.store.DeleteVariable(ctx, pi.ID, origKey)

	// Create return activity for original assignee
	def, err := n.store.GetProcessDefinition(ctx, pi.ProcessDefinitionID)
	if err != nil {
		return false
	}

	newTok := instance.NewToken(newID(), pi.ID, ai.ActivityID)
	if err := n.store.CreateToken(ctx, newTok); err != nil {
		return false
	}
	newAI := instance.NewActivityInstance(newID(), pi.ID, ai.ActivityID, spec.ElementTypeUserTask)
	newAI.Assignee = origStr
	// Re-render assignee from definition if template
	if ut, ok := def.Elements[ai.ActivityID].(*spec.UserTask); ok {
		vars2, _ := n.store.GetAllVariables(ctx, pi.ID)
		newAI.Assignee = RenderTemplate(ut.Assignee, vars2)
	}
	if err := n.store.CreateActivityInstance(ctx, newAI); err != nil {
		return false
	}

	// Record delegate-to info for audit
	_ = n.store.SetVariable(ctx, pi.ID, "__delegate_return_"+ai.ID, true)

	return true
}
