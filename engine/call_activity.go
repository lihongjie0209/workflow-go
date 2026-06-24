package engine

import (
	"context"
	"fmt"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// handleCallActivity manages the execution of a CallActivity element.
// It creates a child process instance and links it to the parent.
func (n *navigator) handleCallActivity(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, ca *spec.CallActivity) error {
	// Look up the called process definition by key.
	calledDef, err := n.store.GetLatestProcessDefinitionByKey(ctx, ca.CalledElement)
	if err != nil {
		return fmt.Errorf("engine: called definition %q not found: %w", ca.CalledElement, err)
	}

	// Create the child process instance.
	childVars := make(map[string]any)
	if ca.InheritVariables {
		vars, err := n.store.GetAllVariables(ctx, pi.ID)
		if err != nil {
			return err
		}
		for k, v := range vars {
			childVars[k] = v
		}
	}

	childPI := instance.NewProcessInstance(newID(), calledDef.ID, childVars)
	childPI.ParentProcessInstanceID = pi.ID
	childPI.ParentActivityID = ca.ID

	if err := n.store.CreateProcessInstance(ctx, childPI); err != nil {
		return err
	}
	for k, v := range childVars {
		if err := n.store.SetVariable(ctx, childPI.ID, k, v); err != nil {
			return err
		}
	}

	// Create parent activity instance and token for the CallActivity (wait state).
	ai := instance.NewActivityInstance(newID(), pi.ID, ca.ID, spec.ElementTypeCallActivity)
	if err := n.store.CreateActivityInstance(ctx, ai); err != nil {
		return err
	}

	// Create a waiting token in the parent at the CallActivity.
	tok := instance.NewToken(newID(), pi.ID, ca.ID)
	if err := n.store.CreateToken(ctx, tok); err != nil {
		return err
	}

	// Start the child process.
	if err := n.startFrom(ctx, calledDef, childPI); err != nil {
		return err
	}

	return nil // parent waits
}

// checkParentCompletion is called when a process instance completes.
// If the instance is a child of a CallActivity, it resumes the parent.
func (n *navigator) checkParentCompletion(ctx context.Context, pi *instance.ProcessInstance) error {
	if pi.ParentProcessInstanceID == "" || pi.ParentActivityID == "" {
		return nil // not a child process
	}

	// Find the parent's waiting token at the CallActivity.
	parentTokens, err := n.store.ListActiveTokens(ctx, pi.ParentProcessInstanceID)
	if err != nil {
		return err
	}

	var parentToken *instance.Token
	for _, tok := range parentTokens {
		if tok.CurrentElementID == pi.ParentActivityID {
			parentToken = tok
			break
		}
	}
	if parentToken == nil {
		return nil // parent already handled
	}

	// Consume the waiting token.
	parentToken.State = instance.TokenStateConsumed
	if err := n.store.UpdateToken(ctx, parentToken); err != nil {
		return err
	}

	// Complete the CallActivity activity instance.
	activities, err := n.store.ListActivitiesByProcessInstance(ctx, pi.ParentProcessInstanceID)
	if err != nil {
		return err
	}
	for _, ai := range activities {
		if ai.ActivityID == pi.ParentActivityID && ai.State == instance.ActivityStateActive {
			ai.Complete()
			if err := n.store.UpdateActivityInstance(ctx, ai); err != nil {
				return err
			}
		}
	}

	// Navigate forward from the CallActivity in the parent.
	return n.navigateFrom(ctx, pi.ParentProcessInstanceID, pi.ParentActivityID)
}
