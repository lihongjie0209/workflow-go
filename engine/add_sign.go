package engine

import (
	"context"
	"fmt"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
)

// handleSignCompletion is called when a sign activity instance is completed.
// It checks whether all sign conditions are met and advances the parent if so.
func (n *navigator) handleSignCompletion(ctx context.Context, pi *instance.ProcessInstance, ai *instance.ActivityInstance, completedVariables map[string]any) error {
	if ai.AdhocParentID == "" {
		return nil // not a sign activity
	}

	parentAI, err := n.store.GetActivityInstance(ctx, ai.AdhocParentID)
	if err != nil {
		return fmt.Errorf("engine: get parent activity: %w", err)
	}

	vars, err := n.store.GetAllVariables(ctx, pi.ID)
	if err != nil {
		return err
	}

	signID := findSignID(vars, parentAI.ID)
	if signID == "" {
		return fmt.Errorf("engine: sign session not found for parent %q", parentAI.ID)
	}

	// Update counters.
	nrCompleted, _ := vars[signID+"_completed"].(float64)
	nrApproved, _ := vars[signID+"_approved"].(float64)
	nrCompleted++

	// Check if this completion carried approval.
	if completedVariables != nil {
		if approved, ok := completedVariables["approved"]; ok {
			if b, ok := approved.(bool); ok && b {
				nrApproved++
			}
		}
	}

	_ = n.store.SetVariable(ctx, pi.ID, signID+"_completed", nrCompleted)
	_ = n.store.SetVariable(ctx, pi.ID, signID+"_approved", nrApproved)

	strategy, _ := vars[signID+"_strategy"].(string)
	total, _ := vars[signID+"_total"].(float64)

	// Decide if signing is complete.
	complete := false
	if strategy == string(StrategyOR) && nrApproved >= 1 {
		complete = true
	}
	if strategy == string(StrategyAND) && nrCompleted >= total {
		complete = true
	}

	if !complete {
		return nil // wait for more signers
	}

	// Signing complete: resume the parent activity.
	signType, _ := vars[signID+"_type"].(string)
	parentAI, err = n.store.GetActivityInstance(ctx, ai.AdhocParentID)
	if err != nil {
		return err
	}

	def, err := n.store.GetProcessDefinition(ctx, pi.ProcessDefinitionID)
	if err != nil {
		return err
	}

	switch SignType(signType) {
	case SignForward, SignParallel:
		// Parent has been waiting: complete it.
		parentAI.Complete()
		if err := n.store.UpdateActivityInstance(ctx, parentAI); err != nil {
			return err
		}
		// Consume ALL remaining tokens at this element (sign tokens + parent's).
		tokens, _ := n.store.ListActiveTokens(ctx, pi.ID)
		for _, tok := range tokens {
			if tok.CurrentElementID == parentAI.ActivityID {
				tok.State = instance.TokenStateConsumed
				_ = n.store.UpdateToken(ctx, tok)
			}
		}
		// Create ONE new token and navigate forward from it.
		newTok := instance.NewToken(newID(), pi.ID, parentAI.ActivityID)
		if err := n.store.CreateToken(ctx, newTok); err != nil {
			return err
		}
		// Now navigateFrom will consume this token and move forward.
		return n.navigateFrom(ctx, pi.ID, parentAI.ActivityID)

	case SignBackward:
		// Parent already completed. Navigate forward from it.
		outgoing := spec.FindOutgoingFlows(def.SequenceFlows, parentAI.ActivityID)
		if len(outgoing) == 0 {
			return n.checkComplete(ctx, pi)
		}
		for _, sf := range outgoing {
			tok := instance.NewToken(newID(), pi.ID, parentAI.ActivityID)
			if err := n.store.CreateToken(ctx, tok); err != nil {
				return err
			}
			if err := n.takeSequenceFlow(ctx, def, pi, sf); err != nil {
				return err
			}
		}
	}

	return nil
}

// hasPendingSigns checks if an activity has active sign activities that must complete.
func hasPendingSigns(ctx context.Context, s storage.Store, pi *instance.ProcessInstance, ai *instance.ActivityInstance) bool {
	acts, err := s.ListActiveActivities(ctx, pi.ID)
	if err != nil {
		return false
	}
	for _, a := range acts {
		if a.AdhocParentID == ai.ID {
			return true
		}
	}
	return false
}

// findSignID scans process variables for a sign session matching the given parent activity ID.
func findSignID(vars map[string]any, parentID string) string {
	for k, v := range vars {
		if len(k) > 7 && k[len(k)-7:] == "_parent" {
			if pid, ok := v.(string); ok && pid == parentID {
				return k[:len(k)-7]
			}
		}
	}
	return ""
}
