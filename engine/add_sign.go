package engine

import (
	"context"
	"fmt"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/storage"
)

func (n *navigator) handleSignCompletion(ctx context.Context, pi *instance.ProcessInstance, ai *instance.ActivityInstance, completedVariables map[string]any) error {
	if ai.AdhocParentID == "" { return nil }
	parentAI, err := n.store.GetActivityInstance(ctx, ai.AdhocParentID)
	if err != nil { return fmt.Errorf("engine: get parent activity: %w", err) }
	vars, err := n.store.GetAllVariables(ctx, pi.ID)
	if err != nil { return err }
	signID := findSignID(vars, parentAI.ID)
	if signID == "" { return nil }
	nrCompleted, _ := vars[signID+"_completed"].(float64)
	nrApproved, _ := vars[signID+"_approved"].(float64)
	nrCompleted++
	if completedVariables != nil {
		if approved, ok := completedVariables["approved"]; ok {
			if b, ok := approved.(bool); ok && b { nrApproved++ }
		}
	}
	_ = n.store.SetVariable(ctx, pi.ID, signID+"_completed", nrCompleted)
	_ = n.store.SetVariable(ctx, pi.ID, signID+"_approved", nrApproved)
	strategy, _ := vars[signID+"_strategy"].(string)
	total, _ := vars[signID+"_total"].(float64)
	complete := (strategy == string(StrategyOR) && nrApproved >= 1) || (strategy == string(StrategyAND) && nrCompleted >= total)
	if !complete { return nil }
	allActs, _ := n.store.ListActivitiesByProcessInstance(ctx, pi.ID)
	for _, a := range allActs {
		if a.AdhocParentID == parentAI.ID && a.State == instance.ActivityStateActive {
			a.Complete(); _ = n.store.UpdateActivityInstance(ctx, a)
		}
	}
	tokens, _ := n.store.ListActiveTokens(ctx, pi.ID)
	var excess []string
	for _, tok := range tokens {
		if tok.CurrentElementID == parentAI.ActivityID { excess = append(excess, tok.ID) }
	}
	for len(excess) > 1 { _ = n.store.DeleteToken(ctx, excess[0]); excess = excess[1:] }
	for k := range vars {
		if len(k) > len(signID) && k[:len(signID)] == signID { _ = n.store.DeleteVariable(ctx, pi.ID, k) }
	}
	if parentAI.State == instance.ActivityStateCompleted {
		tok := instance.NewToken(newID(), pi.ID, parentAI.ActivityID)
		if err := n.store.CreateToken(ctx, tok); err != nil { return err }
		return n.navigateFrom(ctx, pi.ID, parentAI.ActivityID)
	}
	return nil
}

func hasPendingSigns(ctx context.Context, s storage.Store, pi *instance.ProcessInstance, ai *instance.ActivityInstance) bool {
	acts, err := s.ListActiveActivities(ctx, pi.ID)
	if err != nil { return false }
	for _, a := range acts {
		if a.AdhocParentID == ai.ID { return true }
	}
	return false
}

func findSignID(vars map[string]any, parentID string) string {
	for k, v := range vars {
		if len(k) > 7 && k[len(k)-7:] == "_parent" {
			if pid, ok := v.(string); ok && pid == parentID { return k[:len(k)-7] }
		}
	}
	return ""
}
