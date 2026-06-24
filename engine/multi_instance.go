package engine

import (
	"context"
	"fmt"

	"github.com/lihongjie/workflow-go/expr"
	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// handleMultiInstanceTask handles a UserTask with LoopCharacteristics (multi-instance/会签).
// For parallel mode: creates all instances at once.
// For sequential mode: creates one instance at a time.
func (n *navigator) handleMultiInstanceTask(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, ut *spec.UserTask) error {
	lc := ut.LoopCharacteristics
	if lc == nil {
		return fmt.Errorf("engine: handleMultiInstanceTask called without LoopCharacteristics")
	}

	loopID := newID()
	vars, err := n.store.GetAllVariables(ctx, pi.ID)
	if err != nil {
		return err
	}

	// Determine the number of instances.
	count, err := resolveLoopCount(lc, vars)
	if err != nil {
		return fmt.Errorf("engine: resolve loop count: %w", err)
	}

	if count <= 0 {
		// Empty collection or zero count — skip the task entirely.
		return n.handleOutgoingFlows(ctx, def, pi, spec.FindOutgoingFlows(def.SequenceFlows, ut.ID), nil)
	}

	// Resolve collection items for element variable assignment.
	items := resolveCollectionItems(lc, vars)

	// Set loop counter variables using well-known names so that
	// completion conditions can reference nrOfInstances directly.
	fCount := float64(count)
	// Store under both namespaced and global names.
	if err := n.store.SetVariable(ctx, pi.ID, loopID+"_nrOfInstances", fCount); err != nil {
		return err
	}
	if err := n.store.SetVariable(ctx, pi.ID, "nrOfInstances", fCount); err != nil {
		return err
	}
	if err := n.store.SetVariable(ctx, pi.ID, loopID+"_nrOfActiveInstances", fCount); err != nil {
		return err
	}
	if err := n.store.SetVariable(ctx, pi.ID, "nrOfActiveInstances", fCount); err != nil {
		return err
	}
	if err := n.store.SetVariable(ctx, pi.ID, loopID+"_nrOfCompletedInstances", float64(0)); err != nil {
		return err
	}
	if err := n.store.SetVariable(ctx, pi.ID, "nrOfCompletedInstances", float64(0)); err != nil {
		return err
	}

	if lc.IsSequential {
		return n.startSequentialInstance(ctx, def, pi, ut, loopID, items, 0, count)
	}
	return n.startParallelInstances(ctx, def, pi, ut, loopID, items, count)
}

// startParallelInstances creates all multi-instance tokens and activities at once.
func (n *navigator) startParallelInstances(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, ut *spec.UserTask, loopID string, items []any, count int) error {
	for i := 0; i < count; i++ {
		tok := instance.NewToken(newID(), pi.ID, ut.ID)
		if err := n.store.CreateToken(ctx, tok); err != nil {
			return err
		}

		ai := instance.NewActivityInstance(newID(), pi.ID, ut.ID, spec.ElementTypeUserTask)
		ai.MultiInstanceLoopID = loopID
		ai.LoopCounter = i
		// Resolve dynamic assignee for each instance if needed.
		if ut.Assignee != "" {
			ai.Assignee = RenderTemplate(ut.Assignee, pi.Variables)
		}
		if err := n.store.CreateActivityInstance(ctx, ai); err != nil {
			return err
		}

		// Set element variable if collection is provided.
		if ut.LoopCharacteristics.ElementVariable != "" && i < len(items) {
			if err := n.store.SetVariable(ctx, pi.ID, ut.LoopCharacteristics.ElementVariable, items[i]); err != nil {
				return err
			}
		}
	}
	return nil
}

// startSequentialInstance creates the first (or next) sequential multi-instance activity.
func (n *navigator) startSequentialInstance(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, ut *spec.UserTask, loopID string, items []any, index, count int) error {
	tok := instance.NewToken(newID(), pi.ID, ut.ID)
	if err := n.store.CreateToken(ctx, tok); err != nil {
		return err
	}

	ai := instance.NewActivityInstance(newID(), pi.ID, ut.ID, spec.ElementTypeUserTask)
	ai.MultiInstanceLoopID = loopID
	ai.LoopCounter = index
	// Resolve dynamic assignee.
	if ut.Assignee != "" {
		ai.Assignee = RenderTemplate(ut.Assignee, pi.Variables)
	}
	if err := n.store.CreateActivityInstance(ctx, ai); err != nil {
		return err
	}

	// Set element variable for this instance.
	if ut.LoopCharacteristics.ElementVariable != "" && index < len(items) {
		if err := n.store.SetVariable(ctx, pi.ID, ut.LoopCharacteristics.ElementVariable, items[index]); err != nil {
			return err
		}
	}

	// Store remaining count info as float64 for consistent type assertions.
	if err := n.store.SetVariable(ctx, pi.ID, loopID+"_sequential_index", float64(index)); err != nil {
		return err
	}
	if err := n.store.SetVariable(ctx, pi.ID, loopID+"_sequential_count", float64(count)); err != nil {
		return err
	}

	return nil
}

// advanceMultiInstance handles completion of a multi-instance activity.
// Returns true if the loop is complete (all done or completion condition met).
func (n *navigator) advanceMultiInstance(ctx context.Context, pi *instance.ProcessInstance, ai *instance.ActivityInstance) (bool, error) {
	loopID := ai.MultiInstanceLoopID
	if loopID == "" {
		return true, nil // not a multi-instance
	}

	vars, err := n.store.GetAllVariables(ctx, pi.ID)
	if err != nil {
		return false, err
	}

	// Update counters (both namespaced and global).
	nrCompleted, _ := vars[loopID+"_nrOfCompletedInstances"].(float64)
	nrActive, _ := vars[loopID+"_nrOfActiveInstances"].(float64)
	nrCompleted++
	nrActive--

	if err := n.store.SetVariable(ctx, pi.ID, loopID+"_nrOfCompletedInstances", nrCompleted); err != nil {
		return false, err
	}
	if err := n.store.SetVariable(ctx, pi.ID, "nrOfCompletedInstances", nrCompleted); err != nil {
		return false, err
	}
	if err := n.store.SetVariable(ctx, pi.ID, loopID+"_nrOfActiveInstances", nrActive); err != nil {
		return false, err
	}
	if err := n.store.SetVariable(ctx, pi.ID, "nrOfActiveInstances", nrActive); err != nil {
		return false, err
	}

	// Fetch definition once for both completion condition and sequential check.
	def, err := n.store.GetProcessDefinition(ctx, pi.ProcessDefinitionID)
	if err != nil {
		return false, err
	}
	ut, ok := def.Elements[ai.ActivityID].(*spec.UserTask)
	if !ok || ut.LoopCharacteristics == nil {
		return nrCompleted >= (vars[loopID+"_nrOfInstances"].(float64)), nil
	}

	// Check completion condition.
	if ut.LoopCharacteristics.CompletionCondition != "" {
		allVars, err := n.store.GetAllVariables(ctx, pi.ID)
		if err != nil {
			return false, err
		}
		matched, err := evaluateCondition(ut.LoopCharacteristics.CompletionCondition, allVars)
		if err == nil && matched {
			n.store.SetVariable(ctx, pi.ID, loopID+"_nrOfActiveInstances", 0)
			return true, nil
		}
	}

	// Check if this is a sequential multi-instance that should continue.
	if ut.LoopCharacteristics.IsSequential {
		seqIndex, _ := vars[loopID+"_sequential_index"].(float64)
		seqCount, _ := vars[loopID+"_sequential_count"].(float64)
		nextIndex := int(seqIndex) + 1

		if nextIndex < int(seqCount) {
			items := resolveCollectionItems(ut.LoopCharacteristics, vars)
			if err := n.startSequentialInstance(ctx, def, pi, ut, loopID, items, nextIndex, int(seqCount)); err != nil {
				return false, err
			}
			return false, nil // loop continues
		}
	}

	// All instances complete — check if loop is done.
	if nrActive <= 0 || int(nrCompleted) >= getTotalInstances(vars, loopID) {
		return true, nil
	}

	return false, nil
}

// resolveLoopCount determines the number of multi-instance iterations.
func resolveLoopCount(lc *spec.LoopCharacteristics, vars map[string]any) (int, error) {
	if lc.Collection != "" {
		items, ok := vars[lc.Collection]
		if !ok {
			return 0, nil
		}
		list, ok := items.([]any)
		if !ok {
			return 0, nil
		}
		return len(list), nil
	}

	if lc.LoopCardinality != "" {
		// Evaluate LoopCardinality as a numeric expression.
		c, err := expr.NewCondition(lc.LoopCardinality)
		if err != nil {
			return 1, fmt.Errorf("engine: invalid loopCardinality expression %q: %w", lc.LoopCardinality, err)
		}
		n, err := c.EvaluateNumeric(vars)
		if err != nil {
			return 1, fmt.Errorf("engine: loopCardinality evaluation error: %w", err)
		}
		if n < 0 {
			return 0, nil
		}
		return int(n), nil
	}

	return 1, nil
}

func resolveCollectionItems(lc *spec.LoopCharacteristics, vars map[string]any) []any {
	if lc.Collection == "" {
		return nil
	}
	items, ok := vars[lc.Collection]
	if !ok {
		return nil
	}
	list, ok := items.([]any)
	if !ok {
		return nil
	}
	return list
}

func getTotalInstances(vars map[string]any, loopID string) int {
	if v, ok := vars[loopID+"_nrOfInstances"].(float64); ok {
		return int(v)
	}
	return 0
}
