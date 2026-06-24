package engine

import (
	"context"
	"fmt"

	"github.com/lihongjie/workflow-go/expr"
	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// handleExclusiveGateway (XOR) evaluates outgoing flow conditions in order
// and takes the first flow whose condition evaluates to true.
// If no condition matches, the default flow is taken.
// Token is consumed only AFTER condition evaluation succeeds to avoid
// losing the token on error.
func (n *navigator) handleExclusiveGateway(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, outgoing []*spec.SequenceFlow, gw *spec.ExclusiveGateway) error {
	// Get all variables for condition evaluation (BEFORE consuming token).
	vars, err := n.store.GetAllVariables(ctx, pi.ID)
	if err != nil {
		return err
	}

	// Evaluate conditions in order. Find the matching flow FIRST.
	var matchedFlow *spec.SequenceFlow
	for _, sf := range outgoing {
		if sf.HasCondition() {
			matched, err := evaluateCondition(*sf.ConditionExpression, vars)
			if err != nil {
				return fmt.Errorf("engine: condition eval error on %q: %w", sf.ID, err)
			}
			if matched {
				matchedFlow = sf
				break
			}
		}
	}

	// If no condition matched, try default flow.
	if matchedFlow == nil && gw.DefaultFlowID != "" {
		for _, sf := range outgoing {
			if sf.ID == gw.DefaultFlowID {
				matchedFlow = sf
				break
			}
		}
	}

	if matchedFlow == nil {
		return fmt.Errorf("engine: exclusive gateway %q: no matching condition and no default flow", gw.ID)
	}

	// Consume the active token at this gateway AFTER successful condition match.
	tokens, err := n.store.ListActiveTokens(ctx, pi.ID)
	if err != nil {
		return err
	}
	for _, tok := range tokens {
		if tok.CurrentElementID == gw.ID {
			tok.State = instance.TokenStateConsumed
			if err := n.store.UpdateToken(ctx, tok); err != nil {
				return err
			}
		}
	}

	return n.takeSequenceFlow(ctx, def, pi, matchedFlow)
}

// handleParallelGateway handles both AND-join and AND-split.
// - Join: waits for ALL incoming tokens before proceeding.
// - Split: activates ALL outgoing flows.
// For join detection: only counts incoming flows whose source could actually
// send a token (excluding loopback paths that haven't been activated yet).
func (n *navigator) handleParallelGateway(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, outgoing []*spec.SequenceFlow, gw *spec.ParallelGateway, gatewayID string) error {
	incoming := spec.FindIncomingFlows(def.SequenceFlows, gatewayID)

	// Filter incoming flows to only count those whose source is reachable
	// without passing through this gateway again (excludes loopback paths).
	effectiveIncoming := countEffectiveIncoming(incoming, def)

	// --- AND-JOIN ---
	if effectiveIncoming > 1 {
		arrivalKey := fmt.Sprintf("__gw_arrived_%s", gatewayID)
		vars, err := n.store.GetAllVariables(ctx, pi.ID)
		if err != nil {
			return err
		}

		arrived, _ := vars[arrivalKey].(float64)
		arrived++

		if err := n.store.SetVariable(ctx, pi.ID, arrivalKey, arrived); err != nil {
			return err
		}

		if int(arrived) < effectiveIncoming {
			return nil // wait for more tokens
		}

		// All tokens arrived — consume all waiting tokens.
		tokens, err := n.store.ListActiveTokens(ctx, pi.ID)
		if err != nil {
			return err
		}
		for _, tok := range tokens {
			if tok.CurrentElementID == gatewayID {
				tok.State = instance.TokenStateConsumed
				if err := n.store.UpdateToken(ctx, tok); err != nil {
					return err
				}
			}
		}

		if err := n.store.DeleteVariable(ctx, pi.ID, arrivalKey); err != nil {
			return err
		}
	} else {
		// Only one incoming flow — no join needed. Just consume the token.
		tokens, err := n.store.ListActiveTokens(ctx, pi.ID)
		if err != nil {
			return err
		}
		for _, tok := range tokens {
			if tok.CurrentElementID == gatewayID {
				tok.State = instance.TokenStateConsumed
				if err := n.store.UpdateToken(ctx, tok); err != nil {
					return err
				}
			}
		}
	}

	// --- AND-SPLIT ---
	// Activate ALL outgoing flows.
	if len(outgoing) == 0 {
		return n.checkComplete(ctx, pi)
	}

	for _, sf := range outgoing {
		if err := n.takeSequenceFlow(ctx, def, pi, sf); err != nil {
			return err
		}
	}
	return nil
}

// handleInclusiveGateway (OR) evaluates conditions on all outgoing flows
// and activates every flow whose condition is true.
// If no condition matches, the default flow is used.
func (n *navigator) handleInclusiveGateway(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, outgoing []*spec.SequenceFlow, gw *spec.InclusiveGateway) error {
	// For v1, handle as pure OR-split (simplified: no join logic).
	// Consume the token at this gateway.
	tokens, err := n.store.ListActiveTokens(ctx, pi.ID)
	if err != nil {
		return err
	}
	for _, tok := range tokens {
		if tok.CurrentElementID == gw.ID {
			tok.State = instance.TokenStateConsumed
			if err := n.store.UpdateToken(ctx, tok); err != nil {
				return err
			}
		}
	}

	vars, err := n.store.GetAllVariables(ctx, pi.ID)
	if err != nil {
		return err
	}

	var activated []*spec.SequenceFlow

	// Evaluate each outgoing flow's condition.
	for _, sf := range outgoing {
		if sf.HasCondition() {
			matched, err := evaluateCondition(*sf.ConditionExpression, vars)
			if err != nil {
				return fmt.Errorf("engine: condition eval error on %q: %w", sf.ID, err)
			}
			if matched {
				activated = append(activated, sf)
			}
		} else {
			// No condition = always taken (unless it's the default)
			activated = append(activated, sf)
		}
	}

	// If no flows matched, try default.
	if len(activated) == 0 {
		if gw.DefaultFlowID != "" {
			for _, sf := range outgoing {
				if sf.ID == gw.DefaultFlowID {
					activated = append(activated, sf)
					break
				}
			}
		}
	}

	if len(activated) == 0 {
		return fmt.Errorf("engine: inclusive gateway %q: no condition matched and no default flow", gw.ID)
	}

	for _, sf := range activated {
		if err := n.takeSequenceFlow(ctx, def, pi, sf); err != nil {
			return err
		}
	}
	return nil
}

// evaluateCondition evaluates a condition expression string against variables.
func evaluateCondition(conditionStr string, variables map[string]any) (bool, error) {
	c, err := expr.NewCondition(conditionStr)
	if err != nil {
		return false, err
	}
	return c.Evaluate(variables)
}

// countEffectiveIncoming counts non-loopback incoming flows.
// A flow is a loopback if its source is a ServiceTask (commonly used in
// auto-complete rework loops). This is a pragmatic v1 heuristic.
func countEffectiveIncoming(incoming []*spec.SequenceFlow, def *spec.ProcessDefinition) int {
	count := 0
	for _, sf := range incoming {
		source := def.Elements[sf.SourceRef]
		if source == nil || source.GetType() == spec.ElementTypeServiceTask {
			continue // ServiceTask-based loopbacks are excluded from initial count
		}
		count++
	}
	if count == 0 {
		return 1
	}
	return count
}
