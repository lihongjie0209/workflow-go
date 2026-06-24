package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

type RejectType string

const (
	RejectPrevious  RejectType = "previous"
	RejectInitiator RejectType = "initiator"
	RejectSpecific  RejectType = "specific"
	RejectTerminate RejectType = "terminate"
)

const defaultMaxRejection = 5

func (e *ProcessEngine) RejectTask(ctx context.Context, activityInstanceID string, rejectType RejectType, reason string, targetNodeID string) error {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get activity instance %q: %w", activityInstanceID, err)
	}
	if ai.State != instance.ActivityStateActive {
		return fmt.Errorf("engine: activity %q is not active", activityInstanceID)
	}
	if ai.ActivityType != spec.ElementTypeUserTask {
		return fmt.Errorf("engine: activity %q is not a userTask", activityInstanceID)
	}

	pi, err := e.store.GetProcessInstance(ctx, ai.ProcessInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get process instance: %w", err)
	}
	if pi.State != instance.ProcessInstanceStateRunning {
		return fmt.Errorf("engine: process instance %q is not running", pi.ID)
	}

	def, err := e.store.GetProcessDefinition(ctx, pi.ProcessDefinitionID)
	if err != nil {
		return fmt.Errorf("engine: get definition: %w", err)
	}

	var allVars map[string]any
	allVars, err = e.store.GetAllVariables(ctx, pi.ID)
	if err != nil {
		return err
	}
	rejectionCount := toFloat64(allVars["__rejectionCount"])
	maxRejection := defaultMaxRejection
	if v := toFloat64(allVars["__maxRejectionCount"]); v > 0 {
		maxRejection = int(v)
	}
	if int(rejectionCount) >= maxRejection {
		return fmt.Errorf("engine: rejection limit reached (%d/%d)", int(rejectionCount), maxRejection)
	}

	var targetElementID string
	switch rejectType {
	case RejectPrevious:
		targetElementID = findPreviousUserTask(def, ai.ActivityID, make(map[string]bool))
		if targetElementID == "" {
			return fmt.Errorf("engine: no previous UserTask found from %q", ai.ActivityID)
		}
	case RejectInitiator:
		targetElementID = findInitiatorTask(def)
		if targetElementID == "" {
			return fmt.Errorf("engine: no initiator task found")
		}
	case RejectSpecific:
		if targetNodeID == "" {
			return fmt.Errorf("engine: targetNodeID is required for RejectSpecific")
		}
		if _, ok := def.Elements[targetNodeID]; !ok {
			return fmt.Errorf("engine: target node %q not found in definition", targetNodeID)
		}
		targetElementID = targetNodeID
	case RejectTerminate:
		ai.Complete()
		e.store.UpdateActivityInstance(ctx, ai)
		tokens, _ := e.store.ListActiveTokens(ctx, pi.ID)
		for _, tok := range tokens {
			tok.State = instance.TokenStateConsumed
			e.store.UpdateToken(ctx, tok)
		}
		pi.State = instance.ProcessInstanceStateRejected
		now := time.Now()
		pi.EndedAt = &now
		e.store.UpdateProcessInstance(ctx, pi)
		e.store.SetVariable(ctx, pi.ID, "__rejectionReason", reason)
		return nil
	default:
		return fmt.Errorf("engine: unknown reject type %q", rejectType)
	}

	ai.Complete()
	e.store.UpdateActivityInstance(ctx, ai)
	tokens, _ := e.store.ListActiveTokens(ctx, pi.ID)
	for _, tok := range tokens {
		if tok.CurrentElementID == ai.ActivityID && tok.State == instance.TokenStateActive {
			tok.State = instance.TokenStateConsumed
			if err := e.store.UpdateToken(ctx, tok); err != nil {
				return err
			}
			break
		}
	}

	newTok := instance.NewToken(newID(), pi.ID, targetElementID)
	if err := e.store.CreateToken(ctx, newTok); err != nil {
		return err
	}
	newAI := instance.NewActivityInstance(newID(), pi.ID, targetElementID, spec.ElementTypeUserTask)
	if ut, ok := def.Elements[targetElementID].(*spec.UserTask); ok {
		av, _ := e.store.GetAllVariables(ctx, pi.ID)

		newAI.Assignee = RenderTemplate(ut.Assignee, av)
	}
	if err := e.store.CreateActivityInstance(ctx, newAI); err != nil {
		return err
	}

	newCount := rejectionCount + 1
	if err := e.store.SetVariable(ctx, pi.ID, "__rejectionCount", newCount); err != nil {
		return fmt.Errorf("engine: set rejection count: %w", err)
	}

	// Clean up delegate tracking vars (Bug08)
	for _, key := range []string{"__delegate_to_", "__delegate_orig_", "__delegate_return_"} {
		e.store.DeleteVariable(ctx, pi.ID, key+ai.ID)
	}
	if reason != "" {
		e.store.SetVariable(ctx, pi.ID, "__rejectionReason", reason)
	}
	if rejectType == RejectInitiator {
		e.store.SetVariable(ctx, pi.ID, "__rejectedToInitiator", true)
	}
	return nil
}

func findPreviousUserTask(def *spec.ProcessDefinition, currentID string, visited map[string]bool) string {
	if visited[currentID] {
		return ""
	}
	visited[currentID] = true
	incoming := spec.FindIncomingFlows(def.SequenceFlows, currentID)
	for _, sf := range incoming {
		sourceEl := def.Elements[sf.SourceRef]
		if sourceEl == nil {
			continue
		}
		switch sourceEl.GetType() {
		case spec.ElementTypeUserTask:
			return sf.SourceRef
		case spec.ElementTypeExclusiveGateway, spec.ElementTypeParallelGateway, spec.ElementTypeInclusiveGateway:
			if found := findPreviousUserTask(def, sf.SourceRef, visited); found != "" {
				return found
			}
		case spec.ElementTypeStartEvent:
			return sf.SourceRef
		case spec.ElementTypeServiceTask, spec.ElementTypeIntermediateCatchEvent, spec.ElementTypeIntermediateThrowEvent:
			if found := findPreviousUserTask(def, sf.SourceRef, visited); found != "" {
				return found
			}
		default:
			if found := findPreviousUserTask(def, sf.SourceRef, visited); found != "" {
				return found
			}
		}
	}
	return ""
}

func findInitiatorTask(def *spec.ProcessDefinition) string {
	if _, ok := def.Elements[def.StartEventID]; !ok {
		return ""
	}
	outgoing := spec.FindOutgoingFlows(def.SequenceFlows, def.StartEventID)
	if len(outgoing) == 0 {
		return ""
	}
	return findFirstUserTaskForward(def, outgoing[0].TargetRef, make(map[string]bool))
}

func findFirstUserTaskForward(def *spec.ProcessDefinition, currentID string, visited map[string]bool) string {
	if visited[currentID] {
		return ""
	}
	visited[currentID] = true
	el := def.Elements[currentID]
	if el == nil {
		return ""
	}
	if el.GetType() == spec.ElementTypeUserTask {
		return currentID
	}
	outgoing := spec.FindOutgoingFlows(def.SequenceFlows, currentID)
	for _, sf := range outgoing {
		if found := findFirstUserTaskForward(def, sf.TargetRef, visited); found != "" {
			return found
		}
	}
	return ""
}

func toFloat64(v any) float64 {
	if v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case int:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}
