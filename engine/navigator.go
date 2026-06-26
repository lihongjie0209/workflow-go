package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/lihongjie/workflow-go/identity"
	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
)

// navigator handles the process execution flow, moving tokens
// from one element to the next based on the process topology.
type navigator struct {
	store    storage.Store
	identity identity.Service
}

// startFrom begins execution from the start event of a process definition.
func (n *navigator) startFrom(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance) error {
	startID := def.StartEventID

	// Create activity instance for StartEvent (completes instantly).
	ai := instance.NewActivityInstance(newID(), pi.ID, startID, spec.ElementTypeStartEvent)
	ai.TenantID = pi.TenantID
	ai.Complete()
	if err := n.store.CreateActivityInstance(ctx, ai); err != nil {
		return err
	}

	return n.navigateFrom(ctx, pi.ID, startID)
}

// navigateFrom takes the active token at sourceElementID and advances it
// through outgoing sequence flows, applying gateway logic as needed.
func (n *navigator) navigateFrom(ctx context.Context, processInstanceID, sourceElementID string) error {
	pi, err := n.store.GetProcessInstance(ctx, processInstanceID)
	if err != nil {
		return err
	}
	if pi.State != instance.ProcessInstanceStateRunning {
		return nil
	}

	def, err := n.store.GetProcessDefinition(ctx, pi.ProcessDefinitionID)
	if err != nil {
		return err
	}

	sourceElement, ok := def.Elements[sourceElementID]
	if !ok {
		return fmt.Errorf("engine: element %q not found in definition %q", sourceElementID, def.ID)
	}

	outgoing := spec.FindOutgoingFlows(def.SequenceFlows, sourceElementID)

	switch sourceElement.GetType() {
	case spec.ElementTypeStartEvent:
		return n.handleOutgoingFlows(ctx, def, pi, outgoing, nil)

	case spec.ElementTypeEndEvent:
		return n.handleEndEvent(ctx, pi, sourceElementID)

	case spec.ElementTypeUserTask:
		// Check for multi-instance loop advancement.
		ut, isMI := sourceElement.(*spec.UserTask)
		if isMI && ut.LoopCharacteristics != nil {
			// Consume the token for this completed instance.
			tokens, err := n.store.ListActiveTokens(ctx, pi.ID)
			if err != nil {
				return err
			}
			for _, tok := range tokens {
				if tok.CurrentElementID == sourceElementID {
					tok.State = instance.TokenStateConsumed
					if err := n.store.UpdateToken(ctx, tok); err != nil {
						return err
					}
					break
				}
			}
			// Find the completed MI activity instance.
			var completedMI *instance.ActivityInstance
			activities, err := n.store.ListActivitiesByProcessInstance(ctx, pi.ID)
			if err != nil {
				return err
			}
			for _, a := range activities {
				if a.ActivityID == sourceElementID && a.State == instance.ActivityStateCompleted && a.MultiInstanceLoopID != "" {
					completedMI = a
				}
			}
			if completedMI != nil {
				done, err := n.advanceMultiInstance(ctx, pi, completedMI)
				if err != nil {
					return err
				}
				if done {
					// Loop complete: consume ALL remaining tokens at this element
					// before proceeding to outgoing flows.
					remaining, _ := n.store.ListActiveTokens(ctx, pi.ID)
					for _, tok := range remaining {
						if tok.CurrentElementID == sourceElementID {
							tok.State = instance.TokenStateConsumed
							n.store.UpdateToken(ctx, tok)
						}
					}
					return n.handleOutgoingFlows(ctx, def, pi, outgoing, nil)
				}
				return nil // loop continues
			}
			return n.handleOutgoingFlows(ctx, def, pi, outgoing, nil)
		}
		return n.handleOutgoingFlows(ctx, def, pi, outgoing, nil)

	case spec.ElementTypeServiceTask:
		return n.handleOutgoingFlows(ctx, def, pi, outgoing, nil)

	case spec.ElementTypeExclusiveGateway:
		return n.handleExclusiveGateway(ctx, def, pi, outgoing, sourceElement.(*spec.ExclusiveGateway))

	case spec.ElementTypeParallelGateway:
		return n.handleParallelGateway(ctx, def, pi, outgoing, sourceElement.(*spec.ParallelGateway), sourceElementID)

	case spec.ElementTypeInclusiveGateway:
		return n.handleInclusiveGateway(ctx, def, pi, outgoing, sourceElement.(*spec.InclusiveGateway))

	case spec.ElementTypeIntermediateCatchEvent, spec.ElementTypeIntermediateThrowEvent, spec.ElementTypeBoundaryEvent:
		return n.handleOutgoingFlows(ctx, def, pi, outgoing, nil)

	case spec.ElementTypeCallActivity:
		return n.handleOutgoingFlows(ctx, def, pi, outgoing, nil)

	default:
		return fmt.Errorf("engine: unknown element type %q", sourceElement.GetType())
	}
}

// handleOutgoingFlows takes all outgoing sequence flows.
// It consumes one active token at the source element before proceeding.
func (n *navigator) handleOutgoingFlows(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, flows []*spec.SequenceFlow, _ map[string]any) error {
	if len(flows) == 0 {
		return n.checkComplete(ctx, pi)
	}

	// Consume one token at the source element's element ID.
	// The source element is the element that was just completed.
	// The flows tell us which element is the source (flows[0].SourceRef).
	sourceID := flows[0].SourceRef
	tokens, err := n.store.ListActiveTokens(ctx, pi.ID)
	if err != nil {
		return err
	}
	for _, tok := range tokens {
		if tok.CurrentElementID == sourceID {
			tok.State = instance.TokenStateConsumed
			if err := n.store.UpdateToken(ctx, tok); err != nil {
				return err
			}
			break
		}
	}

	for _, sf := range flows {
		if err := n.takeSequenceFlow(ctx, def, pi, sf); err != nil {
			return err
		}
	}
	return nil
}

// takeSequenceFlow moves execution along a single sequence flow.
// It places a token at the target element and handles it based on type.
func (n *navigator) takeSequenceFlow(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, sf *spec.SequenceFlow) error {
	targetElement, ok := def.Elements[sf.TargetRef]
	if !ok {
		return fmt.Errorf("engine: target element %q not found", sf.TargetRef)
	}

	switch targetElement.GetType() {
	case spec.ElementTypeUserTask:
		// Check if this is a multi-instance UserTask.
		if ut, ok := targetElement.(*spec.UserTask); ok && ut.LoopCharacteristics != nil {
			return n.handleMultiInstanceTask(ctx, def, pi, ut)
		}
		tok := instance.NewToken(newID(), pi.ID, targetElement.GetID())
	tok.TenantID = pi.TenantID
		if err := n.store.CreateToken(ctx, tok); err != nil {
			return err
		}
		ai := instance.NewActivityInstance(newID(), pi.ID, targetElement.GetID(), spec.ElementTypeUserTask)
	ai.TenantID = pi.TenantID
		if ut, ok := targetElement.(*spec.UserTask); ok {
			vars, _ := n.store.GetAllVariables(ctx, pi.ID)
			if ut.Assignee != "" {
				ai.Assignee = RenderTemplate(ut.Assignee, vars)
			} else if len(ut.CandidateUsers) > 0 || ut.CandidateGroup != "" {
				candidates := n.resolveCandidates(ctx, ut.CandidateUsers, ut.CandidateGroup)
				if len(candidates) > 0 {
					ai.State = instance.ActivityStateUnclaimed
					_ = n.store.SetVariable(ctx, pi.ID, "__candidates_"+ai.ID, candidates)
				}
			}
			}
		if err := n.store.CreateActivityInstance(ctx, ai); err != nil {
			return err
		}
		// Attach boundary events (timer/signal subscriptions) to this UserTask.
		if err := n.attachBoundaryEvents(ctx, def, pi, targetElement.GetID()); err != nil {
			return err
		}
		return nil // UserTask: wait for CompleteTask.

	case spec.ElementTypeServiceTask:
		tok := instance.NewToken(newID(), pi.ID, targetElement.GetID())
	tok.TenantID = pi.TenantID
		if err := n.store.CreateToken(ctx, tok); err != nil {
			return err
		}
		ai := instance.NewActivityInstance(newID(), pi.ID, targetElement.GetID(), spec.ElementTypeServiceTask)
	ai.TenantID = pi.TenantID
		ai.Complete()
		if err := n.store.CreateActivityInstance(ctx, ai); err != nil {
			return err
		}
		tok.State = instance.TokenStateConsumed
		if err := n.store.UpdateToken(ctx, tok); err != nil {
			return err
		}
		return n.navigateFrom(ctx, pi.ID, targetElement.GetID())

	case spec.ElementTypeEndEvent:
		ai := instance.NewActivityInstance(newID(), pi.ID, targetElement.GetID(), spec.ElementTypeEndEvent)
	ai.TenantID = pi.TenantID
		ai.Complete()
		if err := n.store.CreateActivityInstance(ctx, ai); err != nil {
			return err
		}
		return n.navigateFrom(ctx, pi.ID, targetElement.GetID())

	case spec.ElementTypeExclusiveGateway, spec.ElementTypeParallelGateway, spec.ElementTypeInclusiveGateway:
		tok := instance.NewToken(newID(), pi.ID, targetElement.GetID())
	tok.TenantID = pi.TenantID
		if err := n.store.CreateToken(ctx, tok); err != nil {
			return err
		}
		return n.navigateFrom(ctx, pi.ID, targetElement.GetID())

	case spec.ElementTypeIntermediateCatchEvent:
		ice := targetElement.(*spec.IntermediateCatchEvent)
		return n.handleIntermediateCatchEvent(ctx, def, pi, ice)

	case spec.ElementTypeIntermediateThrowEvent:
		ite := targetElement.(*spec.IntermediateThrowEvent)
		return n.handleIntermediateThrowEvent(ctx, def, pi, ite)

	case spec.ElementTypeBoundaryEvent:
		return fmt.Errorf("engine: boundary event %q cannot be a direct target", targetElement.GetID())

	case spec.ElementTypeCallActivity:
		ca := targetElement.(*spec.CallActivity)
		return n.handleCallActivity(ctx, def, pi, ca)

	default:
		return fmt.Errorf("engine: unhandled target element type %q", targetElement.GetType())
	}
}

// handleEndEvent processes an end event: check if process is complete.
// Tokens at the incoming element are already consumed by handleOutgoingFlows.
func (n *navigator) handleEndEvent(ctx context.Context, pi *instance.ProcessInstance, elementID string) error {
	// Only consume tokens specifically at this end event if any exist.
	tokens, err := n.store.ListActiveTokens(ctx, pi.ID)
	if err != nil {
		return err
	}
	for _, tok := range tokens {
		if tok.CurrentElementID == elementID {
			tok.State = instance.TokenStateConsumed
			if err := n.store.UpdateToken(ctx, tok); err != nil {
				return err
			}
		}
	}
	// Always check completion when reaching an end event.
	return n.checkComplete(ctx, pi)
}

// checkComplete checks if the process instance has no active tokens
// and marks it as completed if so.
func (n *navigator) checkComplete(ctx context.Context, pi *instance.ProcessInstance) error {
	active, err := n.store.ListActiveTokens(ctx, pi.ID)
	if err != nil {
		return err
	}
	if len(active) == 0 {
		pi.State = instance.ProcessInstanceStateCompleted
		now := time.Now()
		pi.EndedAt = &now
		if err := n.store.UpdateProcessInstance(ctx, pi); err != nil {
			return err
		}
		return n.checkParentCompletion(ctx, pi)
	}
	return nil
}

// resolveCandidates resolves candidate users from explicit list and group membership.
func (n *navigator) resolveCandidates(ctx context.Context, candidateUsers []string, candidateGroup string) []string {
	if n.identity == nil {
		return nil
	}
	result, err := n.identity.ResolveCandidateUsers(ctx, candidateUsers, candidateGroup)
	if err != nil {
		return nil
	}
	return result
}
