package engine

import (
	"context"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// handleBoundaryEventAttachments creates timer jobs and signal subscriptions
// for boundary events attached to the given activity element.
func (n *navigator) attachBoundaryEvents(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, activityID string) error {
	for _, el := range def.Elements {
		be, ok := el.(*spec.BoundaryEvent)
		if !ok || be.AttachedToRef != activityID {
			continue
		}

		if be.TimerDefinition != nil {
			dueAt := parseTimerDuration(be.TimerDefinition, time.Now())
			job := &instance.TimerJob{
				ID:                newID(),
				ProcessInstanceID: pi.ID,
				ElementID:         be.ID,
				DueAt:             dueAt,
				Fired:             false,
			}
			if err := n.store.CreateTimerJob(ctx, job); err != nil {
				return err
			}
		}

		if be.SignalDefinition != nil {
			sub := &instance.SignalSubscription{
				ID:                newID(),
				ProcessInstanceID: pi.ID,
				ElementID:         be.ID,
				SignalRef:         be.SignalDefinition.SignalRef,
			}
			if err := n.store.CreateSignalSubscription(ctx, sub); err != nil {
				return err
			}
		}
	}
	return nil
}

// handleIntermediateCatchEvent sets up a wait state for events.
func (n *navigator) handleIntermediateCatchEvent(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, ice *spec.IntermediateCatchEvent) error {
	tok := instance.NewToken(newID(), pi.ID, ice.ID)
	if err := n.store.CreateToken(ctx, tok); err != nil {
		return err
	}

	if ice.TimerDefinition != nil {
		dueAt := parseTimerDuration(ice.TimerDefinition, time.Now())
		job := &instance.TimerJob{
			ID:                newID(),
			ProcessInstanceID: pi.ID,
			ElementID:         ice.ID,
			DueAt:             dueAt,
			Fired:             false,
		}
		if err := n.store.CreateTimerJob(ctx, job); err != nil {
			return err
		}
	}

	if ice.SignalDefinition != nil {
		sub := &instance.SignalSubscription{
			ID:                newID(),
			ProcessInstanceID: pi.ID,
			ElementID:         ice.ID,
			SignalRef:         ice.SignalDefinition.SignalRef,
		}
		if err := n.store.CreateSignalSubscription(ctx, sub); err != nil {
			return err
		}
	}

	return nil // wait state
}

// handleIntermediateThrowEvent fires events and continues immediately.
func (n *navigator) handleIntermediateThrowEvent(ctx context.Context, def *spec.ProcessDefinition, pi *instance.ProcessInstance, ite *spec.IntermediateThrowEvent) error {
	// Create completed activity.
	ai := instance.NewActivityInstance(newID(), pi.ID, ite.ID, spec.ElementTypeIntermediateThrowEvent)
	ai.Complete()
	if err := n.store.CreateActivityInstance(ctx, ai); err != nil {
		return err
	}

	// Fire signal if defined.
	if ite.SignalDefinition != nil {
		vars, err := n.store.GetAllVariables(ctx, pi.ID)
		if err != nil {
			return err
		}
		if err := n.fireSignal(ctx, ite.SignalDefinition.SignalRef, vars); err != nil {
			return err
		}
	}

	// Continue navigation.
	outgoing := spec.FindOutgoingFlows(def.SequenceFlows, ite.ID)
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

// fireSignal looks up all subscriptions for the signal ref and triggers them.
func (n *navigator) fireSignal(ctx context.Context, signalRef string, variables map[string]any) error {
	subs, err := n.store.ListSignalSubscriptions(ctx, signalRef)
	if err != nil {
		return err
	}
	for _, sub := range subs {
		if err := n.triggerEventElement(ctx, sub.ProcessInstanceID, sub.ElementID); err != nil {
			return err
		}
		if err := n.store.DeleteSignalSubscription(ctx, sub.ID); err != nil {
			return err
		}
	}
	return nil
}

// triggerEventElement consumes the waiting token at an event element
// and navigates forward from it.
func (n *navigator) triggerEventElement(ctx context.Context, processInstanceID, elementID string) error {
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

	element, ok := def.Elements[elementID]
	if !ok {
		return nil
	}

	// Check if this is a BoundaryEvent — need to consume the attached activity's token and complete it.
	if be, ok := element.(*spec.BoundaryEvent); ok {
		// Interrupting boundary: consume token at the attached activity.
		if be.CancelActivity {
			tokens, err := n.store.ListActiveTokens(ctx, processInstanceID)
			if err != nil {
				return err
			}
			for _, tok := range tokens {
				if tok.CurrentElementID == be.AttachedToRef {
					tok.State = instance.TokenStateConsumed
					if err := n.store.UpdateToken(ctx, tok); err != nil {
						return err
					}
				}
			}
			// Also complete the activity instance attached to this boundary.
			activities, err := n.store.ListActiveActivities(ctx, processInstanceID)
			if err != nil {
				return err
			}
			for _, ai := range activities {
				if ai.ActivityID == be.AttachedToRef && ai.State == instance.ActivityStateActive {
					ai.Complete()
					if err := n.store.UpdateActivityInstance(ctx, ai); err != nil {
						return err
					}
				}
			}
		}
	}

	// Consume the waiting token at this event element.
	tokens, err := n.store.ListActiveTokens(ctx, processInstanceID)
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

	// Create completed activity for the event.
	ai := instance.NewActivityInstance(newID(), processInstanceID, elementID, element.GetType())
	ai.Complete()
	if err := n.store.CreateActivityInstance(ctx, ai); err != nil {
		return err
	}

	// Navigate forward from the event.
	return n.navigateFrom(ctx, processInstanceID, elementID)
}

// parseTimerDuration parses a timer definition and returns the due time.
// Supports simple duration expressions like "PT1H", "PT30M", or fixed dates.
// For v1, this is a simplified implementation.
func parseTimerDuration(td *spec.TimerEventDefinition, now time.Time) time.Time {
	if td.TimerDate != "" {
		t, err := time.Parse(time.RFC3339, td.TimerDate)
		if err == nil {
			return t
		}
		t2, err := time.Parse("2006-01-02T15:04:05", td.TimerDate)
		if err == nil {
			return t2
		}
	}

	if td.TimerDuration != "" {
		return parseISODuration(td.TimerDuration, now)
	}

	if td.TimerCycle != "" {
		return parseISODuration(td.TimerCycle, now)
	}

	return now.Add(24 * time.Hour) // default: 24h
}

// parseISODuration parses ISO 8601 duration strings (simplified).
// Supports PT{num}H, PT{num}M, PT{num}S, P{num}D patterns.
// The parser is case-sensitive: 'T' separates date from time.
func parseISODuration(duration string, from time.Time) time.Time {
	var totalMinutes int
	var currentVal int
	reading := false
	inTime := false
	for i := 0; i < len(duration); i++ {
		c := duration[i]
		if c >= '0' && c <= '9' {
			currentVal = currentVal*10 + int(c-'0')
			reading = true
		} else if c == 'T' {
			inTime = true
			reading = false
		} else if c == 'P' {
			reading = false
		} else if reading {
			switch c {
			case 'H':
				totalMinutes += currentVal * 60
			case 'M':
				if inTime {
					totalMinutes += currentVal
				} else {
					totalMinutes += currentVal * 24 * 60 // month approximation: ignore
				}
			case 'S':
				// seconds -> minute, rounding up if >=30s
				totalMinutes += (currentVal + 30) / 60
			case 'D':
				totalMinutes += currentVal * 24 * 60
			}
			currentVal = 0
			reading = false
		}
	}
	if totalMinutes > 0 {
		return from.Add(time.Duration(totalMinutes) * time.Minute)
	}
	return from.Add(24 * time.Hour)
}
