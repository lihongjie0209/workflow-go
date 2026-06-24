package engine

import (
	"context"
	"fmt"
	"time"

	"github.com/lihongjie/workflow-go/instance"
)

// CheckTimeouts scans all active process instances and auto-completes or
// auto-rejects activities that have passed their expire time.
// Should be called periodically (e.g., every 30s) by a background goroutine.
func (e *ProcessEngine) CheckTimeouts(ctx context.Context) (int, error) {
	count := 0

	// Get all process definitions to iterate instances
	defs, err := e.store.ListProcessDefinitions(ctx)
	if err != nil {
		return 0, err
	}

	for _, def := range defs {
		instances, err := e.store.ListProcessInstances(ctx, def.ID)
		if err != nil {
			continue
		}
		for _, pi := range instances {
			if pi.State != instance.ProcessInstanceStateRunning {
				continue
			}
			activities, err := e.store.ListActiveActivities(ctx, pi.ID)
			if err != nil {
				continue
			}
			for _, ai := range activities {
				if ai.ExpireTime == nil || ai.TermMode == 0 {
					continue
				}
				if time.Now().After(*ai.ExpireTime) {
					if ai.TermMode == 1 {
						// Auto complete
						if err := e.autoCompleteTask(ctx, ai); err != nil {
							return count, fmt.Errorf("auto-complete %q: %w", ai.ID, err)
						}
						count++
					} else if ai.TermMode == 2 {
						// Auto reject
						if err := e.autoRejectTask(ctx, ai); err != nil {
							return count, fmt.Errorf("auto-reject %q: %w", ai.ID, err)
						}
						count++
					}
				}
			}
		}
	}
	return count, nil
}

// autoCompleteTask automatically completes a timed-out activity.
func (e *ProcessEngine) autoCompleteTask(ctx context.Context, ai *instance.ActivityInstance) error {
	return e.CompleteTask(ctx, ai.ID, map[string]any{"approved": true, "__auto": true})
}

// autoRejectTask automatically rejects a timed-out activity.
func (e *ProcessEngine) autoRejectTask(ctx context.Context, ai *instance.ActivityInstance) error {
	return e.CompleteTask(ctx, ai.ID, map[string]any{"approved": false, "__auto": true})
}

// SetTimeout configures an activity with an expiration time and term mode.
// termMode: 1=auto-pass, 2=auto-reject
func (e *ProcessEngine) SetTimeout(ctx context.Context, activityInstanceID string, duration time.Duration, termMode int) error {
	ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
	if err != nil {
		return fmt.Errorf("engine: get activity %q: %w", activityInstanceID, err)
	}
	if ai.State != instance.ActivityStateActive {
		return fmt.Errorf("engine: activity %q is not active", activityInstanceID)
	}
	expire := time.Now().Add(duration)
	ai.ExpireTime = &expire
	ai.TermMode = termMode
	return e.store.UpdateActivityInstance(ctx, ai)
}
