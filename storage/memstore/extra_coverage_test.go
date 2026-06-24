package memstore

import (
	"context"
	"testing"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
)

// TimerJobStore coverage
func TestStore_TimerJob(t *testing.T) {
	s := New()
	ctx := context.Background()

	// Create a PI first to satisfy any cross-table refs
	pi := instance.NewProcessInstance("pi-timer", "def1", nil)
	s.CreateProcessInstance(ctx, pi)

	now := time.Now()
	job := &instance.TimerJob{
		ID: "tj1", ProcessInstanceID: "pi-timer",
		ElementID: "elem1", DueAt: now.Add(time.Hour), Fired: false,
	}

	if err := s.CreateTimerJob(ctx, job); err != nil {
		t.Fatalf("CreateTimerJob: %v", err)
	}

	got, err := s.ListDueTimerJobs(ctx, now.Add(2*time.Hour))
	if err != nil { t.Fatalf("ListDueTimerJobs: %v", err) }
	if len(got) != 1 { t.Fatalf("expected 1 due job, got %d", len(got)) }

	job.Fired = true
	s.UpdateTimerJob(ctx, job)

	due, _ := s.ListDueTimerJobs(ctx, now.Add(2*time.Hour))
	if len(due) != 0 { t.Errorf("expected 0 due after fired, got %d", len(due)) }

	s.DeleteTimerJob(ctx, "tj1")
	s.DeleteTimerJobsByInstance(ctx, "pi-timer")
}

// SignalSubscriptionStore coverage
func TestStore_SignalSubscription(t *testing.T) {
	s := New()
	ctx := context.Background()

	sub := &instance.SignalSubscription{
		ID: "ss1", ProcessInstanceID: "pi-1",
		ElementID: "catch1", SignalRef: "mySignal",
	}

	if err := s.CreateSignalSubscription(ctx, sub); err != nil {
		t.Fatalf("CreateSignalSubscription: %v", err)
	}

	list, err := s.ListSignalSubscriptions(ctx, "mySignal")
	if err != nil { t.Fatalf("ListSignalSubscriptions: %v", err) }
	if len(list) != 1 { t.Fatalf("expected 1 sub, got %d", len(list)) }

	s.DeleteSignalSubscription(ctx, "ss1")
	list2, _ := s.ListSignalSubscriptions(ctx, "mySignal")
	if len(list2) != 0 { t.Errorf("expected 0 after delete, got %d", len(list2)) }

	// Test DeleteByInstance
	s.CreateSignalSubscription(ctx, &instance.SignalSubscription{ID: "ss2", ProcessInstanceID: "pi-2", SignalRef: "s"})
	s.DeleteSubscriptionsByInstance(ctx, "pi-2")
	list3, _ := s.ListSignalSubscriptions(ctx, "s")
	if len(list3) != 0 { t.Errorf("expected 0 after instance delete, got %d", len(list3)) }
}

// HistoricActivityInstanceStore coverage
func TestStore_HistoricActivityInstance(t *testing.T) {
	s := New()
	ctx := context.Background()

	hai := &instance.HistoricActivityInstance{
		ID: "hai1", ProcessInstanceID: "pi-1",
		ActivityID: "task1", ActivityType: spec.ElementTypeUserTask,
		Variables: map[string]any{"approved": true},
	}

	if err := s.CreateHistoricActivityInstance(ctx, hai); err != nil {
		t.Fatalf("CreateHistoricActivityInstance: %v", err)
	}

	list, err := s.ListHistoricByProcessInstance(ctx, "pi-1")
	if err != nil { t.Fatalf("ListHistoric: %v", err) }
	if len(list) != 1 { t.Fatalf("expected 1 historic, got %d", len(list)) }
	if list[0].Variables["approved"] != true { t.Errorf("variables lost") }
}

// GetLatestProcessDefinitionByKey coverage
func TestStore_GetLatestByKey(t *testing.T) {
	s := New()
	ctx := context.Background()

	def := &spec.ProcessDefinition{
		ID: "test:v1", Key: "test", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"}, "end": &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "end"}},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)

	got, err := s.GetLatestProcessDefinitionByKey(ctx, "test")
	if err != nil { t.Fatalf("GetLatest: %v", err) }
	if got.Version != 1 { t.Errorf("version=%d, want 1", got.Version) }

	// Non-existent key
	_, err = s.GetLatestProcessDefinitionByKey(ctx, "missing")
	if err == nil { t.Error("expected error for missing key") }
}

// ListCompletedProcessInstances coverage (limit)
func TestStore_ListCompleted(t *testing.T) {
	s := New()
	ctx := context.Background()

	pi1 := instance.NewProcessInstance("p1", "d", nil)
	pi2 := instance.NewProcessInstance("p2", "d", nil)
	s.CreateProcessInstance(ctx, pi1)
	s.CreateProcessInstance(ctx, pi2)
	now := time.Now()
	pi1.State = instance.ProcessInstanceStateCompleted; pi1.EndedAt = &now
	pi2.State = instance.ProcessInstanceStateCompleted; pi2.EndedAt = &now
	s.UpdateProcessInstance(ctx, pi1)
	s.UpdateProcessInstance(ctx, pi2)

	list, _ := s.ListCompletedProcessInstances(ctx, 0)
	if len(list) != 2 { t.Errorf("limit=0 should return all, got %d", len(list)) }

	list2, _ := s.ListCompletedProcessInstances(ctx, 1)
	if len(list2) != 1 { t.Errorf("limit=1 returned %d", len(list2)) }
}

// ListActivitiesByLoopID coverage
func TestStore_ListByLoopID(t *testing.T) {
	s := New()
	ctx := context.Background()
	pi := instance.NewProcessInstance("pi-loop", "d", nil)
	s.CreateProcessInstance(ctx, pi)

	ai1 := instance.NewActivityInstance("ai-l1", "pi-loop", "task", spec.ElementTypeUserTask)
	ai1.MultiInstanceLoopID = "loop1"
	ai2 := instance.NewActivityInstance("ai-l2", "pi-loop", "task", spec.ElementTypeUserTask)
	ai2.MultiInstanceLoopID = "loop1"
	s.CreateActivityInstance(ctx, ai1)
	s.CreateActivityInstance(ctx, ai2)

	list, _ := s.ListActivitiesByLoopID(ctx, "pi-loop", "loop1")
	if len(list) != 2 { t.Errorf("expected 2, got %d", len(list)) }
}

var _ storage.Store = (*Store)(nil)
