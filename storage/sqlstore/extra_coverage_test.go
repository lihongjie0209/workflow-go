package sqlstore

import (
	"context"
	"testing"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

func TestStore_TimerJob(t *testing.T) {
	s := New(WithMemory())
	ctx := context.Background()
	pi := instance.NewProcessInstance("pi-timer", "d", nil)
	s.CreateProcessInstance(ctx, pi)

	now := time.Now()
	job := &instance.TimerJob{ID: "tj1", ProcessInstanceID: "pi-timer", ElementID: "e", DueAt: now.Add(time.Hour)}
	s.CreateTimerJob(ctx, job)

	due, _ := s.ListDueTimerJobs(ctx, now.Add(2*time.Hour))
	if len(due) != 1 { t.Fatalf("expected 1 due, got %d", len(due)) }

	job.Fired = true; s.UpdateTimerJob(ctx, job)
	s.DeleteTimerJob(ctx, "tj1")
	s.DeleteTimerJobsByInstance(ctx, "pi-timer")
}

func TestStore_SignalSubscription(t *testing.T) {
	s := New(WithMemory())
	ctx := context.Background()
	sub := &instance.SignalSubscription{ID: "s1", ProcessInstanceID: "p", ElementID: "e", SignalRef: "sig"}
	s.CreateSignalSubscription(ctx, sub)
	list, _ := s.ListSignalSubscriptions(ctx, "sig")
	if len(list) != 1 { t.Fatalf("expected 1, got %d", len(list)) }
	s.DeleteSignalSubscription(ctx, "s1")
	s.CreateSignalSubscription(ctx, &instance.SignalSubscription{ID: "s2", ProcessInstanceID: "p", SignalRef: "s"})
	s.DeleteSubscriptionsByInstance(ctx, "p")
}

func TestStore_Historic(t *testing.T) {
	s := New(WithMemory())
	ctx := context.Background()
	hai := &instance.HistoricActivityInstance{
		ID: "h1", ProcessInstanceID: "p", ActivityID: "a",
		ActivityType: spec.ElementTypeUserTask, Variables: map[string]any{"k": "v"},
	}
	s.CreateHistoricActivityInstance(ctx, hai)
	list, _ := s.ListHistoricByProcessInstance(ctx, "p")
	if len(list) != 1 { t.Fatalf("expected 1, got %d", len(list)) }
	if list[0].Variables["k"] != "v" { t.Errorf("variables lost") }
}

func TestStore_GetLatestByKey(t *testing.T) {
	s := New(WithMemory())
	ctx := context.Background()
	def := &spec.ProcessDefinition{
		ID: "t:v1", Key: "t", Version: 1,
		Elements: map[string]spec.FlowElement{"start": &spec.StartEvent{ID: "start"}, "end": &spec.EndEvent{ID: "end"}},
		SequenceFlows: []*spec.SequenceFlow{{ID: "s1", SourceRef: "start", TargetRef: "end"}},
		StartEventID: "start",
	}
	s.CreateProcessDefinition(ctx, def)
	got, _ := s.GetLatestProcessDefinitionByKey(ctx, "t")
	if got.Version != 1 { t.Errorf("version=%d", got.Version) }
	_, err := s.GetLatestProcessDefinitionByKey(ctx, "x")
	if err == nil { t.Error("expected error") }
}

func TestStore_ListCompleted(t *testing.T) {
	s := New(WithMemory())
	ctx := context.Background()
	pi1 := instance.NewProcessInstance("p1", "d", nil)
	pi2 := instance.NewProcessInstance("p2", "d", nil)
	s.CreateProcessInstance(ctx, pi1); s.CreateProcessInstance(ctx, pi2)
	now := time.Now()
	pi1.State = instance.ProcessInstanceStateCompleted; pi1.EndedAt = &now; s.UpdateProcessInstance(ctx, pi1)
	pi2.State = instance.ProcessInstanceStateCompleted; pi2.EndedAt = &now; s.UpdateProcessInstance(ctx, pi2)
	list, _ := s.ListCompletedProcessInstances(ctx, 1)
	if len(list) != 1 { t.Errorf("limit=1 returned %d", len(list)) }
}

func TestStore_ListByLoopID(t *testing.T) {
	s := New(WithMemory())
	ctx := context.Background()
	pi := instance.NewProcessInstance("p", "d", nil)
	s.CreateProcessInstance(ctx, pi)
	ai1 := instance.NewActivityInstance("a1", "p", "t", spec.ElementTypeUserTask)
	ai1.MultiInstanceLoopID = "l1"
	ai2 := instance.NewActivityInstance("a2", "p", "t", spec.ElementTypeUserTask)
	ai2.MultiInstanceLoopID = "l1"
	s.CreateActivityInstance(ctx, ai1); s.CreateActivityInstance(ctx, ai2)
	list, _ := s.ListActivitiesByLoopID(ctx, "p", "l1")
	if len(list) != 2 { t.Errorf("expected 2, got %d", len(list)) }
}
