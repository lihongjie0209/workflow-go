package api

import (
	"context"
	"testing"

	"github.com/lihongjie/workflow-go/engine"
	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage/memstore"
)

func TestWorkflowEngine_StartAndComplete(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })

	eng := engine.NewProcessEngine(s)
	wf := NewWorkflowEngine(eng)

	def := &spec.ProcessDefinition{
		ID: "test", Key: "test", Version: 1,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task", Name: "审批"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	if err := s.CreateProcessDefinition(ctx, def); err != nil {
		t.Fatal(err)
	}

	pi, err := wf.StartProcessInstance(ctx, "test", nil)
	if err != nil {
		t.Fatalf("StartProcessInstance: %v", err)
	}
	if pi.State != instance.ProcessInstanceStateRunning {
		t.Errorf("state=%q, want running", pi.State)
	}

	// Verify pending activities through query
	q := NewQueryService(s)
	pending, err := q.ListPendingActivities(ctx, ActivityFilter{ProcessInstanceID: pi.ID}, DefaultPageRequest())
	if err != nil {
		t.Fatalf("ListPendingActivities: %v", err)
	}
	if len(pending.Items) != 1 || pending.Items[0].ActivityID != "task" {
		t.Fatalf("expected 1 pending task, got %v", pending.Items)
	}
	if pending.Total != 1 || pending.TotalPages != 1 {
		t.Errorf("page info mismatch: total=%d pages=%d", pending.Total, pending.TotalPages)
	}

	// Complete it
	if err := wf.CompleteTask(ctx, pending.Items[0].ID, nil); err != nil {
		t.Fatalf("CompleteTask: %v", err)
	}

	pi2, _ := q.GetInstance(ctx, pi.ID)
	if pi2.State != instance.ProcessInstanceStateCompleted {
		t.Errorf("state=%q, want completed", pi2.State)
	}

	// Verify history
	hist, _ := q.ListHistoryByProcess(ctx, HistoryFilter{ProcessInstanceID: pi.ID}, DefaultPageRequest())
	if len(hist.Items) == 0 {
		t.Error("expected historic records")
	}
}

func TestWorkflowEngine_DeployAndQuery(t *testing.T) {
	ctx := context.Background()
	s := memstore.New()
	t.Cleanup(func() { s.Close() })

	eng := engine.NewProcessEngine(s)
	ds := engine.NewDeploymentService(s)

	defSvc := NewDefinitionService(ds, s)
	q := NewQueryService(s)

	// Deploy a definition
	def := &spec.ProcessDefinition{
		Key: "leave", Version: 0,
		Elements: map[string]spec.FlowElement{
			"start": &spec.StartEvent{ID: "start"},
			"task":  &spec.UserTask{ID: "task"},
			"end":   &spec.EndEvent{ID: "end"},
		},
		SequenceFlows: []*spec.SequenceFlow{
			{ID: "s1", SourceRef: "start", TargetRef: "task"},
			{ID: "s2", SourceRef: "task", TargetRef: "end"},
		},
		StartEventID: "start",
	}
	deployed, err := defSvc.Deploy(ctx, def)
	if err != nil {
		t.Fatalf("Deploy: %v", err)
	}
	if deployed.Version != 1 {
		t.Errorf("version=%d, want 1", deployed.Version)
	}

	// List definitions (with filter + page)
	list, err := defSvc.List(ctx, DefinitionFilter{}, DefaultPageRequest())
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(list.Items) != 1 {
		t.Errorf("expected 1 definition, got %d", len(list.Items))
	}
	if list.Total != 1 || list.PageSize != 20 {
		t.Errorf("page info: total=%d pageSize=%d", list.Total, list.PageSize)
	}

	// Get by key
	got, _ := defSvc.GetByKey(ctx, "leave")
	if got == nil {
		t.Fatal("GetByKey returned nil")
	}
	if got.Version != 1 {
		t.Errorf("version=%d, want 1", got.Version)
	}

	// Start an instance
	wf := NewWorkflowEngine(eng)
	wf.StartProcessInstance(ctx, deployed.ID, nil)

	counts, err := q.CountByState(ctx)
	if err != nil {
		t.Fatalf("CountByState: %v", err)
	}
	if counts[instance.ProcessInstanceStateRunning] != 1 {
		t.Errorf("expected 1 running, got %d", counts[instance.ProcessInstanceStateRunning])
	}
}

func TestPagination(t *testing.T) {
	p := DefaultPageRequest()
	if p.Page != 1 || p.PageSize != 20 {
		t.Errorf("default: page=%d size=%d", p.Page, p.PageSize)
	}
	if p.Offset() != 0 {
		t.Errorf("offset should be 0 for page 1")
	}
	_ = NewPage([]int{1, 2}, 10, 1, 5)
}
