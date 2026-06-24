package api

import (
	"context"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/storage"
)

type queryServiceImpl struct {
	store storage.Store
}

func NewQueryService(store storage.Store) QueryService {
	return &queryServiceImpl{store: store}
}

func (q *queryServiceImpl) GetInstance(ctx context.Context, processInstanceID string) (*instance.ProcessInstance, error) {
	return q.store.GetProcessInstance(ctx, processInstanceID)
}

func (q *queryServiceImpl) ListInstances(ctx context.Context, filter ProcessInstanceFilter, page PageRequest) (Page[*instance.ProcessInstance], error) {
	results, total, err := q.store.QueryProcessInstances(ctx, storage.InstQuery{
		DefID:       filter.DefID,
		State:       filter.State,
		DefKey:      filter.DefKey,
		Initiator:   filter.Initiator,
		StartAfter:  filter.StartAfter,
		StartBefore: filter.StartBefore,
		Offset:      page.Offset(),
		Limit:       page.Limit(),
	})
	if err != nil {
		return Page[*instance.ProcessInstance]{}, err
	}
	return NewPage(results, total, page.Page, page.Limit()), nil
}

func (q *queryServiceImpl) ListCompletedInstances(ctx context.Context, filter ProcessInstanceFilter, page PageRequest) (Page[*instance.ProcessInstance], error) {
	results, total, err := q.store.QueryProcessInstances(ctx, storage.InstQuery{
		State:       string(instance.ProcessInstanceStateCompleted),
		DefKey:      filter.DefKey,
		StartAfter:  filter.StartAfter,
		StartBefore: filter.StartBefore,
		Offset:      page.Offset(),
		Limit:       page.Limit(),
	})
	if err != nil {
		return Page[*instance.ProcessInstance]{}, err
	}
	return NewPage(results, total, page.Page, page.Limit()), nil
}

func (q *queryServiceImpl) ListPendingActivities(ctx context.Context, filter ActivityFilter, page PageRequest) (Page[*instance.ActivityInstance], error) {
	if filter.ProcessInstanceID == "" {
		return Page[*instance.ActivityInstance]{}, nil
	}
	results, total, err := q.store.QueryActivities(ctx, storage.ActQuery{
		ProcessInstanceID: filter.ProcessInstanceID,
		Assignee:          filter.Assignee,
		ActivityID:        filter.ActivityID,
		ActivityType:      filter.ActivityType,
		State:             string(instance.ActivityStateActive),
		IsSign:            filter.IsSign,
		Offset:            page.Offset(),
		Limit:             page.Limit(),
	})
	if err != nil {
		return Page[*instance.ActivityInstance]{}, err
	}
	return NewPage(results, total, page.Page, page.Limit()), nil
}

func (q *queryServiceImpl) ListMyPendingActivities(ctx context.Context, assignee string, page PageRequest) (Page[*instance.ActivityInstance], error) {
	results, total, err := q.store.QueryActivities(ctx, storage.ActQuery{
		Assignee: assignee,
		State:    string(instance.ActivityStateActive),
		Offset:   page.Offset(),
		Limit:    page.Limit(),
	})
	if err != nil {
		return Page[*instance.ActivityInstance]{}, err
	}
	return NewPage(results, total, page.Page, page.Limit()), nil
}

func (q *queryServiceImpl) ListActivitiesByProcess(ctx context.Context, processInstanceID string, filter ActivityFilter, page PageRequest) (Page[*instance.ActivityInstance], error) {
	results, total, err := q.store.QueryActivities(ctx, storage.ActQuery{
		ProcessInstanceID: processInstanceID,
		Assignee:          filter.Assignee,
		ActivityID:        filter.ActivityID,
		ActivityType:      filter.ActivityType,
		State:             filter.State,
		IsSign:            filter.IsSign,
		Offset:            page.Offset(),
		Limit:             page.Limit(),
	})
	if err != nil {
		return Page[*instance.ActivityInstance]{}, err
	}
	return NewPage(results, total, page.Page, page.Limit()), nil
}

func (q *queryServiceImpl) ListHistoryByProcess(ctx context.Context, filter HistoryFilter, page PageRequest) (Page[*instance.HistoricActivityInstance], error) {
	if filter.ProcessInstanceID == "" {
		return Page[*instance.HistoricActivityInstance]{}, nil
	}
	results, total, err := q.store.QueryHistoricActivities(ctx, storage.HistQuery{
		ProcessInstanceID: filter.ProcessInstanceID,
		ActivityID:        filter.ActivityID,
		Assignee:          filter.Assignee,
		CompletedAfter:    filter.CompletedAfter,
		CompletedBefore:   filter.CompletedBefore,
		Offset:            page.Offset(),
		Limit:             page.Limit(),
	})
	if err != nil {
		return Page[*instance.HistoricActivityInstance]{}, err
	}
	return NewPage(results, total, page.Page, page.Limit()), nil
}

func (q *queryServiceImpl) GetVariables(ctx context.Context, processInstanceID string) (map[string]any, error) {
	return q.store.GetAllVariables(ctx, processInstanceID)
}

func (q *queryServiceImpl) CountByState(ctx context.Context) (map[instance.ProcessInstanceState]int, error) {
	result := map[instance.ProcessInstanceState]int{
		instance.ProcessInstanceStateRunning:    0,
		instance.ProcessInstanceStateSuspended:  0,
		instance.ProcessInstanceStateCompleted:  0,
		instance.ProcessInstanceStateTerminated: 0,
		instance.ProcessInstanceStateRejected:   0,
	}
	defs, err := q.store.ListProcessDefinitions(ctx)
	if err != nil {
		return nil, err
	}
	for _, def := range defs {
		instances, err := q.store.ListProcessInstances(ctx, def.ID)
		if err != nil {
			continue
		}
		for _, pi := range instances {
			result[pi.State]++
		}
	}
	return result, nil
}

var _ QueryService = (*queryServiceImpl)(nil)
