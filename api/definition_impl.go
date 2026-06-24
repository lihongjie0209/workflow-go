package api

import (
	"context"

	"github.com/lihongjie/workflow-go/engine"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
)

type definitionServiceImpl struct {
	ds    *engine.DeploymentService
	store storage.Store
}

func NewDefinitionService(ds *engine.DeploymentService, store storage.Store) DefinitionService {
	return &definitionServiceImpl{ds: ds, store: store}
}

func (d *definitionServiceImpl) Deploy(ctx context.Context, def *spec.ProcessDefinition) (*spec.ProcessDefinition, error) {
	return d.ds.DeployProcessDefinition(ctx, def)
}

func (d *definitionServiceImpl) GetByID(ctx context.Context, id string) (*spec.ProcessDefinition, error) {
	return d.store.GetProcessDefinition(ctx, id)
}

func (d *definitionServiceImpl) GetByKey(ctx context.Context, key string) (*spec.ProcessDefinition, error) {
	return d.store.GetLatestProcessDefinitionByKey(ctx, key)
}

func (d *definitionServiceImpl) GetByKeyVersion(ctx context.Context, key string, version int) (*spec.ProcessDefinition, error) {
	return d.store.GetProcessDefinitionByKeyVersion(ctx, key, version)
}

func (d *definitionServiceImpl) List(ctx context.Context, filter DefinitionFilter, page PageRequest) (Page[*spec.ProcessDefinition], error) {
	results, total, err := d.store.QueryDefinitions(ctx, storage.DefQuery{
		Key:     filter.Key,
		Name:    filter.Name,
		Version: filter.Version,
		Offset:  page.Offset(),
		Limit:   page.Limit(),
	})
	if err != nil {
		return Page[*spec.ProcessDefinition]{}, err
	}
	return NewPage(results, total, page.Page, page.Limit()), nil
}

func (d *definitionServiceImpl) Delete(ctx context.Context, id string) error {
	return d.store.DeleteProcessDefinition(ctx, id)
}

var _ DefinitionService = (*definitionServiceImpl)(nil)
