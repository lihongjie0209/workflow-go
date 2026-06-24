package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
)

// DeploymentService manages process definition deployment and versioning.
type DeploymentService struct {
	store storage.Store
}

// NewDeploymentService creates a new deployment service.
func NewDeploymentService(store storage.Store) *DeploymentService {
	return &DeploymentService{store: store}
}

// DeployProcessDefinition deploys a process definition.
// If the definition has a Key that already exists, the Version is auto-incremented.
// If the definition's ID already exists, it returns an error.
func (ds *DeploymentService) DeployProcessDefinition(ctx context.Context, def *spec.ProcessDefinition) (*spec.ProcessDefinition, error) {
	if def.Key == "" {
		return nil, fmt.Errorf("deployment: process definition key is required")
	}

	// Auto-increment version if the key already exists.
	existing, err := ds.store.GetLatestProcessDefinitionByKey(ctx, def.Key)
	if err == nil && existing != nil {
		def.Version = existing.Version + 1
	} else if errors.Is(err, storage.ErrNotFound) {
		def.Version = 1
	} else if err != nil {
		return nil, fmt.Errorf("deployment: get latest definition by key: %w", err)
	} else {
		def.Version = 1
	}

	def.ID = fmt.Sprintf("%s:v%d", def.Key, def.Version)

	if err := ds.store.CreateProcessDefinition(ctx, def); err != nil {
		return nil, fmt.Errorf("deployment: create definition: %w", err)
	}

	return def, nil
}

// GetDeployedDefinition retrieves the latest deployed version of a process definition by key.
func (ds *DeploymentService) GetDeployedDefinition(ctx context.Context, key string) (*spec.ProcessDefinition, error) {
	return ds.store.GetLatestProcessDefinitionByKey(ctx, key)
}

// ListDeployedDefinitions lists all deployed process definitions.
func (ds *DeploymentService) ListDeployedDefinitions(ctx context.Context) ([]*spec.ProcessDefinition, error) {
	return ds.store.ListProcessDefinitions(ctx)
}
