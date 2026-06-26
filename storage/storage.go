// Package storage defines the persistence interfaces for the workflow engine.
// Implementations include in-memory (memstore) and SQLite (sqlstore).
//
// The Store interface is composed of five sub-interfaces:
//   - ProcessDefinitionStore
//   - ProcessInstanceStore
//   - ActivityInstanceStore
//   - TokenStore
//   - VariableStore
//
// Each implementation must satisfy all interfaces and pass the contract
// test suite in the storagetest package.
package storage

import (
	"context"
	"errors"
	"io"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// ErrNotFound is returned when a requested entity is not found.
var ErrNotFound = errors.New("entity not found")

// Store is the aggregate storage interface combining all sub-stores.
// Implementations must be safe for concurrent access.
type Store interface {
	ProcessDefinitionStore
	ProcessInstanceStore
	ActivityInstanceStore
	TokenStore
	VariableStore
	TimerJobStore
	SignalSubscriptionStore
	HistoricActivityInstanceStore
	QueryStore
	io.Closer
}

// ProcessDefinitionStore manages process definitions (the workflow blueprints).
type ProcessDefinitionStore interface {
	CreateProcessDefinition(ctx context.Context, def *spec.ProcessDefinition) error
	GetProcessDefinition(ctx context.Context, id string) (*spec.ProcessDefinition, error)
	GetProcessDefinitionByKeyVersion(ctx context.Context, key string, version int) (*spec.ProcessDefinition, error)
	GetLatestProcessDefinitionByKey(ctx context.Context, key string) (*spec.ProcessDefinition, error)
	ListProcessDefinitions(ctx context.Context) ([]*spec.ProcessDefinition, error)
	DeleteProcessDefinition(ctx context.Context, id string) error
}

// ProcessInstanceStore manages running and completed process instances.
type ProcessInstanceStore interface {
	CreateProcessInstance(ctx context.Context, pi *instance.ProcessInstance) error
	UpdateProcessInstance(ctx context.Context, pi *instance.ProcessInstance) error
	GetProcessInstance(ctx context.Context, id string) (*instance.ProcessInstance, error)
	ListProcessInstances(ctx context.Context, defID string) ([]*instance.ProcessInstance, error)
	ListCompletedProcessInstances(ctx context.Context, limit int) ([]*instance.ProcessInstance, error)
}

// ActivityInstanceStore manages activity execution records.
type ActivityInstanceStore interface {
	CreateActivityInstance(ctx context.Context, ai *instance.ActivityInstance) error
	UpdateActivityInstance(ctx context.Context, ai *instance.ActivityInstance) error
	GetActivityInstance(ctx context.Context, id string) (*instance.ActivityInstance, error)
	ListActiveActivities(ctx context.Context, processInstanceID string) ([]*instance.ActivityInstance, error)
	ListActivitiesByProcessInstance(ctx context.Context, processInstanceID string) ([]*instance.ActivityInstance, error)
	ListActivitiesByLoopID(ctx context.Context, processInstanceID, loopID string) ([]*instance.ActivityInstance, error)
}

// DefQuery 流程定义查询参数
type DefQuery struct {
	Key      string // 精确匹配
	Name     string // 模糊匹配(包含)
	Version  int    // 精确匹配
	TenantID string
	Offset   int
	Limit    int
}

// InstQuery 流程实例查询参数
type InstQuery struct {
	DefID      string
	State      string            // running/completed/terminated/rejected
	DefKey     string
	Assignee   string
	Initiator  string
	BusinessKey string
	TenantID   string
	StartAfter  *time.Time
	StartBefore *time.Time
	Offset     int
	Limit      int
}

// ActQuery 活动查询参数
type ActQuery struct {
	ProcessInstanceID string
	Assignee          string
	ActivityID        string
	ActivityType      string
	State             string // active/completed
	IsSign            *bool
	TenantID          string
	Offset            int
	Limit             int
}

// HistQuery 历史活动查询参数
type HistQuery struct {
	ProcessInstanceID string
	ActivityID        string
	Assignee          string
	TenantID          string
	CompletedAfter    *time.Time
	CompletedBefore   *time.Time
	Offset            int
	Limit             int
}

// QueryStore provides filtered + paginated read operations.
// Each method returns (results, totalCount, error).
type QueryStore interface {
	QueryDefinitions(ctx context.Context, q DefQuery) ([]*spec.ProcessDefinition, int, error)
	QueryProcessInstances(ctx context.Context, q InstQuery) ([]*instance.ProcessInstance, int, error)
	QueryActivities(ctx context.Context, q ActQuery) ([]*instance.ActivityInstance, int, error)
	QueryHistoricActivities(ctx context.Context, q HistQuery) ([]*instance.HistoricActivityInstance, int, error)
}

// HistoricActivityInstanceStore manages historical (completed) activity records.
type TimerJobStore interface {
	CreateTimerJob(ctx context.Context, job *instance.TimerJob) error
	UpdateTimerJob(ctx context.Context, job *instance.TimerJob) error
	ListDueTimerJobs(ctx context.Context, before time.Time) ([]*instance.TimerJob, error)
	DeleteTimerJob(ctx context.Context, id string) error
	DeleteTimerJobsByInstance(ctx context.Context, processInstanceID string) error
}

type SignalSubscriptionStore interface {
	CreateSignalSubscription(ctx context.Context, sub *instance.SignalSubscription) error
	ListSignalSubscriptions(ctx context.Context, signalRef string) ([]*instance.SignalSubscription, error)
	DeleteSignalSubscription(ctx context.Context, id string) error
	DeleteSubscriptionsByInstance(ctx context.Context, processInstanceID string) error
}

type HistoricActivityInstanceStore interface {
	CreateHistoricActivityInstance(ctx context.Context, hai *instance.HistoricActivityInstance) error
	ListHistoricByProcessInstance(ctx context.Context, processInstanceID string) ([]*instance.HistoricActivityInstance, error)
}

// TokenStore manages execution tokens (active paths through the process).
type TokenStore interface {
	CreateToken(ctx context.Context, t *instance.Token) error
	UpdateToken(ctx context.Context, t *instance.Token) error
	GetToken(ctx context.Context, id string) (*instance.Token, error)
	ListActiveTokens(ctx context.Context, processInstanceID string) ([]*instance.Token, error)
	DeleteToken(ctx context.Context, id string) error
}

// VariableStore manages process-scoped runtime variables.
type VariableStore interface {
	SetVariable(ctx context.Context, processInstanceID, name string, value any) error
	GetVariable(ctx context.Context, processInstanceID, name string) (any, error)
	GetAllVariables(ctx context.Context, processInstanceID string) (map[string]any, error)
	DeleteVariable(ctx context.Context, processInstanceID, name string) error
}

// Ensure Store includes io.Closer.
var _ io.Closer = (Store)(nil)
