// Package api provides the public-facing interfaces for the workflow engine.
// External modules should depend on these interfaces, not on the internal
// engine package directly.
package api

import (
	"context"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
)

// ============================================================
// 分页与通用类型
// ============================================================

// PageRequest 分页请求参数
type PageRequest struct {
	Page     int    `json:"page"`     // 页码, 从 1 开始
	PageSize int    `json:"pageSize"` // 每页条数, 默认 20
	SortBy   string `json:"sortBy,omitempty"`   // 排序字段
	SortDesc bool   `json:"sortDesc,omitempty"` // 是否降序
}

// Page 分页响应
type Page[T any] struct {
	Items      []T `json:"items"`
	Total      int `json:"total"`      // 总记录数
	Page       int `json:"page"`       // 当前页码
	PageSize   int `json:"pageSize"`   // 每页条数
	TotalPages int `json:"totalPages"` // 总页数
}

// NewPage 创建分页响应
func NewPage[T any](items []T, total, page, pageSize int) Page[T] {
	totalPages := total / pageSize
	if total%pageSize > 0 {
		totalPages++
	}
	return Page[T]{Items: items, Total: total, Page: page, PageSize: pageSize, TotalPages: totalPages}
}

// DefaultPageRequest 返回默认分页(第1页, 每页20条)
func DefaultPageRequest() PageRequest {
	return PageRequest{Page: 1, PageSize: 20}
}

// Offset 计算 SQL LIMIT 偏移量
func (p PageRequest) Offset() int {
	if p.Page < 1 {
		return 0
	}
	return (p.Page - 1) * p.PageSize
}

// Limit 返回每页条数
func (p PageRequest) Limit() int {
	if p.PageSize < 1 {
		return 20
	}
	return p.PageSize
}

// ============================================================
// 过滤条件
// ============================================================

// ProcessInstanceFilter 流程实例过滤条件
type ProcessInstanceFilter struct {
	DefKey      string    `json:"defKey,omitempty"`      // 流程定义 Key
	DefID       string    `json:"defId,omitempty"`       // 流程定义 ID
	BusinessKey string    `json:"businessKey,omitempty"` // 业务 Key
	State       string    `json:"state,omitempty"`       // 状态: running/suspended/completed/terminated/rejected
	Assignee    string    `json:"assignee,omitempty"`    // 当前处理人
	Initiator   string    `json:"initiator,omitempty"`   // 发起人(从变量中匹配)
	StartAfter  *time.Time `json:"startAfter,omitempty"` // 创建时间起始
	StartBefore *time.Time `json:"startBefore,omitempty"`// 创建时间截止
	Keyword     string    `json:"keyword,omitempty"`    // 关键字搜索(流程名称/ID)
}

// ActivityFilter 活动/待办过滤条件
type ActivityFilter struct {
	ProcessInstanceID string `json:"processInstanceId,omitempty"` // 流程实例 ID
	Assignee          string `json:"assignee,omitempty"`          // 处理人
	ActivityID        string `json:"activityId,omitempty"`       // 节点 ID
	ActivityType      string `json:"activityType,omitempty"`     // 节点类型
	State             string `json:"state,omitempty"`            // 状态: active/completed
	IsSign            *bool  `json:"isSign,omitempty"`           // 是否加签活动
}

// HistoryFilter 历史记录过滤条件
type HistoryFilter struct {
	ProcessInstanceID string    `json:"processInstanceId,omitempty"`
	ActivityID        string    `json:"activityId,omitempty"`
	ActivityType      string    `json:"activityType,omitempty"`
	CompletedAfter    *time.Time `json:"completedAfter,omitempty"`
	CompletedBefore   *time.Time `json:"completedBefore,omitempty"`
	Assignee          string    `json:"assignee,omitempty"`
}

// DefinitionFilter 流程定义过滤条件
type DefinitionFilter struct {
	Key     string `json:"key,omitempty"`     // 定义 Key
	Name    string `json:"name,omitempty"`    // 定义名称(模糊匹配)
	Version int    `json:"version,omitempty"` // 版本号
}

// ============================================================
// 类型常量（对外暴露）
// ============================================================

type RejectType string
const (
	RejectPrevious  RejectType = "previous"
	RejectInitiator RejectType = "initiator"
	RejectSpecific  RejectType = "specific"
	RejectTerminate RejectType = "terminate"
)

type SignType string
const (
	SignForward  SignType = "forward"
	SignBackward SignType = "backward"
	SignParallel SignType = "parallel"
)

type SignStrategy string
const (
	StrategyOR  SignStrategy = "or"
	StrategyAND SignStrategy = "and"
)

// ============================================================
// 1. WorkflowEngine — 流程操作与管理的核心接口
// ============================================================

type WorkflowEngine interface {
	// --- 流程实例生命周期 ---
	StartProcessInstance(ctx context.Context, defID string, variables map[string]any) (*instance.ProcessInstance, error)
	SuspendProcessInstance(ctx context.Context, processInstanceID string) error
	ResumeProcessInstance(ctx context.Context, processInstanceID string) error
	TerminateProcessInstance(ctx context.Context, processInstanceID string) error

	// --- 任务操作 ---
	CompleteTask(ctx context.Context, activityInstanceID string, variables map[string]any) error
	ClaimTask(ctx context.Context, activityInstanceID, userID string) error
	UnclaimTask(ctx context.Context, activityInstanceID string) error
	TransferTask(ctx context.Context, activityInstanceID, newAssignee string) error
	DelegateTask(ctx context.Context, activityInstanceID, delegateAssignee string) error
	ReclaimTask(ctx context.Context, currentActivityID string) error
	RejectTask(ctx context.Context, activityInstanceID string, rejectType RejectType, reason string, targetNodeID string) error
	JumpTask(ctx context.Context, activityInstanceID, targetNodeID string) error
	UrgeTask(ctx context.Context, activityInstanceID string) (assignee string, err error)
	CcTask(ctx context.Context, processInstanceID, ccUser string) error

	// --- 加签/减签 ---
	AddSign(ctx context.Context, activityInstanceID string, signType SignType, strategy SignStrategy, assignees []string) error
	RemoveSign(ctx context.Context, activityInstanceID, assignee string) error

	// --- 超时处理 ---
	SetTimeout(ctx context.Context, activityInstanceID string, duration time.Duration, termMode int) error
	CheckTimeouts(ctx context.Context) (int, error)

	// --- 事件/信号/消息 ---
	ReceiveSignal(ctx context.Context, signalRef string, variables map[string]any) error
	ReceiveMessage(ctx context.Context, messageRef string, variables map[string]any) error
}

// ============================================================
// 2. DefinitionService — 流程定义管理 (供前端 CRUD)
// ============================================================

type DefinitionService interface {
	Deploy(ctx context.Context, def *spec.ProcessDefinition) (*spec.ProcessDefinition, error)
	GetByID(ctx context.Context, id string) (*spec.ProcessDefinition, error)
	GetByKey(ctx context.Context, key string) (*spec.ProcessDefinition, error)
	GetByKeyVersion(ctx context.Context, key string, version int) (*spec.ProcessDefinition, error)

	// List 分页查询流程定义, 支持按 Key/Name/Version 过滤
	List(ctx context.Context, filter DefinitionFilter, page PageRequest) (Page[*spec.ProcessDefinition], error)

	Delete(ctx context.Context, id string) error
}

// ============================================================
// 3. QueryService — 待办/历史/流程查询 (供审批端)
// ============================================================

type QueryService interface {
	// --- 流程实例查询(带过滤+分页) ---

	// GetInstance 获取单个流程实例详情
	GetInstance(ctx context.Context, processInstanceID string) (*instance.ProcessInstance, error)

	// ListInstances 分页查询流程实例, 支持多维度过滤
	ListInstances(ctx context.Context, filter ProcessInstanceFilter, page PageRequest) (Page[*instance.ProcessInstance], error)

	// ListCompletedInstances 分页查询已完成的流程实例
	ListCompletedInstances(ctx context.Context, filter ProcessInstanceFilter, page PageRequest) (Page[*instance.ProcessInstance], error)

	// --- 待办任务查询(带过滤+分页) ---

	// ListPendingActivities 分页查询指定流程实例的待办活动
	ListPendingActivities(ctx context.Context, filter ActivityFilter, page PageRequest) (Page[*instance.ActivityInstance], error)

	// ListMyPendingActivities 分页查询某用户的待办任务(按 assignee 筛选)
	ListMyPendingActivities(ctx context.Context, assignee string, page PageRequest) (Page[*instance.ActivityInstance], error)

	// ListActivitiesByProcess 分页查询流程实例的所有活动历史(按时间倒序)
	ListActivitiesByProcess(ctx context.Context, processInstanceID string, filter ActivityFilter, page PageRequest) (Page[*instance.ActivityInstance], error)

	// --- 历史查询(带过滤+分页) ---

	// ListHistoryByProcess 分页查询历史记录
	ListHistoryByProcess(ctx context.Context, filter HistoryFilter, page PageRequest) (Page[*instance.HistoricActivityInstance], error)

	// --- 变量查询 ---

	// GetVariables 获取流程实例的变量
	GetVariables(ctx context.Context, processInstanceID string) (map[string]any, error)

	// --- 统计查询 ---

	// CountByState 按状态统计流程实例数量
	CountByState(ctx context.Context) (map[instance.ProcessInstanceState]int, error)
}
