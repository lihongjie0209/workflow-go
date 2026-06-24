# Workflow-Go 开发计划 & 技术参考手册

## 当前状态 vs FlowLong 功能对比

| 类别 | 功能 | 状态 | 优先级 |
|------|------|------|--------|
| **核心** | 转办 (Transfer) | ❌ 缺失 | **P0** |
| **核心** | 减签 (RemoveSign) | ❌ 缺失 | **P0** |
| **核心** | 超时自动审批 (Timeout) | ❌ 缺失 | **P0** |
| **核心** | 驳回策略完善 (Reject+) | ✅ 已有(需增强) | **P0** |
| **审批** | 委派 (Delegate) | ❌ 缺失 | P1 |
| **审批** | 代理 (Agent) | ❌ 缺失 | P1 |
| **审批** | 拿回 (Reclaim/Withdraw) | ❌ 缺失 | P1 |
| **审批** | 跳转 (Jump) | ❌ 缺失 | P1 |
| **审批** | 唤醒 (Resume) | ❌ 缺失 | P1 |
| **审批** | 撤销 (Cancel) | ❌ 缺失 | P1 |
| **审批** | 暂存 (Draft) | ❌ 缺失 | P1 |
| **审批** | 催办 (Urge) | ❌ 缺失 | P1 |
| **会签** | 票签 (Voting) | ❌ 缺失 | P1 |
| **增强** | 超时提醒 (Reminder) | ❌ 缺失 | P1 |
| **增强** | 抄送 (CC) | ❌ 缺失 | P1 |
| **增强** | 分组策略 (Group) | ❌ 缺失 | P2 |
| **增强** | AI 审批 | ❌ 缺失 | P2 |
| **增强** | 多租户 | ❌ 缺失 | P2 |

---

## P0 功能详细设计

### 1. 转办 (Transfer)

**FlowLong 参考实现:**
```java
// TaskServiceImpl.java - 单任务转办
public void transferTask(Long taskId, FlowCreator flowCreator, FlowCreator assigneeFlowCreator) {
    // 1. 验证当前用户是任务的合法参与者
    // 2. 检查是否已转办过 (assignorId 非空则拒绝)
    // 3. 删除当前用户的 actor 记录
    // 4. 为目标用户创建新的 actor 记录
    // 5. 设置 taskType = transfer
    // 6. 触发 TaskEventType.transfer
}

// 批量转办 (离职)
public void transferTask(FlowCreator flowCreator, FlowCreator assigneeFlowCreator) {
    // 查询该用户所有任务参与者记录
    // 逐条替换 actorId 和 actorName
}
```

**我们的 Go 实现方案:**

ActivityInstance 当前只有单个 Assignee，转办即修改 Assignee 并记录原值。

```go
type EngineOption struct {
    // ...
}

// TransferTask 将当前活动转办给新的审批人
// 原审批人不再拥有该任务，新审批人接手
func (e *ProcessEngine) TransferTask(ctx, activityInstanceID, newAssignee string, variables map[string]any) error {
    ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
    // 验证 state=active, type=userTask
    
    // 记录转办前信息
    e.store.SetVariable(ctx, ai.ProcessInstanceID, 
        "__transfer_from_"+ai.ID, ai.Assignee)
    
    // 更新审批人
    ai.Assignee = newAssignee
    return e.store.UpdateActivityInstance(ctx, ai)
}

// BatchTransferTask 批量转办 (用于离职)
func (e *ProcessEngine) BatchTransferTask(ctx, fromUser, toUser string) (int, error) {
    // 查询所有 pi 下 active 活动 assignee=fromUser
    // 逐个转办
}
```

---

### 2. 减签 (RemoveSign)

**FlowLong 参考实现:**
```java
// TaskServiceImpl.java
public boolean removeTaskActor(Long taskId, List<String> actorIds, FlowCreator flowCreator) {
    // 1. 查询已有的所有 task actor
    // 2. 过滤出要删除的 actor
    // 3. 校验: 不能删除所有参与者 (至少留一个)
    // 4. 删除对应的 actor 记录
    // 5. 触发 TaskEventType.removeTaskActor
}
```

**我们的 Go 实现方案:**
减签即删除特定的加签 ActivityInstance + Token

```go
func (e *ProcessEngine) RemoveSign(ctx, activityInstanceID string, assignee string) error {
    ai, err := e.store.GetActivityInstance(ctx, activityInstanceID)
    // 验证
    // 
    // 查找该活动的所有加签子活动中 assignee 匹配的
    // 找到后:
    // 1. 完成该加签活动
    // 2. 消费对应的 Token
    // 3. 更新加签计数变量
}
```

---

### 3. 超时自动审批 (Timeout)

**FlowLong 参考实现:**
```java
// FlowLongScheduler.java
// cron: */5 * * * * ?
public void remind() {
    // 1. 分布式锁
    // 2. 查询过期待办: expireTime <= now OR remindTime <= now
    // 3. 提醒处理: remindTime <= now → 调用 TaskReminder.remind()
    // 4. 超时处理: expireTime <= now → 根据 termMode 决定:
    //    - termMode=0 → autoCompleteTask() 自动通过
    //    - termMode=1 → autoRejectTask() 自动驳回
    //    - termMode=null → timeout() 强制超时结束
}

// NodeModel.java
// term: 超时小时数
// termAuto: 是否启用超时处理
// termMode: 0=自动通过, 1=自动驳回
```

**我们的 Go 实现方案:**
利用已有的 TimerJob 机制 + 新增 `expireTime` 字段

```go
// ActivityInstance 新增字段
type ActivityInstance struct {
    // ...
    ExpireTime  *time.Time // 超时时间
    TermMode    int        // 0=自动通过, 1=自动驳回
}

// TimerScheduler 增强: 检查超时活动
func (ts *TimerScheduler) checkTimeouts(ctx) {
    // 查询所有活跃活动的 expireTime <= now
    // 根据 termMode autoCompleteTask 或 autoRejectTask
}
```

---

### 4. 驳回策略增强

**FlowLong 参考实现:**
```java
// RejectStrategy.java
TO_INITIATOR(1), TO_PREVIOUS_NODE(2), TO_SPECIFIED_NODE(3), TERMINATE_APPROVAL(4), TO_PARENT_NODE(5)

// FlowLongEngineImpl.java
// 使用 executeJumpTask 实现跳转 + 归档原任务
// 使用 undoTask 拿回父任务
```

**我们的当前状态:**
- 已有 RejectPrevious, RejectInitiator, RejectSpecific, RejectTerminate ✅
- 缺少 **重新审批策略**: 继续执行(向前) vs 退回驳回节点(重新审批)
- 缺少 **TO_PARENT_NODE**: 驳回到模型中的父节点

需要增强:
```go
// 新增 RejectAfterType
type RejectAfterType string
const (
    RejectAfterContinue RejectAfterType = "continue" // 继续执行(默认)
    RejectAfterRework   RejectAfterType = "rework"   // 退回重新审批
)
```

---

## 实现顺序

```
Phase P0-1: 转办 (TransferTask)      ← 当前
Phase P0-2: 减签 (RemoveSign)
Phase P0-3: 超时自动审批 (Timeout)
Phase P0-4: 驳回策略 + 票签 (Voting)
Phase P1:   委派/代理/拿回/跳转/催办/抄送
```
