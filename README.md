# Workflow-Go

A Go-native workflow engine inspired by Flowable/Activiti BPMN engines. Built with a pluggable storage architecture and comprehensive gateway support.

## Features

- **Process Definition DSL** — Native Go types for defining workflows (no XML required)
- **Three Gateway Types** — Exclusive (XOR), Parallel (AND), Inclusive (OR) with full join/split semantics
- **Pluggable Storage** — Storage interface with in-memory and SQLite implementations included
- **Expression Conditions** — Runtime condition evaluation using `expr-lang/expr` (`${amount > 100}`)
- **Token-Driven Execution** — Clean execution model based on BPMN token concepts
- **UserTask & ServiceTask** — Human tasks with explicit completion; auto-completing service tasks
- **TDD Built** — Every feature is tested with table-driven tests; storage contract tests ensure consistency across backends

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "github.com/lihongjie/workflow-go/engine"
    "github.com/lihongjie/workflow-go/instance"
    "github.com/lihongjie/workflow-go/spec"
    "github.com/lihongjie/workflow-go/storage/memstore"
)

func main() {
    ctx := context.Background()
    store := memstore.New()
    defer store.Close()
    eng := engine.NewProcessEngine(store)

    // 1. Define a process
    def := &spec.ProcessDefinition{
        ID:      "leave-approval",
        Key:     "leave-approval",
        Version: 1,
        Elements: map[string]spec.FlowElement{
            "start": &spec.StartEvent{ID: "start"},
            "approve": &spec.UserTask{ID: "approve", Name: "Manager Approval"},
            "end":     &spec.EndEvent{ID: "end"},
        },
        SequenceFlows: []*spec.SequenceFlow{
            {ID: "sf1", SourceRef: "start", TargetRef: "approve"},
            {ID: "sf2", SourceRef: "approve", TargetRef: "end"},
        },
        StartEventID: "start",
    }
    store.CreateProcessDefinition(ctx, def)

    // 2. Start a process instance
    pi, _ := eng.StartProcessInstance(ctx, "leave-approval", nil)
    fmt.Printf("Instance %s is %s\n", pi.ID, pi.State) // → running

    // 3. Find and complete the user task
    activities, _ := store.ListActiveActivities(ctx, pi.ID)
    eng.CompleteTask(ctx, activities[0].ID, map[string]any{"approved": true})

    // 4. Process completes automatically
    pi, _ = store.GetProcessInstance(ctx, pi.ID)
    fmt.Printf("Instance %s is %s\n", pi.ID, pi.State) // → completed
}
```

## Architecture

```
workflow-go/
├── spec/                  # Process definition types
│   ├── spec.go            # FlowElement interface, ProcessDefinition, Validate()
│   ├── elements.go        # StartEvent, EndEvent, UserTask, ServiceTask
│   ├── gateways.go        # ExclusiveGateway, ParallelGateway, InclusiveGateway
│   └── sequence_flow.go   # SequenceFlow with condition expressions
│
├── instance/              # Runtime state models
│   ├── process_instance.go
│   ├── activity_instance.go
│   └── token.go           # Execution token (active path)
│
├── expr/                  # Condition expression engine
│   └── expr.go            # Wraps expr-lang/expr: NewCondition().Evaluate(vars)
│
├── storage/               # Pluggable storage layer
│   ├── storage.go         # Store interface (5 sub-interfaces)
│   ├── memstore/          # In-memory implementation
│   ├── sqlstore/          # SQLite implementation (modernc.org/sqlite)
│   └── storagetest/       # Reusable contract test suite
│
└── engine/                # Core execution engine
    ├── engine.go          # ProcessEngine API
    ├── navigator.go       # navigateFrom: core transition function
    ├── gateway.go         # XOR/AND/OR evaluation logic
    └── engine_test.go     # Integration tests
```

## Defining a Process

### Linear Flow
```go
def := &spec.ProcessDefinition{
    ID: "simple",
    Elements: map[string]spec.FlowElement{
        "start": &spec.StartEvent{ID: "start"},
        "task1": &spec.UserTask{ID: "task1", Name: "Do Work"},
        "end":   &spec.EndEvent{ID: "end"},
    },
    SequenceFlows: []*spec.SequenceFlow{
        {ID: "sf1", SourceRef: "start", TargetRef: "task1"},
        {ID: "sf2", SourceRef: "task1", TargetRef: "end"},
    },
    StartEventID: "start",
}
```

### Exclusive Gateway (XOR) — If/Else
```go
high := "${amount > 100}"
def := &spec.ProcessDefinition{
    ID: "approval",
    Elements: map[string]spec.FlowElement{
        "start":    &spec.StartEvent{ID: "start"},
        "xor":      &spec.ExclusiveGateway{ID: "xor", DefaultFlowID: "sf_auto"},
        "approve":  &spec.UserTask{ID: "approve"},
        "auto":     &spec.ServiceTask{ID: "auto"},
        "end":      &spec.EndEvent{ID: "end"},
    },
    SequenceFlows: []*spec.SequenceFlow{
        {ID: "sf1", SourceRef: "start", TargetRef: "xor"},
        {ID: "sf_high", SourceRef: "xor", TargetRef: "approve", ConditionExpression: &high},
        {ID: "sf_auto", SourceRef: "xor", TargetRef: "auto"},
        {ID: "sf3", SourceRef: "approve", TargetRef: "end"},
        {ID: "sf4", SourceRef: "auto", TargetRef: "end"},
    },
    StartEventID: "start",
}
```

### Parallel Gateway (AND) — Fork/Join
```go
def := &spec.ProcessDefinition{
    ID: "review",
    Elements: map[string]spec.FlowElement{
        "start": &spec.StartEvent{ID: "start"},
        "fork":  &spec.ParallelGateway{ID: "fork"},
        "review_a": &spec.UserTask{ID: "review_a", Name: "Reviewer A"},
        "review_b": &spec.UserTask{ID: "review_b", Name: "Reviewer B"},
        "join":  &spec.ParallelGateway{ID: "join"},
        "end":   &spec.EndEvent{ID: "end"},
    },
    SequenceFlows: []*spec.SequenceFlow{
        {ID: "sf1", SourceRef: "start", TargetRef: "fork"},
        {ID: "sf2", SourceRef: "fork", TargetRef: "review_a"},
        {ID: "sf3", SourceRef: "fork", TargetRef: "review_b"},
        {ID: "sf4", SourceRef: "review_a", TargetRef: "join"},
        {ID: "sf5", SourceRef: "review_b", TargetRef: "join"},
        {ID: "sf6", SourceRef: "join", TargetRef: "end"},
    },
    StartEventID: "start",
}
```

### Inclusive Gateway (OR) — Multi-Condition Branch
```go
condA := "${category == \"priority\"}"
condB := "${urgent == true}"
def := &spec.ProcessDefinition{
    ID: "or-example",
    Elements: map[string]spec.FlowElement{
        "start": &spec.StartEvent{ID: "start"},
        "or":   &spec.InclusiveGateway{ID: "or"},
        "task_a": &spec.UserTask{ID: "task_a"},
        "task_b": &spec.UserTask{ID: "task_b"},
        "task_c": &spec.UserTask{ID: "task_c"},
        "end":   &spec.EndEvent{ID: "end"},
    },
    SequenceFlows: []*spec.SequenceFlow{
        {ID: "sf1", SourceRef: "start", TargetRef: "or"},
        {ID: "sf_a", SourceRef: "or", TargetRef: "task_a", ConditionExpression: &condA},
        {ID: "sf_b", SourceRef: "or", TargetRef: "task_b", ConditionExpression: &condB},
        {ID: "sf_c", SourceRef: "or", TargetRef: "task_c"},
        {ID: "sfa_e", SourceRef: "task_a", TargetRef: "end"},
        {ID: "sfb_e", SourceRef: "task_b", TargetRef: "end"},
        {ID: "sfc_e", SourceRef: "task_c", TargetRef: "end"},
    },
    StartEventID: "start",
}
```

## Storage

The `storage.Store` interface supports pluggable backends:

```go
type Store interface {
    ProcessDefinitionStore  // Create/Get/List/Delete definitions
    ProcessInstanceStore    // Create/Update/Get/List instances
    ActivityInstanceStore   // Create/Update/Get/List activities
    TokenStore              // Create/Update/Get/ListActive/Delete tokens
    VariableStore           // Set/Get/GetAll/Delete variables
    io.Closer
}
```

**In-Memory**: `memstore.New()` — thread-safe, ideal for testing.

**SQLite**: `sqlstore.New(sqlstore.WithMemory())` or `sqlstore.New(sqlstore.WithDBPath("workflow.db"))`:
- Pure Go via `modernc.org/sqlite` (no CGO)
- Automatic schema creation
- Supports `:memory:` mode for tests

To add a custom storage backend, implement the `storage.Store` interface and verify with `storagetest.RunStoreTestSuite`.

## API Reference

| Method | Description |
|--------|-------------|
| `StartProcessInstance(ctx, defID, variables)` | Create and start a process instance |
| `CompleteTask(ctx, activityInstanceID, variables)` | Complete a UserTask and advance execution |
| `GetStore()` | Access the underlying storage |

## Development

```bash
go test ./...    # Run all tests
go test -v ./... # Verbose output
go test -cover ./... # Coverage report
```

## Dependencies

- `github.com/expr-lang/expr` — Expression evaluation for gateway conditions
- `modernc.org/sqlite` — Pure Go SQLite driver (no CGO)

## License

MIT
