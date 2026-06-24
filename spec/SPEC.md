# Workflow Definition JSON Format (SPEC)

This document defines the JSON serialization format for `workflow-go` process definitions. The format is designed to be human-readable, machine-writable, and compatible with standard JSON tooling.

## Quick Reference

```json
{
  "id": "leave-approval",
  "name": "Leave Approval Process",
  "key": "leave-approval",
  "version": 1,
  "startEventId": "start",
  "elements": [ /* ... flow elements ... */ ],
  "sequenceFlows": [ /* ... edges ... */ ]
}
```

## Top-Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | **yes** | Unique identifier for the process definition |
| `name` | string | no | Human-readable name |
| `key` | string | no | Business key for versioning; used with `version` and deployment |
| `version` | int | no | Version number (auto-incremented on deploy) |
| `startEventId` | string | **yes** | ID of the start event element |
| `elements` | array | **yes** | Array of flow element objects (min 1) |
| `sequenceFlows` | array | **yes** | Array of sequence flow objects (can be empty) |
| `deploymentId` | string | no | Deployment identifier (reserved) |

## Element Types

The `type` field on each element determines which additional fields apply.

| type | Element | Extra Fields |
|------|---------|-------------|
| `startEvent` | Process start | _(none)_ |
| `endEvent` | Process end | _(none)_ |
| `userTask` | Human task | `assignee`, `candidateUsers`, `candidateGroup`, `formKey`, `loopCharacteristics` |
| `serviceTask` | Auto task | `serviceType` |
| `exclusiveGateway` | XOR gateway | `defaultFlowId` |
| `parallelGateway` | AND gateway | _(none)_ |
| `inclusiveGateway` | OR gateway | `defaultFlowId` |
| `boundaryEvent` | Event on activity | `attachedToRef`, `cancelActivity`, `timerDefinition`, `signalDefinition`, `messageDefinition` |
| `intermediateCatchEvent` | Wait in flow | `timerDefinition`, `signalDefinition`, `messageDefinition` |
| `intermediateThrowEvent` | Fire & continue | `signalDefinition`, `messageDefinition` |
| `callActivity` | Sub-process call | `calledElement`, `inheritVariables` |

### Common Fields

Every element has:
```json
{ "id": "element-id", "name": "Display Name", "type": "elementType" }
```

- `id` — unique within the process definition **(required)**
- `name` — display name; supports `${}` variable templates
- `type` — element type from the table above **(required)**

### StartEvent

```json
{ "id": "start", "name": "Start", "type": "startEvent" }
```

- Must have exactly one outgoing sequence flow.

### EndEvent

```json
{ "id": "end", "name": "End", "type": "endEvent" }
```

- Consumes incoming token and triggers process completion check.
- At least one EndEvent is required per process definition.

### UserTask

```json
{
  "id": "approval",
  "name": "Manager Approval",
  "type": "userTask",
  "assignee": "${manager}",
  "candidateUsers": ["user1", "user2"],
  "candidateGroup": "managers",
  "formKey": "form_leave"
}
```

Fields:
- `assignee` — assigned user or `${expression}` for dynamic resolution at runtime
- `candidateUsers` — list of candidate user IDs
- `candidateGroup` — candidate group name
- `formKey` — form identifier; supports `${}` templates
- `loopCharacteristics` — multi-instance config (会签), see below

### ServiceTask

```json
{ "id": "autoCheck", "name": "Check Inventory", "type": "serviceTask", "serviceType": "http" }
```

- Completes automatically when execution arrives.
- `serviceType` — reserved for future service integration.

### ExclusiveGateway (XOR)

```json
{
  "id": "decision",
  "name": "Amount Check",
  "type": "exclusiveGateway",
  "defaultFlowId": "sf_default"
}
```

- Routes to exactly one outgoing flow.
- Conditions evaluated in order; first match wins.
- `defaultFlowId` — fallback when no condition matches.

### ParallelGateway (AND)

```json
{ "id": "fork", "name": "Parallel Fork", "type": "parallelGateway" }
```

- Join: waits for ALL incoming tokens before proceeding.
- Split: activates ALL outgoing flows simultaneously.

### InclusiveGateway (OR)

```json
{
  "id": "multiChoice",
  "name": "Multi-Condition",
  "type": "inclusiveGateway",
  "defaultFlowId": "sf_else"
}
```

- Evaluates conditions on all outgoing flows.
- Activates every flow whose condition is true.
- `defaultFlowId` — used when no condition matches.

### BoundaryEvent

```json
{
  "id": "timeout",
  "name": "Approval Timeout",
  "type": "boundaryEvent",
  "attachedToRef": "approval",
  "cancelActivity": true,
  "timerDefinition": { "timerDuration": "PT1H" }
}
```

- Attaches to a UserTask or ServiceTask.
- `attachedToRef` — ID of the target activity **(required)**
- `cancelActivity` — true=interrupting (default), consumes the activity's token
- One of `timerDefinition`, `signalDefinition`, or `messageDefinition` is required.

### IntermediateCatchEvent

```json
{
  "id": "waitForSignal",
  "name": "Wait for Signal",
  "type": "intermediateCatchEvent",
  "signalDefinition": { "signalRef": "orderReady" }
}
```

- Pauses execution until the specified event occurs.
- One of `timerDefinition`, `signalDefinition`, or `messageDefinition` is required.

### IntermediateThrowEvent

```json
{
  "id": "notify",
  "name": "Notify Shipping",
  "type": "intermediateThrowEvent",
  "signalDefinition": { "signalRef": "orderReady" }
}
```

- Fires a signal or message and continues immediately.
- One of `signalDefinition` or `messageDefinition` is required.

### CallActivity (SubProcess)

```json
{
  "id": "fulfill",
  "name": "Fulfill Order",
  "type": "callActivity",
  "calledElement": "fulfillment-process",
  "inheritVariables": true
}
```

- Calls another process definition by its `key`.
- `calledElement` — the `key` of the process definition to invoke **(required)**
- `inheritVariables` — if true, copies parent process variables into the child

## Nested Objects

### LoopCharacteristics (for UserTask multi-instance)

```json
{
  "loopCharacteristics": {
    "isSequential": false,
    "loopCardinality": "5",
    "collection": "reviewers",
    "elementVariable": "reviewer",
    "completionCondition": "${nrOfCompletedInstances >= 2}"
  }
}
```

| Field | Type | Description |
|-------|------|-------------|
| `isSequential` | bool | true=sequential (one at a time), false=parallel (all at once) |
| `loopCardinality` | string | Fixed instance count as numeric expression |
| `collection` | string | Variable name of the input array |
| `elementVariable` | string | Variable name per instance (from collection item) |
| `completionCondition` | string | `${}` expression for early termination |

Runtime counter variables (accessible in expressions):
- `nrOfInstances` — total instances
- `nrOfActiveInstances` — currently active instances
- `nrOfCompletedInstances` — completed instances

### TimerEventDefinition

```json
{ "timerDuration": "PT1H" }
```

| Field | Description |
|-------|-------------|
| `timerDuration` | ISO 8601 duration (e.g. `PT1H`, `PT30M`, `PT1H30M`, `P1D`) |
| `timerDate` | Fixed date/time (RFC3339) |
| `timerCycle` | Repeating interval (ISO 8601) |

### SignalEventDefinition

```json
{ "signalRef": "mySignal" }
```

- `signalRef` — signal name, matched with `ReceiveSignal` API calls

### MessageEventDefinition

```json
{ "messageRef": "myMessage" }
```

- `messageRef` — message name. In v1, uses the same mechanism as signals.

## SequenceFlow

```json
{
  "id": "sf1",
  "name": "high amount path",
  "sourceRef": "xor_gateway",
  "targetRef": "director_approval",
  "conditionExpression": "${amount > 5000}"
}
```

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `id` | string | **yes** | Unique flow identifier |
| `name` | string | no | Display name |
| `sourceRef` | string | **yes** | Source element ID |
| `targetRef` | string | **yes** | Target element ID |
| `conditionExpression` | string or null | no | `${}` condition for gateway-outgoing flows. `null` or absent = unconditional |

## Variable Template Syntax

String fields in element definitions support `${expression}` template syntax:

| Template | Variables `{"manager":"张三","dept":"技术部"}` | Result |
|----------|---------------------------------------------|--------|
| `"${manager}"` | `manager: "张三"` | `"张三"` |
| `"hr_${dept}"` | `dept: "技术部"` | `"hr_技术部"` |
| `"报销-${type}-${id}"` | `type: "差旅", id: "001"` | `"报销-差旅-001"` |

Template rendering is performed at runtime by the engine before element activation.
The original definition retains `${}` expressions unchanged.

## Complete Example

```json
{
  "id": "order-fulfillment",
  "name": "Order Fulfillment",
  "key": "order-fulfillment",
  "version": 3,
  "startEventId": "start",
  "elements": [
    { "id": "start", "type": "startEvent" },
    { "id": "check", "type": "userTask", "assignee": "${clerk}", "formKey": "inventory_check" },
    { "id": "fork", "type": "parallelGateway" },
    { "id": "pack", "type": "userTask", "assignee": "warehouse" },
    { "id": "invoice", "type": "serviceTask", "serviceType": "billing" },
    { "id": "join", "type": "parallelGateway" },
    { "id": "ship", "type": "callActivity", "calledElement": "shipping-process", "inheritVariables": true },
    { "id": "notify", "type": "intermediateThrowEvent",
      "signalDefinition": { "signalRef": "orderShipped" } },
    { "id": "end", "type": "endEvent" }
  ],
  "sequenceFlows": [
    { "id": "sf1", "sourceRef": "start", "targetRef": "check" },
    { "id": "sf2", "sourceRef": "check", "targetRef": "fork" },
    { "id": "sf3", "sourceRef": "fork", "targetRef": "pack" },
    { "id": "sf4", "sourceRef": "fork", "targetRef": "invoice" },
    { "id": "sf5", "sourceRef": "pack", "targetRef": "join" },
    { "id": "sf6", "sourceRef": "invoice", "targetRef": "join" },
    { "id": "sf7", "sourceRef": "join", "targetRef": "ship" },
    { "id": "sf8", "sourceRef": "ship", "targetRef": "notify" },
    { "id": "sf9", "sourceRef": "notify", "targetRef": "end" }
  ]
}
```

This process:
1. Starts with a clerk checking inventory
2. Parallel fork: warehouse packs while billing auto-invoices
3. Join waits for both, then calls the shipping sub-process
4. Fires a signal and completes

## Validation Rules

From `spec.Validate()`:

- `id` must be non-empty
- `startEventId` must reference an existing element of type `startEvent`
- All element map keys must match element IDs
- All sequence flow `sourceRef`/`targetRef` must reference existing elements
- Start event must have at least one outgoing sequence flow
- At least one `endEvent` must exist
- Unknown element `type` values cause unmarshal errors
