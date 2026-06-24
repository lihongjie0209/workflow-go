package spec

// StartEvent represents the beginning of a process.
// It must have exactly one outgoing sequence flow.
type StartEvent struct {
	ID   string
	Name string
}

func (e *StartEvent) GetID() string        { return e.ID }
func (e *StartEvent) GetName() string      { return e.Name }
func (e *StartEvent) GetType() ElementType { return ElementTypeStartEvent }

// EndEvent represents the termination of a process path.
// When all tokens reach end events, the process instance completes.
type EndEvent struct {
	ID   string
	Name string
}

func (e *EndEvent) GetID() string        { return e.ID }
func (e *EndEvent) GetName() string      { return e.Name }
func (e *EndEvent) GetType() ElementType { return ElementTypeEndEvent }

// LoopCharacteristics defines how a multi-instance activity behaves.
type LoopCharacteristics struct {
	IsSequential        bool   `json:"isSequential,omitempty"`
	LoopCardinality     string `json:"loopCardinality,omitempty"`
	Collection          string `json:"collection,omitempty"`
	ElementVariable     string `json:"elementVariable,omitempty"`
	CompletionCondition string `json:"completionCondition,omitempty"`
}

// UserTask is a human task that requires explicit completion.
// It represents a wait state in the process — the engine pauses
// at a UserTask until CompleteTask is called.
// When LoopCharacteristics is set, the task becomes a multi-instance
// activity (会签), generating multiple parallel or sequential instances.
type UserTask struct {
	ID                 string
	Name               string
	Assignee           string
	CandidateUsers     []string
	CandidateGroup     string
	FormKey            string
	LoopCharacteristics *LoopCharacteristics
}

func (t *UserTask) GetID() string        { return t.ID }
func (t *UserTask) GetName() string      { return t.Name }
func (t *UserTask) GetType() ElementType { return ElementTypeUserTask }

// ServiceTask is an automatic task executed by the engine.
// In the initial implementation, it completes automatically upon arrival.
type ServiceTask struct {
	ID       string
	Name     string
	Type     string
}

func (t *ServiceTask) GetID() string        { return t.ID }
func (t *ServiceTask) GetName() string      { return t.Name }
func (t *ServiceTask) GetType() ElementType { return ElementTypeServiceTask }

// CallActivity represents a call to another process definition (sub-process).
type CallActivity struct {
	ID               string
	Name             string
	CalledElement    string
	InheritVariables bool
}

func (ca *CallActivity) GetID() string        { return ca.ID }
func (ca *CallActivity) GetName() string      { return ca.Name }
func (ca *CallActivity) GetType() ElementType { return ElementTypeCallActivity }

// --- Event Types ---

// TimerEventDefinition defines a timer-based trigger.
type TimerEventDefinition struct {
	TimerDuration string `json:"timerDuration,omitempty"` // ISO 8601 duration
	TimerDate     string `json:"timerDate,omitempty"`
	TimerCycle    string `json:"timerCycle,omitempty"`
}

// SignalEventDefinition defines a named signal trigger.
type SignalEventDefinition struct {
	SignalRef string `json:"signalRef,omitempty"`
}

// MessageEventDefinition defines a named message trigger.
type MessageEventDefinition struct {
	MessageRef string `json:"messageRef,omitempty"`
}

// BoundaryEvent attaches to an activity and triggers when the event occurs.
// v1 only supports interrupting boundary events (CancelActivity=true).
type BoundaryEvent struct {
	ID                string
	Name              string
	AttachedToRef     string // element ID of the activity this attaches to
	CancelActivity    bool   // true=interrupting (only mode in v1)
	TimerDefinition   *TimerEventDefinition   `json:"timerDefinition,omitempty"`
	SignalDefinition  *SignalEventDefinition  `json:"signalDefinition,omitempty"`
	MessageDefinition *MessageEventDefinition `json:"messageDefinition,omitempty"`
}

func (be *BoundaryEvent) GetID() string        { return be.ID }
func (be *BoundaryEvent) GetName() string      { return be.Name }
func (be *BoundaryEvent) GetType() ElementType { return ElementTypeBoundaryEvent }

// IntermediateCatchEvent waits for an event in the main flow.
type IntermediateCatchEvent struct {
	ID                string
	Name              string
	TimerDefinition   *TimerEventDefinition   `json:"timerDefinition,omitempty"`
	SignalDefinition  *SignalEventDefinition  `json:"signalDefinition,omitempty"`
	MessageDefinition *MessageEventDefinition `json:"messageDefinition,omitempty"`
}

func (ice *IntermediateCatchEvent) GetID() string        { return ice.ID }
func (ice *IntermediateCatchEvent) GetName() string      { return ice.Name }
func (ice *IntermediateCatchEvent) GetType() ElementType { return ElementTypeIntermediateCatchEvent }

// IntermediateThrowEvent fires a signal/message and continues immediately.
type IntermediateThrowEvent struct {
	ID                string
	Name              string
	SignalDefinition  *SignalEventDefinition  `json:"signalDefinition,omitempty"`
	MessageDefinition *MessageEventDefinition `json:"messageDefinition,omitempty"`
}

func (ite *IntermediateThrowEvent) GetID() string        { return ite.ID }
func (ite *IntermediateThrowEvent) GetName() string      { return ite.Name }
func (ite *IntermediateThrowEvent) GetType() ElementType { return ElementTypeIntermediateThrowEvent }
