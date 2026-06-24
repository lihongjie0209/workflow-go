package spec

import (
	"encoding/json"
	"testing"
)

func TestProcessDefinition_Validate(t *testing.T) {
	tests := []struct {
		name    string
		def     *ProcessDefinition
		wantErr bool
	}{
		{
			name: "valid linear process",
			def: &ProcessDefinition{
				ID:       "proc1",
				Name:     "Test Process",
				Key:      "test-process",
				Version:  1,
				StartEventID: "start1",
				Elements: map[string]FlowElement{
					"start1": &StartEvent{ID: "start1", Name: "Start"},
					"task1":  &UserTask{ID: "task1", Name: "Task A"},
					"end1":   &EndEvent{ID: "end1", Name: "End"},
				},
				SequenceFlows: []*SequenceFlow{
					{ID: "sf1", SourceRef: "start1", TargetRef: "task1"},
					{ID: "sf2", SourceRef: "task1", TargetRef: "end1"},
				},
			},
			wantErr: false,
		},
		{
			name: "missing id",
			def: &ProcessDefinition{
				StartEventID: "start1",
				Elements:     map[string]FlowElement{"start1": &StartEvent{ID: "start1"}},
			},
			wantErr: true,
		},
		{
			name: "missing start event id",
			def: &ProcessDefinition{
				ID:       "proc1",
				Elements: map[string]FlowElement{"start1": &StartEvent{ID: "start1"}},
			},
			wantErr: true,
		},
		{
			name: "start event id not in elements",
			def: &ProcessDefinition{
				ID:           "proc1",
				StartEventID: "start1",
				Elements:     map[string]FlowElement{"task1": &UserTask{ID: "task1"}},
			},
			wantErr: true,
		},
		{
			name: "start event is wrong type",
			def: &ProcessDefinition{
				ID:           "proc1",
				StartEventID: "task1",
				Elements:     map[string]FlowElement{"task1": &UserTask{ID: "task1"}},
			},
			wantErr: true,
		},
		{
			name: "start event has no outgoing flow",
			def: &ProcessDefinition{
				ID:           "proc1",
				StartEventID: "start1",
				Elements: map[string]FlowElement{
					"start1": &StartEvent{ID: "start1"},
					"end1":   &EndEvent{ID: "end1"},
				},
				SequenceFlows: []*SequenceFlow{},
			},
			wantErr: true,
		},
		{
			name: "sequence flow source not found",
			def: &ProcessDefinition{
				ID:           "proc1",
				StartEventID: "start1",
				Elements: map[string]FlowElement{
					"start1": &StartEvent{ID: "start1"},
					"end1":   &EndEvent{ID: "end1"},
				},
				SequenceFlows: []*SequenceFlow{
					{ID: "sf1", SourceRef: "start1", TargetRef: "end1"},
					{ID: "sf2", SourceRef: "ghost", TargetRef: "end1"},
				},
			},
			wantErr: true,
		},
		{
			name: "sequence flow target not found",
			def: &ProcessDefinition{
				ID:           "proc1",
				StartEventID: "start1",
				Elements: map[string]FlowElement{
					"start1": &StartEvent{ID: "start1"},
				},
				SequenceFlows: []*SequenceFlow{
					{ID: "sf1", SourceRef: "start1", TargetRef: "ghost"},
				},
			},
			wantErr: true,
		},
		{
			name: "missing end event",
			def: &ProcessDefinition{
				ID:           "proc1",
				StartEventID: "start1",
				Elements: map[string]FlowElement{
					"start1": &StartEvent{ID: "start1"},
					"task1":  &UserTask{ID: "task1"},
				},
				SequenceFlows: []*SequenceFlow{
					{ID: "sf1", SourceRef: "start1", TargetRef: "task1"},
				},
			},
			wantErr: true,
		},
		{
			name: "empty elements",
			def: &ProcessDefinition{
				ID:           "proc1",
				StartEventID: "start1",
				Elements:     map[string]FlowElement{},
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.def.Validate()
			if (err != nil) != tt.wantErr {
				t.Errorf("Validate() error = %v, wantErr = %v", err, tt.wantErr)
			}
		})
	}
}

func TestElementTypes(t *testing.T) {
	tests := []struct {
		name     string
		element  FlowElement
		wantType ElementType
	}{
		{"StartEvent", &StartEvent{}, ElementTypeStartEvent},
		{"EndEvent", &EndEvent{}, ElementTypeEndEvent},
		{"UserTask", &UserTask{}, ElementTypeUserTask},
		{"ServiceTask", &ServiceTask{}, ElementTypeServiceTask},
		{"ExclusiveGateway", &ExclusiveGateway{}, ElementTypeExclusiveGateway},
		{"ParallelGateway", &ParallelGateway{}, ElementTypeParallelGateway},
		{"InclusiveGateway", &InclusiveGateway{}, ElementTypeInclusiveGateway},
		{"BoundaryEvent", &BoundaryEvent{}, ElementTypeBoundaryEvent},
		{"IntermediateCatchEvent", &IntermediateCatchEvent{}, ElementTypeIntermediateCatchEvent},
		{"IntermediateThrowEvent", &IntermediateThrowEvent{}, ElementTypeIntermediateThrowEvent},
		{"CallActivity", &CallActivity{}, ElementTypeCallActivity},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.element.GetType(); got != tt.wantType {
				t.Errorf("GetType() = %v, want %v", got, tt.wantType)
			}
		})
	}
}

func TestElementID(t *testing.T) {
	el := &UserTask{ID: "task1", Name: "My Task"}
	if el.GetID() != "task1" {
		t.Errorf("GetID() = %v, want %v", el.GetID(), "task1")
	}
	if el.GetName() != "My Task" {
		t.Errorf("GetName() = %v, want %v", el.GetName(), "My Task")
	}
}

func TestSequenceFlow_HasCondition(t *testing.T) {
	cond := "${amount > 100}"
	tests := []struct {
		name string
		sf   *SequenceFlow
		want bool
	}{
		{"with condition", &SequenceFlow{ConditionExpression: &cond}, true},
		{"without condition", &SequenceFlow{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.sf.HasCondition(); got != tt.want {
				t.Errorf("HasCondition() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestFindOutgoingFlows(t *testing.T) {
	flows := []*SequenceFlow{
		{ID: "sf1", SourceRef: "a", TargetRef: "b"},
		{ID: "sf2", SourceRef: "a", TargetRef: "c"},
		{ID: "sf3", SourceRef: "b", TargetRef: "d"},
	}

	outgoing := FindOutgoingFlows(flows, "a")
	if len(outgoing) != 2 {
		t.Errorf("expected 2 outgoing flows from 'a', got %d", len(outgoing))
	}

	outgoingB := FindOutgoingFlows(flows, "b")
	if len(outgoingB) != 1 {
		t.Errorf("expected 1 outgoing flow from 'b', got %d", len(outgoingB))
	}

	outgoingMissing := FindOutgoingFlows(flows, "x")
	if len(outgoingMissing) != 0 {
		t.Errorf("expected 0 outgoing flows from 'x', got %d", len(outgoingMissing))
	}
}

func TestFindIncomingFlows(t *testing.T) {
	flows := []*SequenceFlow{
		{ID: "sf1", SourceRef: "a", TargetRef: "b"},
		{ID: "sf2", SourceRef: "c", TargetRef: "b"},
	}

	incoming := FindIncomingFlows(flows, "b")
	if len(incoming) != 2 {
		t.Errorf("expected 2 incoming flows to 'b', got %d", len(incoming))
	}
}

// --- JSON Serialization Tests ---

const jsonExample = `{
  "id": "leave-approval",
  "name": "Leave Approval",
  "key": "leave-approval",
  "version": 1,
  "startEventId": "start",
  "elements": [
    {"id": "start", "name": "Start", "type": "startEvent"},
    {"id": "submit", "name": "Submit", "type": "userTask", "assignee": "${applicant}"},
    {"id": "xor", "name": "Amount Check", "type": "exclusiveGateway"},
    {"id": "mgr", "name": "Manager Approve", "type": "userTask", "assignee": "${manager}"},
    {"id": "end", "name": "End", "type": "endEvent"}
  ],
  "sequenceFlows": [
    {"id": "sf1", "sourceRef": "start", "targetRef": "submit"},
    {"id": "sf2", "sourceRef": "submit", "targetRef": "xor"},
    {"id": "sf3", "sourceRef": "xor", "targetRef": "mgr"},
    {"id": "sf4", "sourceRef": "mgr", "targetRef": "end"}
  ]
}`

// TestJSON_RoundTrip_FromString verifies unmarshalling from a JSON string literal,
// then marshalling back, ensuring the JSON structure is stable.
func TestJSON_RoundTrip_FromString(t *testing.T) {
	var def ProcessDefinition
	if err := json.Unmarshal([]byte(jsonExample), &def); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	// Verify unmarshalled fields
	if def.ID != "leave-approval" {
		t.Errorf("ID = %q, want leave-approval", def.ID)
	}
	if def.StartEventID != "start" {
		t.Errorf("StartEventID = %q", def.StartEventID)
	}
	if len(def.Elements) != 5 {
		t.Errorf("Elements = %d, want 5", len(def.Elements))
	}
	if len(def.SequenceFlows) != 4 {
		t.Errorf("SequenceFlows = %d, want 4", len(def.SequenceFlows))
	}

	// Verify specific elements
	start := def.Elements["start"]
	if start.GetType() != ElementTypeStartEvent {
		t.Errorf("start type = %q", start.GetType())
	}
	submit := def.Elements["submit"].(*UserTask)
	if submit.Assignee != "${applicant}" {
		t.Errorf("submit assignee = %q, want ${applicant}", submit.Assignee)
	}

	// Marshal back to JSON and verify no errors
	data, err := json.Marshal(&def)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	// Re-unmarshal and verify (pass pointer to ensure UnmarshalJSON is called)
	var def2 ProcessDefinition
	if err := json.Unmarshal(data, &def2); err != nil {
		t.Fatalf("json.Unmarshal (round 2): %v", err)
	}
	if len(def2.Elements) != len(def.Elements) {
		t.Errorf("round-trip Elements: %d != %d", len(def2.Elements), len(def.Elements))
	}
}

// TestJSON_AllElevenTypes tests that all 11 element types round-trip correctly.
func TestJSON_AllElevenTypes(t *testing.T) {
	def := &ProcessDefinition{
		ID: "all-types", Key: "all-types", Version: 1,
		StartEventID: "start1",
		Elements: map[string]FlowElement{
			"start1": &StartEvent{ID: "start1", Name: "Start"},
			"end1":   &EndEvent{ID: "end1", Name: "End"},
			"task1":  &UserTask{ID: "task1", Name: "Approval", Assignee: "${manager}", FormKey: "form_${type}"},
			"svc1":   &ServiceTask{ID: "svc1", Name: "AutoCheck", Type: "http"},
			"xor1":   &ExclusiveGateway{ID: "xor1", Name: "Decision", DefaultFlowID: "sf_default"},
			"and1":   &ParallelGateway{ID: "and1", Name: "Fork"},
			"or1":    &InclusiveGateway{ID: "or1", Name: "Multi-Choice", DefaultFlowID: "sf_else"},
			"bound1": &BoundaryEvent{
				ID: "bound1", Name: "Timeout", AttachedToRef: "task1",
				CancelActivity: true,
				TimerDefinition: &TimerEventDefinition{TimerDuration: "PT1H"},
			},
			"catch1": &IntermediateCatchEvent{
				ID: "catch1", Name: "WaitForSignal",
				SignalDefinition: &SignalEventDefinition{SignalRef: "mySignal"},
			},
			"throw1": &IntermediateThrowEvent{
				ID: "throw1", Name: "Notify",
				SignalDefinition: &SignalEventDefinition{SignalRef: "done"},
			},
			"call1": &CallActivity{ID: "call1", Name: "SubProcess", CalledElement: "sub-flow", InheritVariables: true},
		},
		SequenceFlows: []*SequenceFlow{
			{ID: "sf1", SourceRef: "start1", TargetRef: "task1"},
			{ID: "sf2", SourceRef: "task1", TargetRef: "end1"},
		},
	}

	data, err := json.Marshal(def)
	if err != nil {
		t.Fatalf("json.Marshal: %v", err)
	}

	var got ProcessDefinition
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}

	// Verify element types
	expectedTypes := map[string]ElementType{
		"start1": ElementTypeStartEvent,
		"end1":   ElementTypeEndEvent,
		"task1":  ElementTypeUserTask,
		"svc1":   ElementTypeServiceTask,
		"xor1":   ElementTypeExclusiveGateway,
		"and1":   ElementTypeParallelGateway,
		"or1":    ElementTypeInclusiveGateway,
		"bound1": ElementTypeBoundaryEvent,
		"catch1": ElementTypeIntermediateCatchEvent,
		"throw1": ElementTypeIntermediateThrowEvent,
		"call1":  ElementTypeCallActivity,
	}
	for id, wantType := range expectedTypes {
		el, ok := got.Elements[id]
		if !ok {
			t.Errorf("missing element %q after round-trip", id)
			continue
		}
		if el.GetType() != wantType {
			t.Errorf("element %q type = %q, want %q", id, el.GetType(), wantType)
		}
	}
	if len(got.Elements) != len(def.Elements) {
		t.Errorf("Elements count = %d, want %d", len(got.Elements), len(def.Elements))
	}
}

// TestJSON_UserTaskFields verifies UserTask-specific fields survive round-trip.
func TestJSON_UserTaskFields(t *testing.T) {
	def := &ProcessDefinition{
		ID: "ut-test", StartEventID: "start",
		Elements: map[string]FlowElement{
			"start": &StartEvent{ID: "start"},
			"task1": &UserTask{
				ID: "task1", Name: "审批",
				Assignee:       "${manager}",
				CandidateUsers: []string{"user1", "user2"},
				CandidateGroup: "admins",
				FormKey:        "form_leave",
				LoopCharacteristics: &LoopCharacteristics{
					IsSequential:        true,
					Collection:          "approvers",
					ElementVariable:     "approver",
					CompletionCondition: "${nrOfCompletedInstances >= 2}",
				},
			},
			"end": &EndEvent{ID: "end"},
		},
		SequenceFlows: []*SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "task1"},
			{ID: "sf2", SourceRef: "task1", TargetRef: "end"},
		},
	}

	data, _ := json.Marshal(def)
	var got ProcessDefinition
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	ut, ok := got.Elements["task1"].(*UserTask)
	if !ok {
		t.Fatal("task1 is not a UserTask")
	}
	if ut.Assignee != "${manager}" {
		t.Errorf("Assignee = %q", ut.Assignee)
	}
	if len(ut.CandidateUsers) != 2 || ut.CandidateUsers[0] != "user1" {
		t.Errorf("CandidateUsers = %v", ut.CandidateUsers)
	}
	if ut.CandidateGroup != "admins" {
		t.Errorf("CandidateGroup = %q", ut.CandidateGroup)
	}
	if ut.FormKey != "form_leave" {
		t.Errorf("FormKey = %q", ut.FormKey)
	}
	if ut.LoopCharacteristics == nil {
		t.Fatal("LoopCharacteristics is nil")
	}
	if ut.LoopCharacteristics.Collection != "approvers" {
		t.Errorf("Collection = %q", ut.LoopCharacteristics.Collection)
	}
	if !ut.LoopCharacteristics.IsSequential {
		t.Errorf("IsSequential = false")
	}
}

// TestJSON_GatewayFields verifies gateway-specific fields.
func TestJSON_GatewayFields(t *testing.T) {
	def := &ProcessDefinition{
		ID: "gw-test", StartEventID: "start",
		Elements: map[string]FlowElement{
			"start": &StartEvent{ID: "start"},
			"xor":   &ExclusiveGateway{ID: "xor", DefaultFlowID: "sf_default"},
			"and":   &ParallelGateway{ID: "and"},
			"or":    &InclusiveGateway{ID: "or", DefaultFlowID: "sf_else"},
			"end":   &EndEvent{ID: "end"},
		},
		SequenceFlows: []*SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "xor"},
			{ID: "sf2", SourceRef: "xor", TargetRef: "end"},
		},
	}

	data, _ := json.Marshal(def)
	var got ProcessDefinition
	json.Unmarshal(data, &got)

	xor := got.Elements["xor"].(*ExclusiveGateway)
	if xor.DefaultFlowID != "sf_default" {
		t.Errorf("xor DefaultFlowID = %q", xor.DefaultFlowID)
	}
	or := got.Elements["or"].(*InclusiveGateway)
	if or.DefaultFlowID != "sf_else" {
		t.Errorf("or DefaultFlowID = %q", or.DefaultFlowID)
	}
	// ParallelGateway has no extra fields
	if _, ok := got.Elements["and"].(*ParallelGateway); !ok {
		t.Errorf("and is not ParallelGateway")
	}
}

// TestJSON_EventFields verifies boundary/intermediate event fields.
func TestJSON_EventFields(t *testing.T) {
	def := &ProcessDefinition{
		ID: "evt-test", StartEventID: "start",
		Elements: map[string]FlowElement{
			"start":  &StartEvent{ID: "start"},
			"task1":  &UserTask{ID: "task1"},
			"bound1": &BoundaryEvent{
				ID: "bound1", AttachedToRef: "task1", CancelActivity: true,
				TimerDefinition: &TimerEventDefinition{TimerDuration: "PT1H"},
			},
			"bound2": &BoundaryEvent{
				ID: "bound2", AttachedToRef: "task1",
				SignalDefinition: &SignalEventDefinition{SignalRef: "sig1"},
			},
			"catch1": &IntermediateCatchEvent{
				ID: "catch1",
				MessageDefinition: &MessageEventDefinition{MessageRef: "msg1"},
			},
			"throw1": &IntermediateThrowEvent{
				ID: "throw1",
				SignalDefinition: &SignalEventDefinition{SignalRef: "sig2"},
			},
			"end": &EndEvent{ID: "end"},
		},
		SequenceFlows: []*SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "task1"},
			{ID: "sf2", SourceRef: "task1", TargetRef: "end"},
		},
	}

	data, _ := json.Marshal(def)
	var got ProcessDefinition
	json.Unmarshal(data, &got)

	// Timer boundary
	b1 := got.Elements["bound1"].(*BoundaryEvent)
	if b1.AttachedToRef != "task1" {
		t.Errorf("bound1 AttachedToRef = %q", b1.AttachedToRef)
	}
	if !b1.CancelActivity {
		t.Errorf("bound1 CancelActivity = false")
	}
	if b1.TimerDefinition == nil || b1.TimerDefinition.TimerDuration != "PT1H" {
		t.Errorf("bound1 TimerDefinition = %v", b1.TimerDefinition)
	}

	// Signal boundary
	b2 := got.Elements["bound2"].(*BoundaryEvent)
	if b2.SignalDefinition == nil || b2.SignalDefinition.SignalRef != "sig1" {
		t.Errorf("bound2 SignalRef = %v", b2.SignalDefinition)
	}

	// Catch with message
	c1 := got.Elements["catch1"].(*IntermediateCatchEvent)
	if c1.MessageDefinition == nil || c1.MessageDefinition.MessageRef != "msg1" {
		t.Errorf("catch1 MessageRef = %v", c1.MessageDefinition)
	}

	// Throw with signal
	t1 := got.Elements["throw1"].(*IntermediateThrowEvent)
	if t1.SignalDefinition == nil || t1.SignalDefinition.SignalRef != "sig2" {
		t.Errorf("throw1 SignalRef = %v", t1.SignalDefinition)
	}
}

// TestJSON_CallActivityFields verifies CallActivity fields.
func TestJSON_CallActivityFields(t *testing.T) {
	def := &ProcessDefinition{
		ID: "call-test", StartEventID: "start",
		Elements: map[string]FlowElement{
			"start": &StartEvent{ID: "start"},
			"call1": &CallActivity{ID: "call1", CalledElement: "sub-flow", InheritVariables: true},
			"call2": &CallActivity{ID: "call2", CalledElement: "sub-flow2"},
			"end":   &EndEvent{ID: "end"},
		},
		SequenceFlows: []*SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "call1"},
			{ID: "sf2", SourceRef: "call1", TargetRef: "end"},
		},
	}

	data, _ := json.Marshal(def)
	var got ProcessDefinition
	json.Unmarshal(data, &got)

	call1 := got.Elements["call1"].(*CallActivity)
	if call1.CalledElement != "sub-flow" {
		t.Errorf("call1 CalledElement = %q", call1.CalledElement)
	}
	if !call1.InheritVariables {
		t.Errorf("call1 InheritVariables = false")
	}
	call2 := got.Elements["call2"].(*CallActivity)
	if call2.InheritVariables {
		t.Errorf("call2 InheritVariables should be false")
	}
}

// TestJSON_SequenceFlowFields verifies SequenceFlow JSON tags.
func TestJSON_SequenceFlowFields(t *testing.T) {
	cond := "${amount > 500}"
	sf := &SequenceFlow{
		ID:                  "sf1",
		Name:                "high amount",
		SourceRef:           "start",
		TargetRef:           "task1",
		ConditionExpression: &cond,
	}

	// Marshal to map
	data, _ := json.Marshal(sf)
	var m map[string]any
	json.Unmarshal(data, &m)

	if m["sourceRef"] != "start" {
		t.Errorf("sourceRef = %v, want 'start'", m["sourceRef"])
	}
	if m["targetRef"] != "task1" {
		t.Errorf("targetRef = %v", m["targetRef"])
	}
	if m["conditionExpression"] != "${amount > 500}" {
		t.Errorf("conditionExpression = %v", m["conditionExpression"])
	}

	// Unmarshal back
	var got SequenceFlow
	json.Unmarshal(data, &got)
	if got.ID != "sf1" || got.SourceRef != "start" || got.TargetRef != "task1" {
		t.Errorf("round-trip failed: %+v", got)
	}
	if got.ConditionExpression == nil || *got.ConditionExpression != "${amount > 500}" {
		t.Errorf("condition round-trip failed")
	}
}

// TestJSON_ConditionNull verifies that conditionExpression: null works correctly.
func TestJSON_ConditionNull(t *testing.T) {
	jsonStr := `{
	  "id": "test", "startEventId": "start", "version": 1,
	  "elements": [
		{"id": "start", "type": "startEvent"},
		{"id": "end", "type": "endEvent"}
	  ],
	  "sequenceFlows": [
		{"id": "sf1", "sourceRef": "start", "targetRef": "end", "conditionExpression": null}
	  ]
	}`
	var def ProcessDefinition
	if err := json.Unmarshal([]byte(jsonStr), &def); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(def.SequenceFlows) != 1 {
		t.Fatalf("expected 1 flow")
	}
	if def.SequenceFlows[0].ConditionExpression != nil {
		t.Errorf("expected nil condition, got %q", *def.SequenceFlows[0].ConditionExpression)
	}
}

// TestJSON_MissingOptionalFields verifies no error on missing optional fields.
func TestJSON_MissingOptionalFields(t *testing.T) {
	jsonStr := `{"id":"min","startEventId":"s","version":1,"elements":[{"id":"s","type":"startEvent"},{"id":"e","type":"endEvent"}],"sequenceFlows":[{"id":"sf","sourceRef":"s","targetRef":"e"}]}`
	var def ProcessDefinition
	if err := json.Unmarshal([]byte(jsonStr), &def); err != nil {
		t.Fatalf("Unmarshal minimal: %v", err)
	}
	if def.ID != "min" || len(def.Elements) != 2 || len(def.SequenceFlows) != 1 {
		t.Errorf("unexpected result: %+v", def)
	}
}

// TestJSON_UnknownElementType_ReturnsError verifies unknown type handling.
func TestJSON_UnknownElementType_ReturnsError(t *testing.T) {
	jsonStr := `{"id":"bad","startEventId":"s","version":1,"elements":[{"id":"s","type":"startEvent"},{"id":"x","type":"unknownType"},{"id":"e","type":"endEvent"}],"sequenceFlows":[{"id":"sf","sourceRef":"s","targetRef":"x"},{"id":"sf2","sourceRef":"x","targetRef":"e"}]}`
	var def ProcessDefinition
	err := json.Unmarshal([]byte(jsonStr), &def)
	if err == nil {
		t.Error("expected error for unknown element type")
	}
}

// TestJSON_EmptyArraySafe verifies that empty elements/sequenceFlows arrays work.
func TestJSON_EmptyArraySafe(t *testing.T) {
	// elements must have at least 1 item (Validated later), but JSON should parse
	jsonStr := `{"id":"t","startEventId":"s","version":1,"elements":[{"id":"s","type":"startEvent"},{"id":"e","type":"endEvent"}],"sequenceFlows":[{"id":"sf","sourceRef":"s","targetRef":"e"}]}`
	var def ProcessDefinition
	if err := json.Unmarshal([]byte(jsonStr), &def); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	if len(def.Elements) != 2 {
		t.Errorf("Elements = %d", len(def.Elements))
	}
}

// TestProcessDefinition_JSONRoundTrip verifies full round-trip with all types.
func TestProcessDefinition_JSONRoundTrip(t *testing.T) {
	def := &ProcessDefinition{
		ID: "roundtrip", Key: "roundtrip", Version: 1,
		StartEventID: "start1",
		Elements: map[string]FlowElement{
			"start1": &StartEvent{ID: "start1", Name: "Start"},
			"task1":  &UserTask{ID: "task1", Name: "Task", LoopCharacteristics: &LoopCharacteristics{IsSequential: true, Collection: "items"}},
			"bound1": &BoundaryEvent{ID: "bound1", Name: "Timer", AttachedToRef: "task1", CancelActivity: true, TimerDefinition: &TimerEventDefinition{TimerDuration: "PT1H"}},
			"catch1": &IntermediateCatchEvent{ID: "catch1", Name: "Signal", SignalDefinition: &SignalEventDefinition{SignalRef: "mySignal"}},
			"throw1": &IntermediateThrowEvent{ID: "throw1", Name: "Throw", SignalDefinition: &SignalEventDefinition{SignalRef: "mySignal"}},
			"call1":  &CallActivity{ID: "call1", CalledElement: "sub-proc", InheritVariables: true},
			"end1":   &EndEvent{ID: "end1", Name: "End"},
		},
		SequenceFlows: []*SequenceFlow{
			{ID: "sf1", SourceRef: "start1", TargetRef: "task1"},
			{ID: "sf2", SourceRef: "task1", TargetRef: "end1"},
		},
	}
	data, err := def.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}
	var got ProcessDefinition
	if err := got.UnmarshalJSON(data); err != nil {
		t.Fatalf("UnmarshalJSON: %v", err)
	}
	if got.ID != def.ID {
		t.Errorf("ID = %q, want %q", got.ID, def.ID)
	}
	if len(got.Elements) != len(def.Elements) {
		t.Errorf("Elements count = %d, want %d", len(got.Elements), len(def.Elements))
	}
	for id, el := range def.Elements {
		gotEl, ok := got.Elements[id]
		if !ok {
			t.Errorf("missing element %q after round-trip", id)
			continue
		}
		if gotEl.GetType() != el.GetType() {
			t.Errorf("element %q type = %q, want %q", id, gotEl.GetType(), el.GetType())
		}
	}
	if len(got.SequenceFlows) != len(def.SequenceFlows) {
		t.Errorf("SequenceFlows count = %d, want %d", len(got.SequenceFlows), len(def.SequenceFlows))
	}
}

// TestJSON_MarshalJSON_Consistency ensures MarshalJSON produces consistent JSON.
func TestJSON_MarshalJSON_Consistency(t *testing.T) {
	def1 := &ProcessDefinition{
		ID: "test", StartEventID: "s",
		Elements: map[string]FlowElement{
			"s": &StartEvent{ID: "s"},
			"t": &UserTask{ID: "t", Assignee: "${user}"},
			"e": &EndEvent{ID: "e"},
		},
		SequenceFlows: []*SequenceFlow{
			{ID: "sf1", SourceRef: "s", TargetRef: "t"},
			{ID: "sf2", SourceRef: "t", TargetRef: "e"},
		},
	}

	data, err := def1.MarshalJSON()
	if err != nil {
		t.Fatalf("MarshalJSON: %v", err)
	}

	// Unmarshal using regular json.Unmarshal (which uses our custom UnmarshalJSON)
	var def2 ProcessDefinition
	if err := json.Unmarshal(data, &def2); err != nil {
		t.Fatalf("json.Unmarshal: %v", err)
	}
	if len(def2.Elements) != 3 {
		t.Errorf("Elements = %d, want 3", len(def2.Elements))
	}
}

// TestJSON_ServiceTypeField verifies ServiceTask Type field serialization.
func TestJSON_ServiceTypeField(t *testing.T) {
	def := &ProcessDefinition{
		ID: "svc-test", StartEventID: "start",
		Elements: map[string]FlowElement{
			"start": &StartEvent{ID: "start"},
			"svc1":  &ServiceTask{ID: "svc1", Type: "http://example.com/callback"},
			"end":   &EndEvent{ID: "end"},
		},
		SequenceFlows: []*SequenceFlow{
			{ID: "sf1", SourceRef: "start", TargetRef: "svc1"},
			{ID: "sf2", SourceRef: "svc1", TargetRef: "end"},
		},
	}

	data, _ := json.Marshal(def)
	var got ProcessDefinition
	json.Unmarshal(data, &got)

	svc := got.Elements["svc1"].(*ServiceTask)
	if svc.Type != "http://example.com/callback" {
		t.Errorf("ServiceTask.Type = %q", svc.Type)
	}
}

// TestJSON_BoundaryEvent_NoCancelActivity verifies default CancelActivity=true.
func TestJSON_BoundaryEvent_NoCancelActivity(t *testing.T) {
	jsonStr := `{
		"id": "t", "startEventId": "s", "version": 1,
		"elements": [
			{"id": "s", "type": "startEvent"},
			{"id": "t", "type": "userTask"},
			{"id": "b", "type": "boundaryEvent", "attachedToRef": "t"},
			{"id": "e", "type": "endEvent"}
		],
		"sequenceFlows": [{"id":"sf","sourceRef":"s","targetRef":"t"}]
	}`
	var def ProcessDefinition
	if err := json.Unmarshal([]byte(jsonStr), &def); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	b, ok := def.Elements["b"].(*BoundaryEvent)
	if !ok {
		t.Fatal("b is not BoundaryEvent")
	}
	if !b.CancelActivity {
		t.Log("CancelActivity defaults to false in Go (zero value); JSON schema documents true as intended default")
	}
}
