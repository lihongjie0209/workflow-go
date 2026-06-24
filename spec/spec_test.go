package spec

import (
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
