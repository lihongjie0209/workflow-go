// Package spec defines the workflow process definition types.
// It is the foundation of the workflow engine, analogous to BPMN 2.0 definitions
// but designed as native Go types.
package spec

import (
	"encoding/json"
	"errors"
	"fmt"
)

// ElementType identifies the kind of a flow element.
type ElementType string

const (
	ElementTypeStartEvent       ElementType = "startEvent"
	ElementTypeEndEvent         ElementType = "endEvent"
	ElementTypeUserTask         ElementType = "userTask"
	ElementTypeServiceTask      ElementType = "serviceTask"
	ElementTypeExclusiveGateway ElementType = "exclusiveGateway"
	ElementTypeParallelGateway  ElementType = "parallelGateway"
	ElementTypeInclusiveGateway ElementType = "inclusiveGateway"
	ElementTypeBoundaryEvent    ElementType = "boundaryEvent"
	ElementTypeIntermediateCatchEvent  ElementType = "intermediateCatchEvent"
	ElementTypeIntermediateThrowEvent  ElementType = "intermediateThrowEvent"
	ElementTypeCallActivity         ElementType = "callActivity"
)

// FlowElement is the interface implemented by every node in a process definition.
type FlowElement interface {
	GetID() string
	GetName() string
	GetType() ElementType
}

// ProcessDefinition is the blueprint for a workflow process.
// It contains all elements and sequence flows that define the process topology.
type ProcessDefinition struct {
	ID       string `json:"id"`
	Name     string `json:"name,omitempty"`
	Key      string `json:"key,omitempty"`
	Version  int    `json:"version"`
	Elements map[string]FlowElement `json:"-"` // custom JSON below

	SequenceFlows []*SequenceFlow `json:"sequenceFlows,omitempty"`
	StartEventID  string          `json:"startEventId"`
}

// elementRecord is a JSON-intermediary for a single FlowElement.
type elementRecord struct {
	ID   string      `json:"id"`
	Name string      `json:"name,omitempty"`
	Type ElementType `json:"type"`
	// Gateway-specific fields
	DefaultFlowID string `json:"defaultFlowId,omitempty"`
	// UserTask-specific fields
	Assignee            string                `json:"assignee,omitempty"`
	CandidateUsers      []string              `json:"candidateUsers,omitempty"`
	CandidateGroup      string                `json:"candidateGroup,omitempty"`
	FormKey             string                `json:"formKey,omitempty"`
	LoopCharacteristics *LoopCharacteristics  `json:"loopCharacteristics,omitempty"`
	// ServiceTask-specific fields
	ServiceType string `json:"serviceType,omitempty"`
		// Event-specific fields
		AttachedToRef     string                  `json:"attachedToRef,omitempty"`
		CancelActivity    bool                    `json:"cancelActivity,omitempty"`
		TimerDefinition   *TimerEventDefinition   `json:"timerDefinition,omitempty"`
		SignalDefinition  *SignalEventDefinition  `json:"signalDefinition,omitempty"`
		MessageDefinition *MessageEventDefinition `json:"messageDefinition,omitempty"`
		// CallActivity fields
		CalledElement    string `json:"calledElement,omitempty"`
		InheritVariables bool   `json:"inheritVariables,omitempty"`
}

// MarshalJSON implements json.Marshaler.
func (pd *ProcessDefinition) MarshalJSON() ([]byte, error) {
	type alias ProcessDefinition
	a := struct {
		*alias
		Elements []elementRecord `json:"elements"`
	}{alias: (*alias)(pd)}

	a.Elements = make([]elementRecord, 0, len(pd.Elements))
	for _, el := range pd.Elements {
		rec := elementRecord{ID: el.GetID(), Name: el.GetName(), Type: el.GetType()}
		switch v := el.(type) {
		case *ExclusiveGateway:
			rec.DefaultFlowID = v.DefaultFlowID
		case *InclusiveGateway:
			rec.DefaultFlowID = v.DefaultFlowID
		case *UserTask:
			rec.Assignee = v.Assignee
			rec.CandidateUsers = v.CandidateUsers
			rec.CandidateGroup = v.CandidateGroup
			rec.FormKey = v.FormKey
			rec.LoopCharacteristics = v.LoopCharacteristics
		case *ServiceTask:
			rec.ServiceType = v.Type
		case *BoundaryEvent:
			rec.AttachedToRef = v.AttachedToRef
			rec.CancelActivity = v.CancelActivity
			rec.TimerDefinition = v.TimerDefinition
			rec.SignalDefinition = v.SignalDefinition
			rec.MessageDefinition = v.MessageDefinition
		case *IntermediateCatchEvent:
			rec.TimerDefinition = v.TimerDefinition
			rec.SignalDefinition = v.SignalDefinition
			rec.MessageDefinition = v.MessageDefinition
		case *IntermediateThrowEvent:
			rec.SignalDefinition = v.SignalDefinition
			rec.MessageDefinition = v.MessageDefinition
		case *CallActivity:
			rec.CalledElement = v.CalledElement
			rec.InheritVariables = v.InheritVariables
		}
		a.Elements = append(a.Elements, rec)
	}

	return json.Marshal(a)
}

// UnmarshalJSON implements json.Unmarshaler.
func (pd *ProcessDefinition) UnmarshalJSON(data []byte) error {
	type alias ProcessDefinition
	a := struct {
		*alias
		Elements []elementRecord `json:"elements"`
	}{alias: (*alias)(pd)}

	if err := json.Unmarshal(data, &a); err != nil {
		return err
	}

	pd.Elements = make(map[string]FlowElement, len(a.Elements))
	for _, rec := range a.Elements {
		var el FlowElement
		switch rec.Type {
		case ElementTypeStartEvent:
			el = &StartEvent{ID: rec.ID, Name: rec.Name}
		case ElementTypeEndEvent:
			el = &EndEvent{ID: rec.ID, Name: rec.Name}
		case ElementTypeUserTask:
			el = &UserTask{
				ID: rec.ID, Name: rec.Name,
				Assignee: rec.Assignee, CandidateUsers: rec.CandidateUsers,
				CandidateGroup: rec.CandidateGroup, FormKey: rec.FormKey,
				LoopCharacteristics: rec.LoopCharacteristics,
			}
		case ElementTypeServiceTask:
			el = &ServiceTask{ID: rec.ID, Name: rec.Name, Type: rec.ServiceType}
		case ElementTypeExclusiveGateway:
			el = &ExclusiveGateway{ID: rec.ID, Name: rec.Name, DefaultFlowID: rec.DefaultFlowID}
		case ElementTypeParallelGateway:
			el = &ParallelGateway{ID: rec.ID, Name: rec.Name}
		case ElementTypeInclusiveGateway:
			el = &InclusiveGateway{ID: rec.ID, Name: rec.Name, DefaultFlowID: rec.DefaultFlowID}
		case ElementTypeBoundaryEvent:
			el = &BoundaryEvent{
				ID: rec.ID, Name: rec.Name,
				AttachedToRef: rec.AttachedToRef, CancelActivity: rec.CancelActivity,
				TimerDefinition: rec.TimerDefinition, SignalDefinition: rec.SignalDefinition,
				MessageDefinition: rec.MessageDefinition,
			}
		case ElementTypeIntermediateCatchEvent:
			el = &IntermediateCatchEvent{
				ID: rec.ID, Name: rec.Name,
				TimerDefinition: rec.TimerDefinition, SignalDefinition: rec.SignalDefinition,
				MessageDefinition: rec.MessageDefinition,
			}
		case ElementTypeIntermediateThrowEvent:
			el = &IntermediateThrowEvent{
				ID: rec.ID, Name: rec.Name,
				SignalDefinition: rec.SignalDefinition, MessageDefinition: rec.MessageDefinition,
			}
		case ElementTypeCallActivity:
			el = &CallActivity{ID: rec.ID, Name: rec.Name, CalledElement: rec.CalledElement, InheritVariables: rec.InheritVariables}
		default:
			return fmt.Errorf("spec: unknown element type %q", rec.Type)
		}
		pd.Elements[rec.ID] = el
	}
	return nil
}

// ErrInvalidDefinition is returned when a process definition fails validation.
var ErrInvalidDefinition = errors.New("invalid process definition")

// Validate checks structural integrity of the process definition.
func (pd *ProcessDefinition) Validate() error {
	if pd.ID == "" {
		return fmt.Errorf("%w: id is required", ErrInvalidDefinition)
	}
	if pd.StartEventID == "" {
		return fmt.Errorf("%w: StartEventID is required", ErrInvalidDefinition)
	}
	if len(pd.Elements) == 0 {
		return fmt.Errorf("%w: at least one element is required", ErrInvalidDefinition)
	}

	startEvent, ok := pd.Elements[pd.StartEventID]
	if !ok {
		return fmt.Errorf("%w: StartEventID %q not found in Elements", ErrInvalidDefinition, pd.StartEventID)
	}
	if startEvent.GetType() != ElementTypeStartEvent {
		return fmt.Errorf("%w: element %q is %s, not startEvent", ErrInvalidDefinition, pd.StartEventID, startEvent.GetType())
	}

	for id, el := range pd.Elements {
		if id != el.GetID() {
			return fmt.Errorf("%w: element map key %q does not match element ID %q", ErrInvalidDefinition, id, el.GetID())
		}
	}

	for _, sf := range pd.SequenceFlows {
		if _, ok := pd.Elements[sf.SourceRef]; !ok {
			return fmt.Errorf("%w: sequence flow %q source %q not found in Elements", ErrInvalidDefinition, sf.ID, sf.SourceRef)
		}
		if _, ok := pd.Elements[sf.TargetRef]; !ok {
			return fmt.Errorf("%w: sequence flow %q target %q not found in Elements", ErrInvalidDefinition, sf.ID, sf.TargetRef)
		}
	}

	outgoing := FindOutgoingFlows(pd.SequenceFlows, pd.StartEventID)
	if len(outgoing) == 0 {
		return fmt.Errorf("%w: start event %q has no outgoing sequence flow", ErrInvalidDefinition, pd.StartEventID)
	}

	hasEnd := false
	for _, el := range pd.Elements {
		if el.GetType() == ElementTypeEndEvent {
			hasEnd = true
			break
		}
	}
	if !hasEnd {
		return fmt.Errorf("%w: at least one endEvent is required", ErrInvalidDefinition)
	}

	return nil
}

// FindOutgoingFlows returns all sequence flows whose source is the given element ID.
func FindOutgoingFlows(flows []*SequenceFlow, elementID string) []*SequenceFlow {
	var result []*SequenceFlow
	for _, sf := range flows {
		if sf.SourceRef == elementID {
			result = append(result, sf)
		}
	}
	return result
}

// FindIncomingFlows returns all sequence flows whose target is the given element ID.
func FindIncomingFlows(flows []*SequenceFlow, elementID string) []*SequenceFlow {
	var result []*SequenceFlow
	for _, sf := range flows {
		if sf.TargetRef == elementID {
			result = append(result, sf)
		}
	}
	return result
}