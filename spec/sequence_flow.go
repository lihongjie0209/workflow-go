package spec

// SequenceFlow connects two flow elements.
// A condition expression is optional; it is evaluated at runtime on
// gateway-outgoing flows to determine whether the flow should be taken.
type SequenceFlow struct {
	ID                  string  `json:"id"`
	Name                string  `json:"name,omitempty"`
	SourceRef           string  `json:"sourceRef"`
	TargetRef           string  `json:"targetRef"`
	ConditionExpression *string `json:"conditionExpression,omitempty"` // nil means unconditional
}

// HasCondition returns true when a condition expression is set.
func (sf *SequenceFlow) HasCondition() bool {
	return sf.ConditionExpression != nil
}
