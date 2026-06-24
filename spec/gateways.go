package spec

// ExclusiveGateway (XOR) routes execution to exactly one outgoing flow.
// Outgoing flows with conditions are evaluated in order;
// the first whose condition evaluates to true is taken.
// If no condition matches, the DefaultFlowID is used.
type ExclusiveGateway struct {
	ID            string
	Name          string
	DefaultFlowID string // fallback when no condition matches
}

func (g *ExclusiveGateway) GetID() string        { return g.ID }
func (g *ExclusiveGateway) GetName() string      { return g.Name }
func (g *ExclusiveGateway) GetType() ElementType { return ElementTypeExclusiveGateway }

// ParallelGateway (AND) synchronizes (join) and splits execution paths.
// - Join: waits for ALL incoming tokens before proceeding.
// - Split: activates ALL outgoing flows simultaneously.
// Whether join or split behavior applies depends on the number of
// incoming vs outgoing sequence flows in the process topology.
type ParallelGateway struct {
	ID   string
	Name string
}

func (g *ParallelGateway) GetID() string        { return g.ID }
func (g *ParallelGateway) GetName() string      { return g.Name }
func (g *ParallelGateway) GetType() ElementType { return ElementTypeParallelGateway }

// InclusiveGateway (OR) evaluates conditions on all outgoing flows
// and activates every flow whose condition evaluates to true.
// If no condition matches, the DefaultFlowID is used.
// When multiple incoming flows exist, it acts as a join,
// waiting for all expected tokens to arrive before proceeding.
type InclusiveGateway struct {
	ID            string
	Name          string
	DefaultFlowID string // fallback when no condition matches
}

func (g *InclusiveGateway) GetID() string        { return g.ID }
func (g *InclusiveGateway) GetName() string      { return g.Name }
func (g *InclusiveGateway) GetType() ElementType { return ElementTypeInclusiveGateway }
