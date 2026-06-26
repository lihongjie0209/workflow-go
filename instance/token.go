package instance

import "time"

// TokenState enumerates the states of an execution token.
type TokenState string

const (
	TokenStateActive   TokenState = "active"
	TokenStateConsumed TokenState = "consumed"
)

// Token represents one active path of execution through the process.
// Tokens are created when a flow element fires and are consumed
// when the element completes. Gateway semantics (AND/OR join) are
// implemented by counting tokens at the gateway element.
type Token struct {
	ID                string
	ProcessInstanceID string
	TenantID          string
	CurrentElementID  string // the element this token currently occupies
	State             TokenState
	CreatedAt         time.Time
}

// NewToken creates a new active token at the given element.
func NewToken(id, processInstanceID, currentElementID string) *Token {
	return &Token{
		ID:                id,
		ProcessInstanceID: processInstanceID,
		CurrentElementID:  currentElementID,
		State:             TokenStateActive,
		CreatedAt:         time.Now(),
	}
}
