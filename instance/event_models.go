package instance

import "time"

// TimerJob represents a scheduled timer event.
type TimerJob struct {
	ID                string
	ProcessInstanceID string
	TenantID          string
	ElementID         string // the boundary/catch event this timer belongs to
	DueAt             time.Time
	Fired             bool
}

// SignalSubscription represents a subscription for a named signal.
type SignalSubscription struct {
	ID                string
	ProcessInstanceID string
	TenantID          string
	ElementID         string
	SignalRef         string
}
