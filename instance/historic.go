package instance

import (
	"time"

	"github.com/lihongjie/workflow-go/spec"
)

// HistoricActivityInstance records a completed activity with a snapshot
// of process variables at the time of completion.
type HistoricActivityInstance struct {
	ID                string
	ProcessInstanceID string
	TenantID          string
	ActivityID        string
	ActivityType      spec.ElementType
	Variables         map[string]any
	StartedAt         time.Time
	CompletedAt       time.Time
}
