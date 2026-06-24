// Package memstore provides an in-memory implementation of the storage.Store interface.
// It uses sync.RWMutex for concurrent access safety and is suitable for testing
// and single-instance deployments.
package memstore

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"
)

// Store implements storage.Store using synchronized in-memory maps.
type Store struct {
	mu sync.RWMutex

	definitions  map[string]*spec.ProcessDefinition
	instances    map[string]*instance.ProcessInstance
	activities   map[string]*instance.ActivityInstance
	tokens       map[string]*instance.Token
	variables    map[string]map[string]any // processInstanceID -> name -> value
	historicActivities map[string]*instance.HistoricActivityInstance
	timerJobsMap     map[string]*instance.TimerJob
	signalSubsMap    map[string]*instance.SignalSubscription
}

// New creates a new empty in-memory store.
func New() *Store {
	return &Store{
		definitions: make(map[string]*spec.ProcessDefinition),
		instances:   make(map[string]*instance.ProcessInstance),
		activities:  make(map[string]*instance.ActivityInstance),
		tokens:      make(map[string]*instance.Token),
		variables:   make(map[string]map[string]any),
		historicActivities: make(map[string]*instance.HistoricActivityInstance),
			timerJobsMap:       make(map[string]*instance.TimerJob),
			signalSubsMap:      make(map[string]*instance.SignalSubscription),
	}
}

// Close implements io.Closer.
func (s *Store) Close() error {
	return nil
}

// --- ProcessDefinitionStore ---

func (s *Store) CreateProcessDefinition(_ context.Context, def *spec.ProcessDefinition) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.definitions[def.ID]; exists {
		return fmt.Errorf("memstore: process definition %q already exists", def.ID)
	}
	s.definitions[def.ID] = def
	return nil
}

func (s *Store) GetProcessDefinition(_ context.Context, id string) (*spec.ProcessDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	def, exists := s.definitions[id]
	if !exists {
		return nil, fmt.Errorf("memstore: process definition %q not found: %w", id, storage.ErrNotFound)
	}
	return def, nil
}

func (s *Store) GetProcessDefinitionByKeyVersion(_ context.Context, key string, version int) (*spec.ProcessDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for _, def := range s.definitions {
		if def.Key == key && def.Version == version {
			return def, nil
		}
	}
	return nil, fmt.Errorf("memstore: process definition %q version %d not found", key, version)
}

func (s *Store) ListProcessDefinitions(_ context.Context) ([]*spec.ProcessDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]*spec.ProcessDefinition, 0, len(s.definitions))
	for _, def := range s.definitions {
		result = append(result, def)
	}
	return result, nil
}

func (s *Store) DeleteProcessDefinition(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.definitions[id]; !exists {
		return fmt.Errorf("memstore: process definition %q not found: %w", id, storage.ErrNotFound)
	}
	delete(s.definitions, id)
	return nil
}

// --- ProcessInstanceStore ---

func (s *Store) CreateProcessInstance(_ context.Context, pi *instance.ProcessInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.instances[pi.ID]; exists {
		return fmt.Errorf("memstore: process instance %q already exists", pi.ID)
	}
	s.instances[pi.ID] = pi
	return nil
}

func (s *Store) UpdateProcessInstance(_ context.Context, pi *instance.ProcessInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.instances[pi.ID]; !exists {
		return fmt.Errorf("memstore: process instance %q not found: %w", pi.ID, storage.ErrNotFound)
	}
	s.instances[pi.ID] = pi
	return nil
}

func (s *Store) GetProcessInstance(_ context.Context, id string) (*instance.ProcessInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	pi, exists := s.instances[id]
	if !exists {
		return nil, fmt.Errorf("memstore: process instance %q not found: %w", id, storage.ErrNotFound)
	}
	return pi, nil
}

func (s *Store) ListProcessInstances(_ context.Context, defID string) ([]*instance.ProcessInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*instance.ProcessInstance
	for _, pi := range s.instances {
		if pi.ProcessDefinitionID == defID {
			result = append(result, pi)
		}
	}
	return result, nil
}

// --- ActivityInstanceStore ---

func (s *Store) CreateActivityInstance(_ context.Context, ai *instance.ActivityInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.activities[ai.ID]; exists {
		return fmt.Errorf("memstore: activity instance %q already exists", ai.ID)
	}
	s.activities[ai.ID] = ai
	return nil
}

func (s *Store) UpdateActivityInstance(_ context.Context, ai *instance.ActivityInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.activities[ai.ID]; !exists {
		return fmt.Errorf("memstore: activity instance %q not found: %w", ai.ID, storage.ErrNotFound)
	}
	s.activities[ai.ID] = ai
	return nil
}

func (s *Store) GetActivityInstance(_ context.Context, id string) (*instance.ActivityInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ai, exists := s.activities[id]
	if !exists {
		return nil, fmt.Errorf("memstore: activity instance %q not found: %w", id, storage.ErrNotFound)
	}
	return ai, nil
}

func (s *Store) ListActiveActivities(_ context.Context, processInstanceID string) ([]*instance.ActivityInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*instance.ActivityInstance
	for _, ai := range s.activities {
		if ai.ProcessInstanceID == processInstanceID && ai.State == instance.ActivityStateActive {
			result = append(result, ai)
		}
	}
	return result, nil
}

func (s *Store) ListActivitiesByProcessInstance(_ context.Context, processInstanceID string) ([]*instance.ActivityInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*instance.ActivityInstance
	for _, ai := range s.activities {
		if ai.ProcessInstanceID == processInstanceID {
			result = append(result, ai)
		}
	}
	return result, nil
}

func (s *Store) ListActivitiesByLoopID(_ context.Context, processInstanceID, loopID string) ([]*instance.ActivityInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*instance.ActivityInstance
	for _, ai := range s.activities {
		if ai.ProcessInstanceID == processInstanceID && ai.MultiInstanceLoopID == loopID {
			result = append(result, ai)
		}
	}
	return result, nil
}

// --- TokenStore ---

func (s *Store) CreateToken(_ context.Context, t *instance.Token) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tokens[t.ID]; exists {
		return fmt.Errorf("memstore: token %q already exists", t.ID)
	}
	s.tokens[t.ID] = t
	return nil
}

func (s *Store) UpdateToken(_ context.Context, t *instance.Token) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tokens[t.ID]; !exists {
		return fmt.Errorf("memstore: token %q not found: %w", t.ID, storage.ErrNotFound)
	}
	s.tokens[t.ID] = t
	return nil
}

func (s *Store) GetToken(_ context.Context, id string) (*instance.Token, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	t, exists := s.tokens[id]
	if !exists {
		return nil, fmt.Errorf("memstore: token %q not found: %w", id, storage.ErrNotFound)
	}
	return t, nil
}

func (s *Store) ListActiveTokens(_ context.Context, processInstanceID string) ([]*instance.Token, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*instance.Token
	for _, t := range s.tokens {
		if t.ProcessInstanceID == processInstanceID && t.State == instance.TokenStateActive {
			result = append(result, t)
		}
	}
	return result, nil
}

func (s *Store) DeleteToken(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.tokens[id]; !exists {
		return fmt.Errorf("memstore: token %q not found: %w", id, storage.ErrNotFound)
	}
	delete(s.tokens, id)
	return nil
}

// --- VariableStore ---

func (s *Store) SetVariable(_ context.Context, processInstanceID, name string, value any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.variables[processInstanceID]; !exists {
		s.variables[processInstanceID] = make(map[string]any)
	}
	s.variables[processInstanceID][name] = value
	return nil
}

func (s *Store) GetVariable(_ context.Context, processInstanceID, name string) (any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vars, exists := s.variables[processInstanceID]
	if !exists {
		return nil, fmt.Errorf("memstore: variable %q not found for instance %q: %w", name, processInstanceID, storage.ErrNotFound)
	}
	val, exists := vars[name]
	if !exists {
		return nil, fmt.Errorf("memstore: variable %q not found for instance %q: %w", name, processInstanceID, storage.ErrNotFound)
	}
	return val, nil
}

func (s *Store) GetAllVariables(_ context.Context, processInstanceID string) (map[string]any, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	vars, exists := s.variables[processInstanceID]
	if !exists {
		return make(map[string]any), nil
	}
	// Return a copy to avoid data races.
	result := make(map[string]any, len(vars))
	for k, v := range vars {
		result[k] = v
	}
	return result, nil
}

func (s *Store) DeleteVariable(_ context.Context, processInstanceID, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	vars, exists := s.variables[processInstanceID]
	if !exists {
		return fmt.Errorf("memstore: variable %q not found for instance %q: %w", name, processInstanceID, storage.ErrNotFound)
	}
	if _, exists := vars[name]; !exists {
		return fmt.Errorf("memstore: variable %q not found for instance %q: %w", name, processInstanceID, storage.ErrNotFound)
	}
	delete(vars, name)
	return nil
}

// Compile-time check that *Store implements storage.Store.
var _ storage.Store = (*Store)(nil)

// --- HistoricActivityInstanceStore ---

func (s *Store) CreateHistoricActivityInstance(_ context.Context, hai *instance.HistoricActivityInstance) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.historicActivities == nil {
		s.historicActivities = make(map[string]*instance.HistoricActivityInstance)
	}
	s.historicActivities[hai.ID] = hai
	return nil
}

func (s *Store) ListHistoricByProcessInstance(_ context.Context, processInstanceID string) ([]*instance.HistoricActivityInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*instance.HistoricActivityInstance
	for _, hai := range s.historicActivities {
		if hai.ProcessInstanceID == processInstanceID {
			result = append(result, hai)
		}
	}
	return result, nil
}

// --- Additional methods for ProcessDefinitionStore ---

func (s *Store) GetLatestProcessDefinitionByKey(_ context.Context, key string) (*spec.ProcessDefinition, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var latest *spec.ProcessDefinition
	for _, def := range s.definitions {
		if def.Key == key {
			if latest == nil || def.Version > latest.Version {
				latest = def
			}
		}
	}
	if latest == nil {
		return nil, fmt.Errorf("memstore: process definition with key %q not found: %w", key, storage.ErrNotFound)
	}
	return latest, nil
}

// --- Additional methods for ProcessInstanceStore ---

func (s *Store) ListCompletedProcessInstances(_ context.Context, limit int) ([]*instance.ProcessInstance, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*instance.ProcessInstance
	for _, pi := range s.instances {
		if pi.State == instance.ProcessInstanceStateCompleted {
			result = append(result, pi)
			if limit > 0 && len(result) >= limit {
				break
			}
		}
	}
	return result, nil
}

// --- TimerJobStore ---

func (s *Store) timerJobs() map[string]*instance.TimerJob {
	if s.timerJobsMap == nil {
		s.timerJobsMap = make(map[string]*instance.TimerJob)
	}
	return s.timerJobsMap
}

func (s *Store) CreateTimerJob(_ context.Context, job *instance.TimerJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.timerJobs()
	m[job.ID] = job
	return nil
}

func (s *Store) UpdateTimerJob(_ context.Context, job *instance.TimerJob) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.timerJobs()
	m[job.ID] = job
	return nil
}

func (s *Store) ListDueTimerJobs(_ context.Context, before time.Time) ([]*instance.TimerJob, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*instance.TimerJob
	for _, job := range s.timerJobs() {
		if !job.Fired && job.DueAt.Before(before) {
			result = append(result, job)
		}
	}
	return result, nil
}

func (s *Store) DeleteTimerJob(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.timerJobs(), id)
	return nil
}

func (s *Store) DeleteTimerJobsByInstance(_ context.Context, processInstanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.timerJobs()
	for id, job := range m {
		if job.ProcessInstanceID == processInstanceID {
			delete(m, id)
		}
	}
	return nil
}

// --- SignalSubscriptionStore ---

func (s *Store) signalSubscriptions() map[string]*instance.SignalSubscription {
	if s.signalSubsMap == nil {
		s.signalSubsMap = make(map[string]*instance.SignalSubscription)
	}
	return s.signalSubsMap
}

func (s *Store) CreateSignalSubscription(_ context.Context, sub *instance.SignalSubscription) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.signalSubscriptions()
	m[sub.ID] = sub
	return nil
}

func (s *Store) ListSignalSubscriptions(_ context.Context, signalRef string) ([]*instance.SignalSubscription, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []*instance.SignalSubscription
	for _, sub := range s.signalSubscriptions() {
		if sub.SignalRef == signalRef {
			result = append(result, sub)
		}
	}
	return result, nil
}

func (s *Store) DeleteSignalSubscription(_ context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.signalSubscriptions(), id)
	return nil
}

func (s *Store) DeleteSubscriptionsByInstance(_ context.Context, processInstanceID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	m := s.signalSubscriptions()
	for id, sub := range m {
		if sub.ProcessInstanceID == processInstanceID {
			delete(m, id)
		}
	}
	return nil
}
