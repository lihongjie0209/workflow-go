// Package sqlstore provides a SQLite-backed implementation of the storage.Store
// interface using modernc.org/sqlite (pure Go, no CGO dependency).
package sqlstore

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/lihongjie/workflow-go/instance"
	"github.com/lihongjie/workflow-go/spec"
	"github.com/lihongjie/workflow-go/storage"

	_ "modernc.org/sqlite"
)

// Store implements storage.Store backed by a SQLite database.
type Store struct {
	db *sql.DB
}

// Option configures the SQLite store.
type Option func(*Store)

// WithMemory configures the store to use an in-memory SQLite database.
// Useful for testing.
func WithMemory() Option {
	return func(s *Store) {
		var err error
		s.db, err = sql.Open("sqlite", ":memory:")
		if err != nil {
			panic(fmt.Sprintf("sqlstore: failed to open in-memory db: %v", err))
		}
	}
}

// WithDBPath configures the store to use a file-based SQLite database.
func WithDBPath(path string) Option {
	return func(s *Store) {
		var err error
		s.db, err = sql.Open("sqlite", path)
		if err != nil {
			panic(fmt.Sprintf("sqlstore: failed to open db at %q: %v", path, err))
		}
	}
}

// New creates a new SQLite store with the given options.
// Defaults to in-memory database if no options are provided.
func New(opts ...Option) *Store {
	s := &Store{}
	if len(opts) == 0 {
		opts = append(opts, WithMemory())
	}
	for _, opt := range opts {
		opt(s)
	}
	if err := s.init(); err != nil {
		panic(fmt.Sprintf("sqlstore: init failed: %v", err))
	}
	return s
}

// Close implements io.Closer.
func (s *Store) Close() error {
	return s.db.Close()
}

func (s *Store) init() error {
	schema := `
	CREATE TABLE IF NOT EXISTS process_definitions (
		id TEXT PRIMARY KEY,
		name TEXT NOT NULL DEFAULT '',
		key TEXT NOT NULL DEFAULT '',
		version INTEGER NOT NULL DEFAULT 1,
		data BLOB NOT NULL,
		created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS process_instances (
		id TEXT PRIMARY KEY,
		def_id TEXT NOT NULL,
		state TEXT NOT NULL DEFAULT 'running',
		variables TEXT NOT NULL DEFAULT '{}',
		started_at TIMESTAMP NOT NULL,
		ended_at TIMESTAMP,
		parent_process_instance_id TEXT NOT NULL DEFAULT '',
			parent_activity_id TEXT NOT NULL DEFAULT ''
	);

	CREATE TABLE IF NOT EXISTS activity_instances (
		id TEXT PRIMARY KEY,
		process_instance_id TEXT NOT NULL,
		activity_id TEXT NOT NULL,
		activity_type TEXT NOT NULL,
		assignee TEXT NOT NULL DEFAULT '',
		adhoc_parent_id TEXT NOT NULL DEFAULT '',
		state TEXT NOT NULL DEFAULT 'active',
		claim_time TIMESTAMP,
		completed_time TIMESTAMP,
		multi_instance_loop TEXT NOT NULL DEFAULT '',
		loop_counter INTEGER NOT NULL DEFAULT 0,
			expire_time TIMESTAMP,
			term_mode INTEGER NOT NULL DEFAULT 0
	);

	CREATE TABLE IF NOT EXISTS tokens (
		id TEXT PRIMARY KEY,
		process_instance_id TEXT NOT NULL,
		current_element_id TEXT NOT NULL,
		state TEXT NOT NULL DEFAULT 'active',
		created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
	);

	CREATE TABLE IF NOT EXISTS variables (
		process_instance_id TEXT NOT NULL,
		name TEXT NOT NULL,
		value TEXT NOT NULL DEFAULT '',
		PRIMARY KEY (process_instance_id, name)
	);

		CREATE TABLE IF NOT EXISTS historic_activity_instances (
			id TEXT PRIMARY KEY,
			process_instance_id TEXT NOT NULL,
			activity_id TEXT NOT NULL,
			activity_type TEXT NOT NULL,
			variables TEXT NOT NULL DEFAULT '{}',
			started_at TIMESTAMP,
			completed_at TIMESTAMP
		);

		CREATE TABLE IF NOT EXISTS timer_jobs (
			id TEXT PRIMARY KEY,
			process_instance_id TEXT NOT NULL,
			element_id TEXT NOT NULL,
			due_at TIMESTAMP NOT NULL,
			fired INTEGER NOT NULL DEFAULT 0
		);

		CREATE TABLE IF NOT EXISTS signal_subscriptions (
			id TEXT PRIMARY KEY,
			process_instance_id TEXT NOT NULL,
			element_id TEXT NOT NULL,
			signal_ref TEXT NOT NULL
		);
	`
	_, err := s.db.Exec(schema)
	return err
}

// --- ProcessDefinitionStore ---

func (s *Store) CreateProcessDefinition(ctx context.Context, def *spec.ProcessDefinition) error {
	data, err := serializeProcessDefinition(def)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO process_definitions (id, name, key, version, data) VALUES (?, ?, ?, ?, ?)`,
		def.ID, def.Name, def.Key, def.Version, data)
	if err != nil {
		return fmt.Errorf("sqlstore: create definition %q: %w", def.ID, err)
	}
	return nil
}

func (s *Store) GetProcessDefinition(ctx context.Context, id string) (*spec.ProcessDefinition, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data FROM process_definitions WHERE id = ?`, id).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("sqlstore: process definition %q not found: %w", id, storage.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return deserializeProcessDefinition(data)
}

func (s *Store) GetProcessDefinitionByKeyVersion(ctx context.Context, key string, version int) (*spec.ProcessDefinition, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx,
		`SELECT data FROM process_definitions WHERE key = ? AND version = ?`, key, version).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("sqlstore: process definition %q version %d not found: %w", key, version, storage.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return deserializeProcessDefinition(data)
}

func (s *Store) ListProcessDefinitions(ctx context.Context) ([]*spec.ProcessDefinition, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT data FROM process_definitions`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []*spec.ProcessDefinition
	for rows.Next() {
		var data []byte
		if err := rows.Scan(&data); err != nil {
			return nil, err
		}
		def, err := deserializeProcessDefinition(data)
		if err != nil {
			return nil, err
		}
		result = append(result, def)
	}
	return result, rows.Err()
}

func (s *Store) DeleteProcessDefinition(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM process_definitions WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("sqlstore: process definition %q not found: %w", id, storage.ErrNotFound)
	}
	return nil
}

// --- ProcessInstanceStore ---

func (s *Store) CreateProcessInstance(ctx context.Context, pi *instance.ProcessInstance) error {
	vars, err := json.Marshal(pi.Variables)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO process_instances (id, def_id, state, variables, started_at, parent_process_instance_id, parent_activity_id) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		pi.ID, pi.ProcessDefinitionID, string(pi.State), string(vars), pi.StartedAt, pi.ParentProcessInstanceID, pi.ParentActivityID)
	if err != nil {
		return fmt.Errorf("sqlstore: create instance %q: %w", pi.ID, err)
	}
	return nil
}

func (s *Store) UpdateProcessInstance(ctx context.Context, pi *instance.ProcessInstance) error {
	vars, err := json.Marshal(pi.Variables)
	if err != nil {
		return err
	}
	res, err := s.db.ExecContext(ctx,
		`UPDATE process_instances SET state = ?, variables = ?, ended_at = ? WHERE id = ?`,
		string(pi.State), string(vars), pi.EndedAt, pi.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("sqlstore: process instance %q not found: %w", pi.ID, storage.ErrNotFound)
	}
	return nil
}

func (s *Store) GetProcessInstance(ctx context.Context, id string) (*instance.ProcessInstance, error) {
	var (
		defID, stateStr, varsJSON, parentPIID, parentActID string
		startedAt       time.Time
		endedAt         *time.Time
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE id = ?`, id).
		Scan(&defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("sqlstore: process instance %q not found: %w", id, storage.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}

	vars := make(map[string]any)
	if varsJSON != "" {
		if err := json.Unmarshal([]byte(varsJSON), &vars); err != nil {
			return nil, err
		}
	}

	return &instance.ProcessInstance{
		ID:                  id,
		ProcessDefinitionID: defID,
		State:               instance.ProcessInstanceState(stateStr),
		Variables:           vars,
		StartedAt:           startedAt,
		EndedAt:             endedAt,
			ParentProcessInstanceID: parentPIID,
			ParentActivityID:        parentActID,
	}, nil
}

func (s *Store) ListProcessInstances(ctx context.Context, defID string) ([]*instance.ProcessInstance, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE def_id = ?`, defID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	return scanProcessInstances(rows)
}

// --- ActivityInstanceStore ---

func (s *Store) CreateActivityInstance(ctx context.Context, ai *instance.ActivityInstance) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO activity_instances (id, process_instance_id, activity_id, activity_type, assignee, adhoc_parent_id, state, multi_instance_loop, loop_counter, expire_time, term_mode) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		ai.ID, ai.ProcessInstanceID, ai.ActivityID, string(ai.ActivityType), ai.Assignee, ai.AdhocParentID, string(ai.State), ai.MultiInstanceLoopID, ai.LoopCounter, ai.ExpireTime, ai.TermMode)
	if err != nil {
		return fmt.Errorf("sqlstore: create activity instance %q: %w", ai.ID, err)
	}
	return nil
}

func (s *Store) UpdateActivityInstance(ctx context.Context, ai *instance.ActivityInstance) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE activity_instances SET state = ?, assignee = ?, adhoc_parent_id = ?, claim_time = ?, completed_time = ?, multi_instance_loop = ?, loop_counter = ?, expire_time = ?, term_mode = ? WHERE id = ?`,
		string(ai.State), ai.Assignee, ai.AdhocParentID, ai.ClaimTime, ai.CompletedTime, ai.MultiInstanceLoopID, ai.LoopCounter, ai.ExpireTime, ai.TermMode, ai.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("sqlstore: activity instance %q not found: %w", ai.ID, storage.ErrNotFound)
	}
	return nil
}

func (s *Store) GetActivityInstance(ctx context.Context, id string) (*instance.ActivityInstance, error) {
	var (
		activityID, activityTypeStr, stateStr, piID string
		claimTime, completedTime                    *time.Time
		loopID                                      string
		loopCounter                                 int
		assigneeVal                                 string
		adhocParentID                             string
		expireTime                                  *time.Time
		termMode                                    int
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT id, process_instance_id, activity_id, activity_type, assignee, adhoc_parent_id, state, claim_time, completed_time, multi_instance_loop, loop_counter, expire_time, term_mode FROM activity_instances WHERE id = ?`, id).
		Scan(&id, &piID, &activityID, &activityTypeStr, &assigneeVal, &adhocParentID, &stateStr, &claimTime, &completedTime, &loopID, &loopCounter, &expireTime, &termMode)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("sqlstore: activity instance %q not found: %w", id, storage.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}

	return &instance.ActivityInstance{
		ID:                id,
		ProcessInstanceID: piID,
		ActivityID:        activityID,
		ActivityType:      spec.ElementType(activityTypeStr),
		State:             instance.ActivityInstanceState(stateStr),
		ClaimTime:         claimTime,
		CompletedTime:     completedTime,
		Assignee:            assigneeVal,
		AdhocParentID:       adhocParentID,
		MultiInstanceLoopID: loopID,
			LoopCounter:         loopCounter,
			ExpireTime:          expireTime,
			TermMode:            termMode,
	}, nil
}

func (s *Store) ListActiveActivities(ctx context.Context, processInstanceID string) ([]*instance.ActivityInstance, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, process_instance_id, activity_id, activity_type, assignee, adhoc_parent_id, state, claim_time, completed_time, multi_instance_loop, loop_counter, expire_time, term_mode FROM activity_instances WHERE process_instance_id = ? AND state = 'active'`,
		processInstanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanActivityInstances(rows)
}

func (s *Store) ListActivitiesByProcessInstance(ctx context.Context, processInstanceID string) ([]*instance.ActivityInstance, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, process_instance_id, activity_id, activity_type, assignee, adhoc_parent_id, state, claim_time, completed_time, multi_instance_loop, loop_counter, expire_time, term_mode FROM activity_instances WHERE process_instance_id = ?`,
		processInstanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanActivityInstances(rows)
}

func (s *Store) ListActivitiesByLoopID(ctx context.Context, processInstanceID, loopID string) ([]*instance.ActivityInstance, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, process_instance_id, activity_id, activity_type, assignee, adhoc_parent_id, state, claim_time, completed_time, multi_instance_loop, loop_counter, expire_time, term_mode FROM activity_instances WHERE process_instance_id = ? AND multi_instance_loop = ?`,
		processInstanceID, loopID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanActivityInstances(rows)
}

// --- TokenStore ---

func (s *Store) CreateToken(ctx context.Context, t *instance.Token) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO tokens (id, process_instance_id, current_element_id, state) VALUES (?, ?, ?, ?)`,
		t.ID, t.ProcessInstanceID, t.CurrentElementID, string(t.State))
	if err != nil {
		return fmt.Errorf("sqlstore: create token %q: %w", t.ID, err)
	}
	return nil
}

func (s *Store) UpdateToken(ctx context.Context, t *instance.Token) error {
	res, err := s.db.ExecContext(ctx,
		`UPDATE tokens SET current_element_id = ?, state = ? WHERE id = ?`,
		t.CurrentElementID, string(t.State), t.ID)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("sqlstore: token %q not found: %w", t.ID, storage.ErrNotFound)
	}
	return nil
}

func (s *Store) GetToken(ctx context.Context, id string) (*instance.Token, error) {
	var (
		piID, elemID, stateStr string
		createdAt              time.Time
	)
	err := s.db.QueryRowContext(ctx,
		`SELECT id, process_instance_id, current_element_id, state, created_at FROM tokens WHERE id = ?`, id).
		Scan(&id, &piID, &elemID, &stateStr, &createdAt)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("sqlstore: token %q not found: %w", id, storage.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}

	return &instance.Token{
		ID:                id,
		ProcessInstanceID: piID,
		CurrentElementID:  elemID,
		State:             instance.TokenState(stateStr),
		CreatedAt:         createdAt,
	}, nil
}

func (s *Store) ListActiveTokens(ctx context.Context, processInstanceID string) ([]*instance.Token, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, process_instance_id, current_element_id, state, created_at FROM tokens WHERE process_instance_id = ? AND state = 'active'`,
		processInstanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanTokens(rows)
}

func (s *Store) DeleteToken(ctx context.Context, id string) error {
	res, err := s.db.ExecContext(ctx, `DELETE FROM tokens WHERE id = ?`, id)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("sqlstore: token %q not found: %w", id, storage.ErrNotFound)
	}
	return nil
}

// --- VariableStore ---

func (s *Store) SetVariable(ctx context.Context, processInstanceID, name string, value any) error {
	valBytes, err := json.Marshal(value)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT OR REPLACE INTO variables (process_instance_id, name, value) VALUES (?, ?, ?)`,
		processInstanceID, name, string(valBytes))
	return err
}

func (s *Store) GetVariable(ctx context.Context, processInstanceID, name string) (any, error) {
	var valJSON string
	err := s.db.QueryRowContext(ctx,
		`SELECT value FROM variables WHERE process_instance_id = ? AND name = ?`,
		processInstanceID, name).Scan(&valJSON)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("sqlstore: variable %q not found for instance %q: %w", name, processInstanceID, storage.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}

	var val any
	if err := json.Unmarshal([]byte(valJSON), &val); err != nil {
		return nil, err
	}
	return val, nil
}

func (s *Store) GetAllVariables(ctx context.Context, processInstanceID string) (map[string]any, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT name, value FROM variables WHERE process_instance_id = ?`, processInstanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]any)
	for rows.Next() {
		var name, valJSON string
		if err := rows.Scan(&name, &valJSON); err != nil {
			return nil, err
		}
		var val any
		if err := json.Unmarshal([]byte(valJSON), &val); err != nil {
			return nil, err
		}
		result[name] = val
	}
	return result, rows.Err()
}

func (s *Store) DeleteVariable(ctx context.Context, processInstanceID, name string) error {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM variables WHERE process_instance_id = ? AND name = ?`, processInstanceID, name)
	if err != nil {
		return err
	}
	n, _ := res.RowsAffected()
	if n == 0 {
		return fmt.Errorf("sqlstore: variable %q not found for instance %q: %w", name, processInstanceID, storage.ErrNotFound)
	}
	return nil
}



// --- HistoricActivityInstanceStore ---

func (s *Store) CreateHistoricActivityInstance(ctx context.Context, hai *instance.HistoricActivityInstance) error {
	vars, err := json.Marshal(hai.Variables)
	if err != nil {
		return err
	}
	_, err = s.db.ExecContext(ctx,
		`INSERT INTO historic_activity_instances (id, process_instance_id, activity_id, activity_type, variables, started_at, completed_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		hai.ID, hai.ProcessInstanceID, hai.ActivityID, string(hai.ActivityType), string(vars), hai.StartedAt, hai.CompletedAt)
	return err
}

func (s *Store) ListHistoricByProcessInstance(ctx context.Context, processInstanceID string) ([]*instance.HistoricActivityInstance, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, process_instance_id, activity_id, activity_type, variables, started_at, completed_at FROM historic_activity_instances WHERE process_instance_id = ?`,
		processInstanceID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*instance.HistoricActivityInstance
	for rows.Next() {
		var id, piID, actID, actType, varsStr string
		var startedAt, completedAt time.Time
		if err := rows.Scan(&id, &piID, &actID, &actType, &varsStr, &startedAt, &completedAt); err != nil {
			return nil, err
		}
		vars := make(map[string]any)
		if varsStr != "" {
			json.Unmarshal([]byte(varsStr), &vars)
		}
		result = append(result, &instance.HistoricActivityInstance{
			ID: id, ProcessInstanceID: piID, ActivityID: actID,
			ActivityType: spec.ElementType(actType), Variables: vars,
			StartedAt: startedAt, CompletedAt: completedAt,
		})
	}
	return result, rows.Err()
}

// --- Additional ProcessDefinitionStore ---

func (s *Store) GetLatestProcessDefinitionByKey(ctx context.Context, key string) (*spec.ProcessDefinition, error) {
	var data []byte
	err := s.db.QueryRowContext(ctx, `SELECT data FROM process_definitions WHERE key = ? ORDER BY version DESC LIMIT 1`, key).Scan(&data)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("sqlstore: process definition with key %q not found: %w", key, storage.ErrNotFound)
	}
	if err != nil {
		return nil, err
	}
	return deserializeProcessDefinition(data)
}

// --- Additional ProcessInstanceStore ---

func (s *Store) ListCompletedProcessInstances(ctx context.Context, limit int) ([]*instance.ProcessInstance, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, def_id, state, variables, started_at, ended_at, parent_process_instance_id, parent_activity_id FROM process_instances WHERE state = 'completed' ORDER BY ended_at DESC LIMIT ?`, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanProcessInstances(rows)
}

// --- Helpers ---

func serializeProcessDefinition(def *spec.ProcessDefinition) ([]byte, error) {
	return json.Marshal(def)
}

func deserializeProcessDefinition(data []byte) (*spec.ProcessDefinition, error) {
	var def spec.ProcessDefinition
	if err := json.Unmarshal(data, &def); err != nil {
		return nil, err
	}
	return &def, nil
}

func scanProcessInstances(rows *sql.Rows) ([]*instance.ProcessInstance, error) {
	var result []*instance.ProcessInstance
	for rows.Next() {
		var (
			id, defID, stateStr, varsJSON, parentPIID, parentActID string
			startedAt                     time.Time
			endedAt                       *time.Time
		)
		if err := rows.Scan(&id, &defID, &stateStr, &varsJSON, &startedAt, &endedAt, &parentPIID, &parentActID); err != nil {
			return nil, err
		}
		vars := make(map[string]any)
		if varsJSON != "" {
			if err := json.Unmarshal([]byte(varsJSON), &vars); err != nil {
				return nil, err
			}
		}
		result = append(result, &instance.ProcessInstance{
			ID:                  id,
			ProcessDefinitionID: defID,
			State:               instance.ProcessInstanceState(stateStr),
			Variables:           vars,
			StartedAt:           startedAt,
			EndedAt:             endedAt,
			ParentProcessInstanceID: parentPIID,
			ParentActivityID:        parentActID,
		})
	}
	return result, rows.Err()
}

func scanActivityInstances(rows *sql.Rows) ([]*instance.ActivityInstance, error) {
	var result []*instance.ActivityInstance
	for rows.Next() {
		var (
			id, piID, activityID, activityTypeStr, stateStr, loopID, assigneeVal, adhocParentID string
			claimTime, completedTime                                *time.Time
			loopCounter                                             int
			expireTime                                      *time.Time
			termMode                                        int
		)
		if err := rows.Scan(&id, &piID, &activityID, &activityTypeStr, &assigneeVal, &adhocParentID, &stateStr, &claimTime, &completedTime, &loopID, &loopCounter, &expireTime, &termMode); err != nil {
			return nil, err
		}
		result = append(result, &instance.ActivityInstance{
			ID:                id,
			ProcessInstanceID: piID,
			ActivityID:        activityID,
			ActivityType:      spec.ElementType(activityTypeStr),
			State:             instance.ActivityInstanceState(stateStr),
			ClaimTime:         claimTime,
			CompletedTime:     completedTime,
			Assignee:            assigneeVal,
			AdhocParentID:       adhocParentID,
			MultiInstanceLoopID: loopID,
			LoopCounter:         loopCounter,
			ExpireTime:          expireTime,
			TermMode:            termMode,
		})
	}
	return result, rows.Err()
}

func scanTokens(rows *sql.Rows) ([]*instance.Token, error) {
	var result []*instance.Token
	for rows.Next() {
		var (
			id, piID, elemID, stateStr string
			createdAt                  time.Time
		)
		if err := rows.Scan(&id, &piID, &elemID, &stateStr, &createdAt); err != nil {
			return nil, err
		}
		result = append(result, &instance.Token{
			ID:                id,
			ProcessInstanceID: piID,
			CurrentElementID:  elemID,
			State:             instance.TokenState(stateStr),
			CreatedAt:         createdAt,
		})
	}
	return result, rows.Err()
}



// --- TimerJobStore ---

func (s *Store) CreateTimerJob(ctx context.Context, job *instance.TimerJob) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO timer_jobs (id, process_instance_id, element_id, due_at, fired) VALUES (?, ?, ?, ?, ?)`, job.ID, job.ProcessInstanceID, job.ElementID, job.DueAt, job.Fired)
	return err
}

func (s *Store) UpdateTimerJob(ctx context.Context, job *instance.TimerJob) error {
	_, err := s.db.ExecContext(ctx, `UPDATE timer_jobs SET due_at = ?, fired = ? WHERE id = ?`, job.DueAt, job.Fired, job.ID)
	return err
}
func (s *Store) ListDueTimerJobs(ctx context.Context, before time.Time) ([]*instance.TimerJob, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, process_instance_id, element_id, due_at, fired FROM timer_jobs WHERE fired = 0 AND due_at < ?`, before)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*instance.TimerJob
	for rows.Next() {
		var j instance.TimerJob
		if err := rows.Scan(&j.ID, &j.ProcessInstanceID, &j.ElementID, &j.DueAt, &j.Fired); err != nil {
			return nil, err
		}
		result = append(result, &j)
	}
	return result, rows.Err()
}

func (s *Store) DeleteTimerJob(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM timer_jobs WHERE id = ?`, id)
	return err
}

func (s *Store) DeleteTimerJobsByInstance(ctx context.Context, processInstanceID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM timer_jobs WHERE process_instance_id = ?`, processInstanceID)
	return err
}

// --- SignalSubscriptionStore ---

func (s *Store) CreateSignalSubscription(ctx context.Context, sub *instance.SignalSubscription) error {
	_, err := s.db.ExecContext(ctx, `INSERT INTO signal_subscriptions (id, process_instance_id, element_id, signal_ref) VALUES (?, ?, ?, ?)`, sub.ID, sub.ProcessInstanceID, sub.ElementID, sub.SignalRef)
	return err
}

func (s *Store) ListSignalSubscriptions(ctx context.Context, signalRef string) ([]*instance.SignalSubscription, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, process_instance_id, element_id, signal_ref FROM signal_subscriptions WHERE signal_ref = ?`, signalRef)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var result []*instance.SignalSubscription
	for rows.Next() {
		var sub instance.SignalSubscription
		if err := rows.Scan(&sub.ID, &sub.ProcessInstanceID, &sub.ElementID, &sub.SignalRef); err != nil {
			return nil, err
		}
		result = append(result, &sub)
	}
	return result, rows.Err()
}

func (s *Store) DeleteSignalSubscription(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM signal_subscriptions WHERE id = ?`, id)
	return err
}

func (s *Store) DeleteSubscriptionsByInstance(ctx context.Context, processInstanceID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM signal_subscriptions WHERE process_instance_id = ?`, processInstanceID)
	return err
}

// Compile-time check.
var _ storage.Store = (*Store)(nil)
