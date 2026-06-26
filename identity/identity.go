// Package identity provides user, group, and role management for the workflow engine.
// It defines the Service interface for resolving candidate users and groups
// during process execution, along with an in-memory implementation.
package identity

import (
	"context"
	"fmt"
	"sync"
)

// User represents a workflow user.
type User struct {
	ID     string
	Name   string
	Groups []string // group IDs this user belongs to
}

// Group represents a collection of users.
type Group struct {
	ID      string
	Name    string
	Members []string // user IDs
}

// Service defines the identity management interface.
type Service interface {
	// GetUser returns a user by ID.
	GetUser(ctx context.Context, userID string) (*User, error)
	// GetGroup returns a group by ID.
	GetGroup(ctx context.Context, groupID string) (*Group, error)
	// GetUserGroups returns all group IDs the user belongs to.
	GetUserGroups(ctx context.Context, userID string) ([]string, error)
	// GetGroupMembers returns all member user IDs in the group.
	GetGroupMembers(ctx context.Context, groupID string) ([]string, error)
	// IsUserInGroup checks if a user belongs to a specific group.
	IsUserInGroup(ctx context.Context, userID, groupID string) (bool, error)
	// ResolveCandidateUsers resolves the union of explicit candidates and group members.
	// If candidateGroup is non-empty, all group members are included in the result.
	ResolveCandidateUsers(ctx context.Context, candidateUsers []string, candidateGroup string) ([]string, error)
}

// MemoryStore is an in-memory implementation of Service backed by maps.
type MemoryStore struct {
	mu     sync.RWMutex
	users  map[string]*User
	groups map[string]*Group
}

// NewMemoryStore creates an empty in-memory identity store.
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		users:  make(map[string]*User),
		groups: make(map[string]*Group),
	}
}

// AddUser adds a user to the store.
func (s *MemoryStore) AddUser(user *User) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.users[user.ID]; exists {
		return fmt.Errorf("identity: user %q already exists", user.ID)
	}
	s.users[user.ID] = user
	return nil
}

// AddGroup adds a group to the store.
func (s *MemoryStore) AddGroup(group *Group) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.groups[group.ID]; exists {
		return fmt.Errorf("identity: group %q already exists", group.ID)
	}
	s.groups[group.ID] = group
	// Ensure each member's Groups slice includes this group
	for _, memberID := range group.Members {
		u, ok := s.users[memberID]
		if ok {
			found := false
			for _, gid := range u.Groups {
				if gid == group.ID {
					found = true
					break
				}
			}
			if !found {
				u.Groups = append(u.Groups, group.ID)
			}
		}
	}
	return nil
}

// AddUserToGroup adds an existing user to an existing group.
func (s *MemoryStore) AddUserToGroup(userID, groupID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	u, ok := s.users[userID]
	if !ok {
		return fmt.Errorf("identity: user %q not found", userID)
	}
	g, ok := s.groups[groupID]
	if !ok {
		return fmt.Errorf("identity: group %q not found", groupID)
	}
	// Update user's Groups list
	found := false
	for _, gid := range u.Groups {
		if gid == groupID {
			found = true
			break
		}
	}
	if !found {
		u.Groups = append(u.Groups, groupID)
	}
	// Update group's Members list
	for _, mid := range g.Members {
		if mid == userID {
			return nil // already a member
		}
	}
	g.Members = append(g.Members, userID)
	return nil
}

func (s *MemoryStore) GetUser(_ context.Context, userID string) (*User, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[userID]
	if !ok {
		return nil, fmt.Errorf("identity: user %q not found", userID)
	}
	return u, nil
}

func (s *MemoryStore) GetGroup(_ context.Context, groupID string) (*Group, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.groups[groupID]
	if !ok {
		return nil, fmt.Errorf("identity: group %q not found", groupID)
	}
	return g, nil
}

func (s *MemoryStore) GetUserGroups(_ context.Context, userID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[userID]
	if !ok {
		return nil, fmt.Errorf("identity: user %q not found", userID)
	}
	result := make([]string, len(u.Groups))
	copy(result, u.Groups)
	return result, nil
}

func (s *MemoryStore) GetGroupMembers(_ context.Context, groupID string) ([]string, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	g, ok := s.groups[groupID]
	if !ok {
		return nil, fmt.Errorf("identity: group %q not found", groupID)
	}
	result := make([]string, len(g.Members))
	copy(result, g.Members)
	return result, nil
}

func (s *MemoryStore) IsUserInGroup(_ context.Context, userID, groupID string) (bool, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	u, ok := s.users[userID]
	if !ok {
		return false, fmt.Errorf("identity: user %q not found", userID)
	}
	for _, gid := range u.Groups {
		if gid == groupID {
			return true, nil
		}
	}
	return false, nil
}

func (s *MemoryStore) ResolveCandidateUsers(ctx context.Context, candidateUsers []string, candidateGroup string) ([]string, error) {
	seen := make(map[string]bool)
	var result []string

	for _, uid := range candidateUsers {
		if !seen[uid] {
			seen[uid] = true
			result = append(result, uid)
		}
	}

	if candidateGroup != "" {
		members, err := s.GetGroupMembers(ctx, candidateGroup)
		if err != nil {
			return nil, err
		}
		for _, uid := range members {
			if !seen[uid] {
				seen[uid] = true
				result = append(result, uid)
			}
		}
	}

	return result, nil
}

// Compile-time check.
var _ Service = (*MemoryStore)(nil)
