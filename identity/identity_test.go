package identity

import (
	"context"
	"testing"
)

func TestMemoryStore_CRUD(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()

	// Add users
	if err := s.AddUser(&User{ID: "u1", Name: "Alice"}); err != nil {
		t.Fatal(err)
	}
	if err := s.AddUser(&User{ID: "u2", Name: "Bob", Groups: []string{"g1"}}); err != nil {
		t.Fatal(err)
	}
	// Duplicate user
	if err := s.AddUser(&User{ID: "u1", Name: "Alice"}); err == nil {
		t.Error("expected error for duplicate user")
	}

	// Get user
	u, err := s.GetUser(ctx, "u1")
	if err != nil {
		t.Fatal(err)
	}
	if u.Name != "Alice" {
		t.Errorf("got name=%q, want Alice", u.Name)
	}
	// Get nonexistent user
	_, err = s.GetUser(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent user")
	}

	// Add group
	if err := s.AddGroup(&Group{ID: "g1", Name: "HR", Members: []string{"u1"}}); err != nil {
		t.Fatal(err)
	}
	// Duplicate group
	if err := s.AddGroup(&Group{ID: "g1", Name: "HR"}); err == nil {
		t.Error("expected error for duplicate group")
	}

	// Get group
	g, err := s.GetGroup(ctx, "g1")
	if err != nil {
		t.Fatal(err)
	}
	if g.Name != "HR" {
		t.Errorf("got name=%q, want HR", g.Name)
	}

	// IsUserInGroup
	ok, err := s.IsUserInGroup(ctx, "u1", "g1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("u1 should be in g1")
	}
	ok, err = s.IsUserInGroup(ctx, "u2", "g1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("u2 should be in g1 (added via Groups field at creation)")
	}

	// AddUserToGroup
	if err := s.AddUserToGroup("u2", "g1"); err != nil {
		t.Fatal(err)
	}
	// This should be idempotent
	if err := s.AddUserToGroup("u2", "g1"); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryStore_ResolveCandidates(t *testing.T) {
	ctx := context.Background()
	s := NewMemoryStore()
	s.AddUser(&User{ID: "u1", Name: "Alice"})
	s.AddUser(&User{ID: "u2", Name: "Bob"})
	s.AddUser(&User{ID: "u3", Name: "Charlie"})
	s.AddGroup(&Group{ID: "g1", Name: "HR", Members: []string{"u2", "u3"}})

	// Explicit candidates only
	candidates, err := s.ResolveCandidateUsers(ctx, []string{"u1", "u2"}, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 || candidates[0] != "u1" || candidates[1] != "u2" {
		t.Errorf("got %v, want [u1 u2]", candidates)
	}

	// Group only
	candidates, err = s.ResolveCandidateUsers(ctx, nil, "g1")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 {
		t.Errorf("got %d candidates, want 2", len(candidates))
	}

	// Both combined
	candidates, err = s.ResolveCandidateUsers(ctx, []string{"u1"}, "g1")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 3 {
		t.Errorf("got %d candidates, want 3 (u1 + g1 has u2,u3)", len(candidates))
	}

	// Dedup: u2 is both explicit and in group
	candidates, err = s.ResolveCandidateUsers(ctx, []string{"u2"}, "g1")
	if err != nil {
		t.Fatal(err)
	}
	if len(candidates) != 2 {
		t.Errorf("got %d candidates, want 2 (u2 deduped with g1 members)", len(candidates))
	}

	// Nonexistent group
	_, err = s.ResolveCandidateUsers(ctx, nil, "nonexistent")
	if err == nil {
		t.Error("expected error for nonexistent group")
	}
}
