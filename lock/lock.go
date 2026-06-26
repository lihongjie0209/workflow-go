// Package lock provides a pluggable distributed locking interface for the workflow engine.
// Built-in implementations include a mutex-based memory locker suitable for single-instance
// deployments. For multi-instance or clustered deployments, provide a Redis-backed (or other
// distributed) implementation via the Locker interface.
package lock

import (
	"context"
	"errors"
	"time"
)

// ErrLockNotAcquired is returned by TryLock when the lock is already held.
var ErrLockNotAcquired = errors.New("lock not acquired")

// Locker is the interface that wraps the basic lock operations.
// Implementations must be safe for concurrent use.
type Locker interface {
	// Lock acquires the lock, blocking until it is available or the context is cancelled.
	// Returns context.Canceled / context.DeadlineExceeded if ctx is done before acquisition.
	Lock(ctx context.Context, key string) error

	// TryLock attempts to acquire the lock without blocking.
	// Returns true if the lock was acquired, false otherwise.
	// Returns ErrLockNotAcquired if the lock is held by someone else.
	TryLock(ctx context.Context, key string) (bool, error)

	// LockWithTTL acquires the lock with an automatic expiration.
	// The lock is automatically released after the TTL, preventing deadlocks.
	LockWithTTL(ctx context.Context, key string, ttl time.Duration) error

	// Unlock releases the lock. Panics if the lock is not held by the caller.
	Unlock(ctx context.Context, key string) error
}
