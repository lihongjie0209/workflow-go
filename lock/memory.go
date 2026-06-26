package lock

import (
	"context"
	"fmt"
	"sync"
	"time"
)

// MemoryLocker is an in-process, mutex-based implementation of Locker.
// It uses a map of per-key mutexes protected by a main RWMutex.
// Suitable for single-instance deployments and testing.
type MemoryLocker struct {
	mu   sync.Mutex
	locks map[string]*sync.Mutex
	holders map[string]bool // tracks which keys are held (for TryLock)
}

// NewMemoryLocker creates a new MemoryLocker.
func NewMemoryLocker() *MemoryLocker {
	return &MemoryLocker{
		locks:   make(map[string]*sync.Mutex),
		holders: make(map[string]bool),
	}
}

// getOrCreate returns the mutex for the given key, creating it if needed.
func (l *MemoryLocker) getOrCreate(key string) *sync.Mutex {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, ok := l.locks[key]; !ok {
		l.locks[key] = &sync.Mutex{}
	}
	return l.locks[key]
}

func (l *MemoryLocker) Lock(ctx context.Context, key string) error {
	mu := l.getOrCreate(key)
	locked := make(chan struct{}, 1)
	go func() {
		mu.Lock()
		select {
		case locked <- struct{}{}:
		default:
		}
	}()

	select {
	case <-locked:
		l.mu.Lock()
		l.holders[key] = true
		l.mu.Unlock()
		return nil
	case <-ctx.Done():
		go func() {
			// If we grabbed the lock after context cancellation, release it
			select {
			case <-locked:
				mu.Unlock()
			default:
			}
		}()
		return ctx.Err()
	}
}

func (l *MemoryLocker) TryLock(_ context.Context, key string) (bool, error) {
	mu := l.getOrCreate(key)
	if !mu.TryLock() {
		return false, nil
	}
	l.mu.Lock()
	if l.holders[key] {
		l.mu.Unlock()
		mu.Unlock()
		return false, nil
	}
	l.holders[key] = true
	l.mu.Unlock()
	return true, nil
}

func (l *MemoryLocker) LockWithTTL(ctx context.Context, key string, ttl time.Duration) error {
	if err := l.Lock(ctx, key); err != nil {
		return err
	}
	go func(key string) {
		time.Sleep(ttl)
		if err := l.Unlock(context.Background(), key); err != nil {
			_ = err
		}
	}(key)
	return nil
}

func (l *MemoryLocker) Unlock(_ context.Context, key string) error {
	l.mu.Lock()
	if !l.holders[key] {
		l.mu.Unlock()
		return fmt.Errorf("lock: unlock of unlocked key %q", key)
	}
	delete(l.holders, key)
	mu := l.locks[key]
	l.mu.Unlock()
	mu.Unlock()
	return nil
}

// Ensure compile-time check.
var _ Locker = (*MemoryLocker)(nil)
