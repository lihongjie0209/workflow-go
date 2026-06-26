package lock

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestMemoryLocker_LockUnlock(t *testing.T) {
	l := NewMemoryLocker()
	ctx := context.Background()

	if err := l.Lock(ctx, "key1"); err != nil {
		t.Fatal(err)
	}
	if err := l.Unlock(ctx, "key1"); err != nil {
		t.Fatal(err)
	}
}

func TestMemoryLocker_TryLock(t *testing.T) {
	l := NewMemoryLocker()
	ctx := context.Background()

	ok, err := l.TryLock(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected TryLock to succeed")
	}

	// Second attempt should fail
	ok, err = l.TryLock(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatal("expected TryLock to fail when already locked")
	}

	l.Unlock(ctx, "key1")
	ok, err = l.TryLock(ctx, "key1")
	if !ok {
		t.Fatal("expected TryLock to succeed after unlock")
	}
	l.Unlock(ctx, "key1")
}

func TestMemoryLocker_DifferentKeysDontBlock(t *testing.T) {
	l := NewMemoryLocker()
	ctx := context.Background()

	l.Lock(ctx, "key1")
	// The second goroutine should be able to lock "key2" while "key1" is held
	done := make(chan struct{})
	go func() {
		l.Lock(ctx, "key2")
		close(done)
	}()

	select {
	case <-done:
		// Success - key2 was acquired
	case <-time.After(time.Second):
		t.Fatal("key2 was blocked by key1")
	}
	l.Unlock(ctx, "key2")
	l.Unlock(ctx, "key1")
}

func TestMemoryLocker_SameKeyBlocks(t *testing.T) {
	l := NewMemoryLocker()
	ctx := context.Background()

	l.Lock(ctx, "key1")
	blocked := make(chan struct{})
	go func() {
		l.Lock(ctx, "key1")
		close(blocked)
	}()

	select {
	case <-blocked:
		t.Fatal("second lock on same key should block")
	case <-time.After(100 * time.Millisecond):
		// Expected: still blocked
	}

	l.Unlock(ctx, "key1")
	select {
	case <-blocked:
		// Now it should proceed
	case <-time.After(time.Second):
		t.Fatal("second lock should proceed after first unlock")
	}
	l.Unlock(ctx, "key1")
}

func TestMemoryLocker_LockWithTTL(t *testing.T) {
	l := NewMemoryLocker()
	ctx := context.Background()

	if err := l.LockWithTTL(ctx, "key1", 50*time.Millisecond); err != nil {
		t.Fatal(err)
	}

	// After TTL expires, the lock should be auto-released
	time.Sleep(100 * time.Millisecond)

	// Try locking again - should succeed if auto-release worked
	ok, err := l.TryLock(ctx, "key1")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected lock to be auto-released after TTL")
	}
	l.Unlock(ctx, "key1")
}

func TestMemoryLocker_ContextCancellation(t *testing.T) {
	l := NewMemoryLocker()
	ctx := context.Background()

	l.Lock(ctx, "key1")

	cancelCtx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	// This should fail with cancelled context
	err := l.Lock(cancelCtx, "key1")
	if err == nil {
		t.Fatal("expected context cancellation error")
	}

	l.Unlock(ctx, "key1")
}

func TestMemoryLocker_ConcurrentLockStress(t *testing.T) {
	l := NewMemoryLocker()
	ctx := context.Background()

	var wg sync.WaitGroup
	counter := 0
	const ops = 100

	for i := 0; i < ops; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := l.Lock(ctx, "counter"); err != nil {
				return
			}
			counter++
			l.Unlock(ctx, "counter")
		}()
	}
	wg.Wait()

	if counter != ops {
		t.Errorf("expected counter=%d, got %d", ops, counter)
	}
}

func TestMemoryLocker_UnlockWithoutLock(t *testing.T) {
	l := NewMemoryLocker()
	ctx := context.Background()

	err := l.Unlock(ctx, "nonexistent")
	if err == nil {
		t.Fatal("expected error unlocking non-existent key")
	}
}
