package runner

import (
	"sync/atomic"
	"testing"
	"time"
)

// ---------------------------------------------------------------------------
// Every
// ---------------------------------------------------------------------------

func TestSchedulerEveryFires(t *testing.T) {
	s := newScheduler()
	s.start()
	defer s.stop()

	var count atomic.Int64
	s.Every(10*time.Millisecond, func() { count.Add(1) })

	time.Sleep(55 * time.Millisecond)
	got := count.Load()
	if got < 3 {
		t.Fatalf("expected at least 3 ticks in 55ms, got %d", got)
	}
}

func TestSchedulerEveryCancelStopsFiring(t *testing.T) {
	s := newScheduler()
	s.start()
	defer s.stop()

	var count atomic.Int64
	cancel := s.Every(10*time.Millisecond, func() { count.Add(1) })

	time.Sleep(35 * time.Millisecond)
	cancel()
	snapshot := count.Load()

	time.Sleep(35 * time.Millisecond)
	if after := count.Load(); after != snapshot {
		t.Fatalf("job still fired after cancel: before=%d after=%d", snapshot, after)
	}
}

func TestSchedulerEveryCancelIsIdempotent(t *testing.T) {
	s := newScheduler()
	s.start()
	defer s.stop()

	cancel := s.Every(10*time.Millisecond, func() {})
	// Should not panic.
	cancel()
	cancel()
	cancel()
}

func TestSchedulerEveryMultipleJobs(t *testing.T) {
	s := newScheduler()
	s.start()
	defer s.stop()

	var a, b atomic.Int64
	s.Every(10*time.Millisecond, func() { a.Add(1) })
	s.Every(20*time.Millisecond, func() { b.Add(1) })

	time.Sleep(65 * time.Millisecond)

	if a.Load() < 4 {
		t.Fatalf("job A: expected ≥4 ticks, got %d", a.Load())
	}
	if b.Load() < 2 {
		t.Fatalf("job B: expected ≥2 ticks, got %d", b.Load())
	}
	// A should fire roughly twice as often as B.
	if a.Load() < b.Load() {
		t.Fatalf("job A (%d) should fire more often than job B (%d)", a.Load(), b.Load())
	}
}

func TestSchedulerStopCancelsAllEveryJobs(t *testing.T) {
	s := newScheduler()
	s.start()

	var count atomic.Int64
	s.Every(10*time.Millisecond, func() { count.Add(1) })
	s.Every(10*time.Millisecond, func() { count.Add(1) })

	time.Sleep(25 * time.Millisecond)
	s.stop()
	snapshot := count.Load()

	time.Sleep(30 * time.Millisecond)
	if after := count.Load(); after != snapshot {
		t.Fatalf("jobs still fired after stop: before=%d after=%d", snapshot, after)
	}
}

func TestSchedulerEveryPanicsOnZeroInterval(t *testing.T) {
	s := newScheduler()
	s.start()
	defer s.stop()

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic for zero interval")
		}
	}()
	s.Every(0, func() {})
}

// ---------------------------------------------------------------------------
// Cron
// ---------------------------------------------------------------------------

func TestSchedulerCronRejectsInvalidExpression(t *testing.T) {
	s := newScheduler()
	s.start()
	defer s.stop()

	_, err := s.Cron("not-a-cron", func() {})
	if err == nil {
		t.Fatal("expected error for invalid cron expression")
	}
}

func TestSchedulerCronAcceptsValidExpression(t *testing.T) {
	s := newScheduler()
	s.start()
	defer s.stop()

	cancel, err := s.Cron("* * * * *", func() {})
	if err != nil {
		t.Fatalf("unexpected error for valid cron: %v", err)
	}
	cancel() // cleanup
}

func TestSchedulerCronCancelIsIdempotent(t *testing.T) {
	s := newScheduler()
	s.start()
	defer s.stop()

	cancel, err := s.Cron("* * * * *", func() {})
	if err != nil {
		t.Fatalf("cron setup: %v", err)
	}
	// Should not panic.
	cancel()
	cancel()
	cancel()
}

// ---------------------------------------------------------------------------
// SchedulerService interface compliance
// ---------------------------------------------------------------------------

func TestSchedulerImplementsInterface(t *testing.T) {
	var _ SchedulerService = (*scheduler)(nil)
}

// ---------------------------------------------------------------------------
// stop is safe to call without start
// ---------------------------------------------------------------------------

func TestSchedulerStopWithoutStart(t *testing.T) {
	s := newScheduler()
	// Should not hang or panic.
	done := make(chan struct{})
	go func() {
		s.stop()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("stop() deadlocked when called without start")
	}
}
