package runner

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	types "github.com/slidebolt/sdk-types"
)

// TestPathTraversal_DeviceID exposes path traversal vulnerability
func TestPathTraversal_DeviceID(t *testing.T) {
	r := newTestRunner(t)

	// Attempt path traversal
	maliciousID := "../../../tmp/pwned"
	device := types.Device{
		ID:       maliciousID,
		SourceID: maliciousID,
	}

	r.saveDevice(device)

	// Check if file escaped dataDir
	outsidePath := filepath.Join("/tmp", "pwned.json")
	if _, err := os.Stat(outsidePath); err == nil {
		t.Errorf("WEAKNESS: Path traversal! File created outside dataDir at %s", outsidePath)
		os.Remove(outsidePath)
	}
}

// TestNilRunner_NilPointerRisk exposes nil pointer handling
func TestNilRunner_NilPointerRisk(t *testing.T) {
	var r *Runner

	defer func() {
		if rec := recover(); rec != nil {
			t.Logf("WEAKNESS: Operation panics on nil runner: %v", rec)
		}
	}()

	// These should handle nil gracefully
	_, _ = r.SetLogLevel("debug")
	_ = r.EmitEvent(types.InboundEvent{})
	_ = r.LogLevel()
}

// TestEmptyID_ValidationGap exposes missing validation
func TestEmptyID_ValidationGap(t *testing.T) {
	r := newTestRunner(t)

	// Save device with empty ID
	emptyDevice := types.Device{ID: ""}
	r.saveDevice(emptyDevice)

	// Load it back
	loaded := r.loadDeviceByID("")
	if loaded.ID != "" {
		t.Logf("WEAKNESS: Empty ID stored and retrieved as '%s'", loaded.ID)
	}
}

// TestRaceCondition_StatusMap exposes potential race condition
func TestRaceCondition_StatusMap(t *testing.T) {
	r := newTestRunner(t)
	cmdID := "race-test"

	// Concurrent writes
	for i := 0; i < 50; i++ {
		go func() {
			status := types.CommandStatus{
				CommandID: cmdID,
				State:     types.CommandPending,
			}
			r.setCommandStatus(status)
		}()
	}

	// Concurrent reads
	for i := 0; i < 50; i++ {
		go func() {
			r.getCommandStatus(cmdID)
		}()
	}

	time.Sleep(100 * time.Millisecond)
	// If no panic, race detector would catch any issues
}

// TestSilentError_IgnoredErrors exposes error handling gaps
func TestSilentError_IgnoredErrors(t *testing.T) {
	r := newTestRunner(t)

	// Delete non-existent entity - error ignored
	err := r.ensureStateStore().DeleteEntity("fake", "fake")
	if err == nil {
		t.Log("WEAKNESS: DeleteEntity silently succeeds for non-existent entity")
	}

	// Load non-existent state - returns empty, no error
	state := r.loadState("nonexistent")
	if state.Meta == "" && len(state.Data) == 0 {
		t.Log("WEAKNESS: LoadState returns empty struct without error for missing state")
	}
}

// TestEmptyEntityError_ReturnValue exposes ambiguity in error handling
func TestEmptyEntityError_ReturnValue(t *testing.T) {
	r := newTestRunner(t)

	// Load non-existent entity
	ent := r.loadEntity("fake-device", "fake-entity")

	// Empty struct returned - can't distinguish not-found from empty
	if ent.ID == "" {
		t.Log("WEAKNESS: loadEntity returns empty Entity - can't distinguish 'not found' from valid empty entity")
	}
}

// TestDeviceLock_EmptyIDConsistency exposes lock management issue
func TestDeviceLock_EmptyIDConsistency(t *testing.T) {
	r := newTestRunner(t)

	lock1 := r.deviceLock("")
	lock2 := r.deviceLock("")
	lock3 := r.deviceLock("   ")

	// Empty/whitespace should use same lock
	if lock1 != lock2 || lock1 != lock3 {
		t.Log("WEAKNESS: Empty/whitespace device IDs may return different locks")
	}
}

// TestScriptPath_SpecialCharacters exposes path handling issues
func TestScriptPath_SpecialCharacters(t *testing.T) {
	r := newTestRunner(t)

	// Device ID with special characters
	badDeviceID := "device/with/slashes"
	path := r.scriptPath(badDeviceID, "entity")

	// This creates nested directories unintentionally
	if strings.Contains(path, "device/with") {
		t.Logf("WEAKNESS: Special characters in ID create nested paths: %s", path)
	}
}

// TestHashMap_RaceCondition exposes race in hash operations
func TestHashMap_RaceCondition(t *testing.T) {
	r := newTestRunner(t)
	path := "/test/path"

	// Concurrent hash operations
	for i := 0; i < 50; i++ {
		go func() {
			r.setHash(path, "hash1")
			r.getHash(path)
			r.clearHash(path)
		}()
	}

	time.Sleep(50 * time.Millisecond)
}

// TestRegistry_RaceCondition exposes race in registry access
func TestRegistry_RaceCondition(t *testing.T) {
	r := newTestRunner(t)

	// Concurrent registry access
	for i := 0; i < 30; i++ {
		go func(idx int) {
			reg := types.Registration{
				Manifest: types.Manifest{ID: "plugin-" + string(rune('a'+idx%26))},
			}
			r.registryMu.Lock()
			r.registry[reg.Manifest.ID] = reg
			r.registryMu.Unlock()
		}(i)
	}

	for i := 0; i < 30; i++ {
		go func(idx int) {
			r.registryMu.RLock()
			_ = r.snapshotRegistry()
			r.registryMu.RUnlock()
		}(i)
	}

	time.Sleep(50 * time.Millisecond)
}
