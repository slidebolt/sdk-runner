package runner

import (
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	lua "github.com/yuin/gopher-lua"
)

func newTestScriptStore(t *testing.T) *scriptStore {
	return &scriptStore{dataDir: t.TempDir()}
}

func newTestScriptRuntime(key scriptKey, path string, mtime int64) (*scriptRuntime, error) {
	L := lua.NewState()
	rt := &scriptRuntime{
		key:         key,
		path:        path,
		mtimeUnix:   mtime,
		L:           L,
		eventSubs:   map[string]string{},
		commandSubs: map[string]string{},
		dynamicSubs: map[string]*dynamicQuery{},
		state:       map[string]any{},
	}
	return rt, nil
}

func TestScriptRuntimeCache_Ensure_LoadsNewRuntime(t *testing.T) {
	store := newTestScriptStore(t)
	cache := newScriptRuntimeCache(store, newTestScriptRuntime)

	// Create a script file
	deviceID := "d1"
	entityID := "e1"
	scriptPath := store.sourcePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(scriptPath), 0755)
	os.WriteFile(scriptPath, []byte("return 1"), 0644)

	rt, err := cache.ensure(deviceID, entityID)
	if err != nil {
		t.Fatalf("ensure failed: %v", err)
	}
	if rt == nil {
		t.Fatal("expected runtime, got nil")
	}
	if rt.key.DeviceID != deviceID || rt.key.EntityID != entityID {
		t.Errorf("key mismatch: got %v", rt.key)
	}
}

func TestScriptRuntimeCache_Ensure_HotReloadOnMtimeChange(t *testing.T) {
	store := newTestScriptStore(t)
	cache := newScriptRuntimeCache(store, newTestScriptRuntime)

	deviceID := "d1"
	entityID := "e1"
	scriptPath := store.sourcePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(scriptPath), 0755)
	os.WriteFile(scriptPath, []byte("version1"), 0644)

	// First load
	rt1, err := cache.ensure(deviceID, entityID)
	if err != nil {
		t.Fatalf("first ensure failed: %v", err)
	}
	firstMtime := rt1.mtimeUnix

	// Wait and modify file
	time.Sleep(10 * time.Millisecond)
	os.WriteFile(scriptPath, []byte("version2"), 0644)

	// Second load should return new runtime due to mtime change
	rt2, err := cache.ensure(deviceID, entityID)
	if err != nil {
		t.Fatalf("second ensure failed: %v", err)
	}
	if rt2.mtimeUnix == firstMtime {
		t.Error("expected different mtime after file modification")
	}
}

func TestScriptRuntimeCache_Ensure_ReturnsNilWhenFileMissing(t *testing.T) {
	store := newTestScriptStore(t)
	cache := newScriptRuntimeCache(store, newTestScriptRuntime)

	rt, err := cache.ensure("missing", "missing")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if rt != nil {
		t.Error("expected nil runtime for missing file")
	}
}

func TestScriptRuntimeCache_Ensure_ClosesStaleRuntime(t *testing.T) {
	store := newTestScriptStore(t)
	cache := newScriptRuntimeCache(store, newTestScriptRuntime)

	deviceID := "d1"
	entityID := "e1"
	scriptPath := store.sourcePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(scriptPath), 0755)
	os.WriteFile(scriptPath, []byte("version1"), 0644)

	// Load first version
	rt1, _ := cache.ensure(deviceID, entityID)
	if rt1.closed {
		t.Error("first runtime should not be closed yet")
	}

	// Modify file and reload
	time.Sleep(10 * time.Millisecond)
	os.WriteFile(scriptPath, []byte("version2"), 0644)
	rt2, _ := cache.ensure(deviceID, entityID)

	// First runtime should be closed
	if !rt1.closed {
		t.Error("first runtime should be closed after hot reload")
	}
	if rt2.closed {
		t.Error("second runtime should not be closed")
	}
}

func TestScriptRuntimeCache_Invalidate_ClosesAndRemoves(t *testing.T) {
	store := newTestScriptStore(t)
	cache := newScriptRuntimeCache(store, newTestScriptRuntime)

	deviceID := "d1"
	entityID := "e1"
	scriptPath := store.sourcePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(scriptPath), 0755)
	os.WriteFile(scriptPath, []byte("test"), 0644)

	rt, _ := cache.ensure(deviceID, entityID)
	cache.invalidate(deviceID, entityID)

	if !rt.closed {
		t.Error("runtime should be closed after invalidate")
	}

	// Verify removed from cache
	cache.mu.Lock()
	_, exists := cache.entries[scriptKey{DeviceID: deviceID, EntityID: entityID}]
	cache.mu.Unlock()
	if exists {
		t.Error("runtime should be removed from cache")
	}
}

func TestScriptRuntimeCache_Sync_DiscoversNewScripts(t *testing.T) {
	store := newTestScriptStore(t)
	cache := newScriptRuntimeCache(store, newTestScriptRuntime)

	// Create multiple scripts
	for i := 1; i <= 3; i++ {
		scriptPath := store.sourcePath("d"+string(rune('0'+i)), "e"+string(rune('0'+i)))
		os.MkdirAll(filepath.Dir(scriptPath), 0755)
		os.WriteFile(scriptPath, []byte("test"), 0644)
	}

	cache.sync()

	snapshot := cache.snapshot()
	if len(snapshot) != 3 {
		t.Errorf("expected 3 runtimes, got %d", len(snapshot))
	}
}

func TestScriptRuntimeCache_Sync_RemovesDeletedScripts(t *testing.T) {
	store := newTestScriptStore(t)
	cache := newScriptRuntimeCache(store, newTestScriptRuntime)

	// Create and load a script
	scriptPath := store.sourcePath("d1", "e1")
	os.MkdirAll(filepath.Dir(scriptPath), 0755)
	os.WriteFile(scriptPath, []byte("test"), 0644)
	rt, _ := cache.ensure("d1", "e1")

	// Delete the file
	os.Remove(scriptPath)

	// Sync should remove the runtime
	cache.sync()

	if !rt.closed {
		t.Error("runtime should be closed after file deletion")
	}

	snapshot := cache.snapshot()
	if len(snapshot) != 0 {
		t.Errorf("expected 0 runtimes, got %d", len(snapshot))
	}
}

func TestScriptRuntimeCache_Snapshot_ReturnsAllRuntimes(t *testing.T) {
	store := newTestScriptStore(t)
	cache := newScriptRuntimeCache(store, newTestScriptRuntime)

	// Create scripts
	for i := 1; i <= 5; i++ {
		scriptPath := store.sourcePath("device"+string(rune('0'+i)), "entity"+string(rune('0'+i)))
		os.MkdirAll(filepath.Dir(scriptPath), 0755)
		os.WriteFile(scriptPath, []byte("test"), 0644)
		cache.ensure("device"+string(rune('0'+i)), "entity"+string(rune('0'+i)))
	}

	snapshot := cache.snapshot()
	if len(snapshot) != 5 {
		t.Errorf("expected 5 runtimes in snapshot, got %d", len(snapshot))
	}
}

func TestScriptRuntimeCache_ConcurrentAccess_Safe(t *testing.T) {
	store := newTestScriptStore(t)
	cache := newScriptRuntimeCache(store, newTestScriptRuntime)

	deviceID := "d1"
	entityID := "e1"
	scriptPath := store.sourcePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(scriptPath), 0755)
	os.WriteFile(scriptPath, []byte("test"), 0644)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := cache.ensure(deviceID, entityID)
			if err != nil {
				t.Errorf("concurrent ensure failed: %v", err)
			}
		}()
	}
	wg.Wait()

	// Should have exactly one runtime
	snapshot := cache.snapshot()
	if len(snapshot) != 1 {
		t.Errorf("expected 1 runtime after concurrent access, got %d", len(snapshot))
	}
}

func TestScriptRuntimeCache_FactoryError_Propagates(t *testing.T) {
	store := newTestScriptStore(t)

	// Factory that always errors
	errorFactory := func(key scriptKey, path string, mtime int64) (*scriptRuntime, error) {
		return nil, os.ErrInvalid
	}

	cache := newScriptRuntimeCache(store, errorFactory)

	deviceID := "d1"
	entityID := "e1"
	scriptPath := store.sourcePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(scriptPath), 0755)
	os.WriteFile(scriptPath, []byte("test"), 0644)

	_, err := cache.ensure(deviceID, entityID)
	if err == nil {
		t.Error("expected error from failing factory")
	}
}
