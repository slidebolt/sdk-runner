package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestScriptStore_SourcePath_ReturnsCorrectPath(t *testing.T) {
	store := &scriptStore{dataDir: "/data"}
	path := store.sourcePath("device1", "entity1")
	expected := filepath.Join("/data", "devices", "device1", "entities", "entity1.lua")
	if path != expected {
		t.Errorf("sourcePath = %q, want %q", path, expected)
	}
}

func TestScriptStore_StatePath_ReturnsCorrectPath(t *testing.T) {
	store := &scriptStore{dataDir: "/data"}
	path := store.statePath("device1", "entity1")
	expected := filepath.Join("/data", "devices", "device1", "entities", "entity1.state.lua.json")
	if path != expected {
		t.Errorf("statePath = %q, want %q", path, expected)
	}
}

func TestScriptStore_ReadSource_ExistingFile(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}
	deviceID := "d1"
	entityID := "e1"
	content := "return 1"

	// Create the file
	path := store.sourcePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, []byte(content), 0644)

	// Read it back
	data, returnedPath, err := store.readSource(deviceID, entityID)
	if err != nil {
		t.Fatalf("readSource failed: %v", err)
	}
	if data != content {
		t.Errorf("content = %q, want %q", data, content)
	}
	if returnedPath != path {
		t.Errorf("returned path = %q, want %q", returnedPath, path)
	}
}

func TestScriptStore_ReadSource_MissingFile(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}

	_, path, err := store.readSource("missing", "missing")
	if err == nil {
		t.Error("expected error for missing file")
	}
	if !strings.Contains(path, "missing") {
		t.Errorf("path should contain device ID, got %q", path)
	}
}

func TestScriptStore_WriteSource_CreatesDirectories(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}
	deviceID := "d1"
	entityID := "e1"
	content := "return 1"

	path, err := store.writeSource(deviceID, entityID, content)
	if err != nil {
		t.Fatalf("writeSource failed: %v", err)
	}

	// Verify directories were created
	scriptDir := filepath.Dir(path)
	if _, err := os.Stat(scriptDir); os.IsNotExist(err) {
		t.Errorf("directory not created: %s", scriptDir)
	}

	// Verify file content
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("failed to read written file: %v", err)
	}
	if string(data) != content {
		t.Errorf("file content = %q, want %q", string(data), content)
	}
}

func TestScriptStore_DeleteSource_RemovesFile(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}
	deviceID := "d1"
	entityID := "e1"

	// Create then delete
	path, _ := store.writeSource(deviceID, entityID, "test")
	store.deleteSource(deviceID, entityID)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("file should be deleted")
	}
}

func TestScriptStore_LoadState_ExistingFile(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}
	deviceID := "d1"
	entityID := "e1"
	state := map[string]any{"key": "value", "count": 42}

	// Write state file
	path := store.statePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(path), 0755)
	data, _ := json.Marshal(state)
	os.WriteFile(path, data, 0644)

	// Load it
	loaded := store.loadState(deviceID, entityID)
	if loaded["key"] != "value" {
		t.Errorf("key = %v, want value", loaded["key"])
	}
	if loaded["count"] != float64(42) {
		t.Errorf("count = %v, want 42", loaded["count"])
	}
}

func TestScriptStore_LoadState_CorruptedJSON(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}
	deviceID := "d1"
	entityID := "e1"

	// Write corrupted JSON
	path := store.statePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, []byte("not valid json"), 0644)

	// Should return empty map
	loaded := store.loadState(deviceID, entityID)
	if len(loaded) != 0 {
		t.Errorf("expected empty map for corrupted JSON, got %v", loaded)
	}
}

func TestScriptStore_LoadState_MissingFile(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}

	loaded := store.loadState("missing", "missing")
	if len(loaded) != 0 {
		t.Errorf("expected empty map for missing file, got %v", loaded)
	}
}

func TestScriptStore_SaveState_PersistsToDisk(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}
	deviceID := "d1"
	entityID := "e1"
	state := map[string]any{"key": "value"}

	// Mock writeIfChanged that creates directories and writes
	writeIfChanged := func(path string, data []byte) error {
		os.MkdirAll(filepath.Dir(path), 0755)
		return os.WriteFile(path, data, 0644)
	}

	store.saveState(deviceID, entityID, state, writeIfChanged)

	// Verify file exists and contains correct data
	path := store.statePath(deviceID, entityID)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("state file not written: %v", err)
	}

	var loaded map[string]any
	if err := json.Unmarshal(data, &loaded); err != nil {
		t.Fatalf("failed to unmarshal state: %v", err)
	}
	if loaded["key"] != "value" {
		t.Errorf("loaded key = %v, want value", loaded["key"])
	}
}

func TestScriptStore_StatMtime_ReturnsCorrectTime(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}
	deviceID := "d1"
	entityID := "e1"

	// Create file
	path := store.sourcePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, []byte("test"), 0644)

	// Get file info using os.Stat directly for comparison
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("failed to stat file: %v", err)
	}
	expectedMtime := info.ModTime().UnixNano()

	returnedPath, mtime, err := store.statMtime(deviceID, entityID)

	if err != nil {
		t.Fatalf("statMtime failed: %v", err)
	}
	if returnedPath != path {
		t.Errorf("returned path = %q, want %q", returnedPath, path)
	}
	// Allow 1 second tolerance for mtime
	if mtime < expectedMtime-1e9 || mtime > expectedMtime+1e9 {
		t.Errorf("mtime %d differs from expected %d by more than 1 second", mtime, expectedMtime)
	}
}

func TestScriptStore_DiscoverKeys_FindsAllScripts(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}

	// Create multiple scripts
	scripts := []struct {
		device string
		entity string
	}{
		{"device1", "entity1"},
		{"device1", "entity2"},
		{"device2", "entity1"},
	}

	for _, s := range scripts {
		path := store.sourcePath(s.device, s.entity)
		os.MkdirAll(filepath.Dir(path), 0755)
		os.WriteFile(path, []byte("test"), 0644)
	}

	keys := store.discoverKeys()
	if len(keys) != 3 {
		t.Errorf("found %d keys, want 3", len(keys))
	}

	// Verify all expected keys are present
	keyMap := make(map[scriptKey]bool)
	for _, k := range keys {
		keyMap[k] = true
	}

	for _, s := range scripts {
		key := scriptKey{DeviceID: s.device, EntityID: s.entity}
		if !keyMap[key] {
			t.Errorf("key %v not found", key)
		}
	}
}

func TestScriptStore_DeleteState_RemovesStateFile(t *testing.T) {
	store := &scriptStore{dataDir: t.TempDir()}
	deviceID := "d1"
	entityID := "e1"

	// Create state file
	path := store.statePath(deviceID, entityID)
	os.MkdirAll(filepath.Dir(path), 0755)
	os.WriteFile(path, []byte("{}"), 0644)

	store.deleteState(deviceID, entityID)

	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Error("state file should be deleted")
	}
}
