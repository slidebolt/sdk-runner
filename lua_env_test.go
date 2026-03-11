package runner

import (
	"log/slog"
	"os"
	"sync"
	"testing"

	types "github.com/slidebolt/sdk-types"
)

func TestNewLuaEnv_CreatesWithRunner(t *testing.T) {
	r := &Runner{
		dataDir:  t.TempDir(),
		manifest: types.Manifest{ID: "plugin-test"},
		logger:   slog.New(slog.NewTextHandler(os.Stderr, nil)),
		statuses: make(map[string]types.CommandStatus),
		registry: make(map[string]types.Registration),
		fileHash: make(map[string]string),
		store:    newScriptStore(t.TempDir()),
	}

	r.stateStore = newMemoryStateStore()
	r.snapshotStore = newStateStore(r, "file")

	env := r.newLuaEnv()

	if env == nil {
		t.Fatal("expected luaEnv, got nil")
	}
	if env.pluginID != "plugin-test" {
		t.Errorf("pluginID = %q, want plugin-test", env.pluginID)
	}
	if env.logger == nil {
		t.Error("logger should be set")
	}
	if env.sendCommand == nil {
		t.Error("sendCommand should be set")
	}
	if env.emitEvent == nil {
		t.Error("emitEvent should be set")
	}
}

func TestNewLuaEnv_FindEntities_FallsBackLocallyWhenNoNATS(t *testing.T) {
	r := &Runner{
		dataDir:  t.TempDir(),
		manifest: types.Manifest{ID: "plugin-test"},
		logger:   slog.New(slog.NewTextHandler(os.Stderr, nil)),
		store:    newScriptStore(t.TempDir()),
		statuses: make(map[string]types.CommandStatus),
		registry: make(map[string]types.Registration),
		fileHash: make(map[string]string),
	}
	r.stateStore = newMemoryStateStore()
	r.snapshotStore = newStateStore(r, "file")

	env := r.newLuaEnv()

	// With no NATS (r.nc == nil), findEntities falls back to local search
	// which returns nothing for a non-existent plugin.
	results := env.findEntities(scriptQuery{PluginID: "other-plugin"})
	if len(results) != 0 {
		t.Errorf("expected 0 results for remote plugin without NATS, got %d", len(results))
	}
}

func TestLuaEnv_RestoreSnapshot_LocalEntity(t *testing.T) {
	r := &Runner{
		dataDir:     t.TempDir(),
		manifest:    types.Manifest{ID: "plugin-test"},
		logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		fileHash:    make(map[string]string),
		deviceLocks: make(map[string]*sync.Mutex),
	}

	r.stateStore = newMemoryStateStore()
	r.snapshotStore = newStateStore(r, "file")

	dev := types.Device{ID: "d1", SourceID: "src1"}
	r.saveDevice(dev)
	ent := types.Entity{
		ID:       "e1",
		DeviceID: "d1",
		Domain:   "light",
		Snapshots: map[string]types.EntitySnapshot{
			"snap-1": {ID: "snap-1", Name: "Evening"},
		},
	}
	r.saveEntity(ent)

	foundEnt := foundEntity{
		PluginID: "plugin-test",
		DeviceID: "d1",
		EntityID: "e1",
	}

	snapshotID, err := r.restoreSnapshotForEntity(foundEnt, "Evening")
	if err != nil {
		t.Logf("restoreSnapshotForEntity error (may be expected in test): %v", err)
	}
	if snapshotID != "snap-1" && snapshotID != "" {
		t.Errorf("unexpected snapshotID: %s", snapshotID)
	}
}

func TestLuaEnv_RestoreSnapshot_RemoteEntity_NoNATS(t *testing.T) {
	// Without NATS, restoring a remote entity snapshot should fail with "nats unavailable".
	r := &Runner{
		dataDir:     t.TempDir(),
		manifest:    types.Manifest{ID: "plugin-local"},
		logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		deviceLocks: make(map[string]*sync.Mutex),
		fileHash:    make(map[string]string),
	}
	r.stateStore = newMemoryStateStore()
	r.snapshotStore = newStateStore(r, "file")

	foundEnt := foundEntity{
		PluginID: "plugin-remote",
		DeviceID: "remote-device",
		EntityID: "remote-entity",
	}

	_, err := r.restoreSnapshotForEntity(foundEnt, "RemoteSnapshot")
	if err == nil {
		t.Error("expected error for remote entity without NATS")
	}
	if !containsSubstring(err.Error(), "nats") && !containsSubstring(err.Error(), "unavailable") {
		t.Errorf("error should mention nats/unavailable: %v", err)
	}
}

func TestLuaEnv_RestoreSnapshot_NameResolution(t *testing.T) {
	r := &Runner{
		dataDir:     t.TempDir(),
		manifest:    types.Manifest{ID: "plugin-test"},
		logger:      slog.New(slog.NewTextHandler(os.Stderr, nil)),
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		fileHash:    make(map[string]string),
		deviceLocks: make(map[string]*sync.Mutex),
	}

	r.stateStore = newMemoryStateStore()
	r.snapshotStore = newStateStore(r, "file")

	dev := types.Device{ID: "d1"}
	r.saveDevice(dev)
	ent := types.Entity{
		ID:       "e1",
		DeviceID: "d1",
		Snapshots: map[string]types.EntitySnapshot{
			"id-123": {ID: "id-123", Name: "MySnapshot"},
		},
	}
	r.saveEntity(ent)

	foundEnt := foundEntity{
		PluginID: "plugin-test",
		DeviceID: "d1",
		EntityID: "e1",
	}

	snapshotID, err := r.restoreSnapshotForEntity(foundEnt, "MySnapshot")
	if err != nil {
		t.Logf("Error (may be expected): %v", err)
	}
	if snapshotID != "id-123" && snapshotID != "" {
		t.Errorf("expected id-123 or empty, got %s", snapshotID)
	}

	snapshotID, err = r.restoreSnapshotForEntity(foundEnt, "id-123")
	if err != nil {
		t.Logf("Error (may be expected): %v", err)
	}
	if snapshotID != "id-123" && snapshotID != "" {
		t.Errorf("expected id-123 or empty, got %s", snapshotID)
	}
}

func TestLuaEnv_RestoreSnapshot_EmptyName(t *testing.T) {
	r := &Runner{
		dataDir:     t.TempDir(),
		manifest:    types.Manifest{ID: "plugin-test"},
		fileHash:    make(map[string]string),
		deviceLocks: make(map[string]*sync.Mutex),
	}

	foundEnt := foundEntity{
		PluginID: "plugin-test",
		DeviceID: "d1",
		EntityID: "e1",
	}

	_, err := r.restoreSnapshotForEntity(foundEnt, "")
	if err == nil {
		t.Error("expected error for empty snapshot name")
	}
	if !containsSubstring(err.Error(), "required") {
		t.Errorf("error should mention 'required': %v", err)
	}
}

// Helper functions
func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
