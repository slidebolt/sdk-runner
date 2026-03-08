package runner

import (
	"encoding/json"
	"sync"
	"testing"

	types "github.com/slidebolt/sdk-types"
)

// newTestRunnerWithPlugin builds a test runner wired to a plugin so that
// createCommand (used by RestoreSnapshot) has a working plugin.OnCommand.
func newTestRunnerWithPlugin(t *testing.T, p Plugin) *Runner {
	t.Helper()
	r := &Runner{
		dataDir:     t.TempDir(),
		plugin:      p,
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		scripts:     make(map[scriptKey]*scriptRuntime),
		deviceLocks: make(map[string]*sync.Mutex),
		fileHash:    make(map[string]string),
	}
	r.stateStore = newStateStore(r, "file")
	return r
}

// snapshotTestPlugin is a minimal plugin that records commands dispatched to it.
type snapshotTestPlugin struct {
	createFallbackPlugin
	dispatched []types.Command
}

func (p *snapshotTestPlugin) OnCommand(cmd types.Command, entity types.Entity) (types.Entity, error) {
	p.dispatched = append(p.dispatched, cmd)
	return entity, nil
}

func seedLightEntity(t *testing.T, r *Runner) types.Entity {
	t.Helper()
	ent := types.Entity{
		ID:       "light-001",
		DeviceID: "dev-001",
		Domain:   "light",
		Data: types.EntityData{
			Effective: json.RawMessage(`{"power":true,"brightness":80,"rgb":[255,128,0]}`),
		},
	}
	r.saveEntity(ent)
	return ent
}

func TestSaveSnapshot_CreatesSnapshotOnEntity(t *testing.T) {
	r := newTestRunner(t)
	seedLightEntity(t, r)

	snap, err := r.SaveSnapshot("dev-001", "light-001", "MovieTime", map[string][]string{"room": {"living-room"}})
	if err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}
	if snap.ID == "" {
		t.Error("snapshot ID should not be empty")
	}
	if snap.Name != "MovieTime" {
		t.Errorf("snapshot Name: got %q want %q", snap.Name, "MovieTime")
	}
	if string(snap.State) != `{"power":true,"brightness":80,"rgb":[255,128,0]}` {
		t.Errorf("snapshot State: got %s", snap.State)
	}

	// Verify persisted on entity.
	loaded := r.loadEntity("dev-001", "light-001")
	if _, ok := loaded.Snapshots[snap.ID]; !ok {
		t.Errorf("snapshot %q not found on persisted entity", snap.ID)
	}
}

func TestSaveSnapshot_EntityNotFound(t *testing.T) {
	r := newTestRunner(t)
	_, err := r.SaveSnapshot("dev-missing", "ent-missing", "X", nil)
	if err == nil {
		t.Fatal("expected error for missing entity")
	}
}

func TestListSnapshots_OrderedByCreatedAt(t *testing.T) {
	r := newTestRunner(t)
	seedLightEntity(t, r)

	for _, name := range []string{"A", "B", "C"} {
		if _, err := r.SaveSnapshot("dev-001", "light-001", name, nil); err != nil {
			t.Fatalf("SaveSnapshot %q failed: %v", name, err)
		}
	}

	snaps, err := r.ListSnapshots("dev-001", "light-001")
	if err != nil {
		t.Fatalf("ListSnapshots failed: %v", err)
	}
	if len(snaps) != 3 {
		t.Fatalf("expected 3 snapshots, got %d", len(snaps))
	}
	for i := 1; i < len(snaps); i++ {
		if snaps[i].CreatedAt.Before(snaps[i-1].CreatedAt) {
			t.Errorf("snapshots not ordered by CreatedAt at index %d", i)
		}
	}
}

func TestDeleteSnapshot_RemovesFromEntity(t *testing.T) {
	r := newTestRunner(t)
	seedLightEntity(t, r)

	snap, _ := r.SaveSnapshot("dev-001", "light-001", "ToDelete", nil)

	if err := r.DeleteSnapshot("dev-001", "light-001", snap.ID); err != nil {
		t.Fatalf("DeleteSnapshot failed: %v", err)
	}

	loaded := r.loadEntity("dev-001", "light-001")
	if _, ok := loaded.Snapshots[snap.ID]; ok {
		t.Error("snapshot still present after delete")
	}
}

func TestDeleteSnapshot_NotFound(t *testing.T) {
	r := newTestRunner(t)
	seedLightEntity(t, r)
	err := r.DeleteSnapshot("dev-001", "light-001", "does-not-exist")
	if err == nil {
		t.Fatal("expected error deleting non-existent snapshot")
	}
}

func TestSnapshotsSurviveFlushAndHydrate(t *testing.T) {
	r := newTestRunner(t)
	r.stateStore = newMemoryStateStore()
	r.snapshotStore = newStateStore(r, "file")

	seedLightEntity(t, r)
	snap, err := r.SaveSnapshot("dev-001", "light-001", "Persist", nil)
	if err != nil {
		t.Fatalf("SaveSnapshot failed: %v", err)
	}

	r.flushSnapshot()

	r2 := &Runner{
		dataDir:     r.dataDir,
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		scripts:     make(map[scriptKey]*scriptRuntime),
		deviceLocks: make(map[string]*sync.Mutex),
		fileHash:    make(map[string]string),
	}
	r2.stateStore = newMemoryStateStore()
	r2.snapshotStore = newStateStore(r2, "file")
	r2.hydrateCanonicalFromSnapshot()

	loaded := r2.loadEntity("dev-001", "light-001")
	if _, ok := loaded.Snapshots[snap.ID]; !ok {
		t.Errorf("snapshot %q missing after flush+hydrate", snap.ID)
	}
}

func TestRestoreSnapshot_DispatchesCommands(t *testing.T) {
	// Register a minimal state-to-commands handler for the "light" domain
	// so the runner can decompose the snapshot state without importing sdk-entities.
	types.RegisterStateToCommands("light", func(stateJSON json.RawMessage) ([]json.RawMessage, error) {
		// Return two dummy commands to verify dispatch count.
		return []json.RawMessage{
			json.RawMessage(`{"type":"turn_on"}`),
			json.RawMessage(`{"type":"set_brightness","brightness":80}`),
		}, nil
	})

	plug := &snapshotTestPlugin{}
	r := newTestRunnerWithPlugin(t, plug)
	r.manifest = types.Manifest{ID: "test-plugin"}

	seedLightEntity(t, r)
	snap, _ := r.SaveSnapshot("dev-001", "light-001", "Movie", nil)

	if err := r.RestoreSnapshot("dev-001", "light-001", snap.ID); err != nil {
		t.Fatalf("RestoreSnapshot failed: %v", err)
	}
	if len(plug.dispatched) != 2 {
		t.Errorf("expected 2 commands dispatched, got %d", len(plug.dispatched))
	}
}

func TestRestoreSnapshot_NotFound(t *testing.T) {
	r := newTestRunner(t)
	seedLightEntity(t, r)
	err := r.RestoreSnapshot("dev-001", "light-001", "bad-uuid")
	if err == nil {
		t.Fatal("expected error restoring non-existent snapshot")
	}
}
