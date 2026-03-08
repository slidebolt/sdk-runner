package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	types "github.com/slidebolt/sdk-types"
)

func newTestRunner(t *testing.T) *Runner {
	t.Helper()
	r := &Runner{
		dataDir:     t.TempDir(),
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		scripts:     make(map[scriptKey]*scriptRuntime),
		deviceLocks: make(map[string]*sync.Mutex),
		fileHash:    make(map[string]string),
	}
	r.stateStore = newStateStore(r, "file")
	return r
}

func TestWriteIfChangedSkipsIdenticalWrite(t *testing.T) {
	r := newTestRunner(t)
	path := filepath.Join(r.dataDir, "devices", "d1.json")
	payload := []byte(`{"id":"d1","name":"demo"}`)

	if err := r.writeIfChanged(path, payload); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	st1, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after first write failed: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	if err := r.writeIfChanged(path, payload); err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	st2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after second write failed: %v", err)
	}

	if !st2.ModTime().Equal(st1.ModTime()) {
		t.Fatalf("expected unchanged write to preserve modtime, got %v -> %v", st1.ModTime(), st2.ModTime())
	}
}

func TestWriteIfChangedUpdatesWhenContentDiffers(t *testing.T) {
	r := newTestRunner(t)
	path := filepath.Join(r.dataDir, "devices", "d1.json")
	a := []byte(`{"id":"d1","name":"a"}`)
	b := []byte(`{"id":"d1","name":"b"}`)

	if err := r.writeIfChanged(path, a); err != nil {
		t.Fatalf("first write failed: %v", err)
	}
	st1, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after first write failed: %v", err)
	}

	time.Sleep(20 * time.Millisecond)
	if err := r.writeIfChanged(path, b); err != nil {
		t.Fatalf("second write failed: %v", err)
	}
	st2, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat after second write failed: %v", err)
	}
	if !st2.ModTime().After(st1.ModTime()) {
		t.Fatalf("expected changed write to update modtime, got %v -> %v", st1.ModTime(), st2.ModTime())
	}
}

func TestSaveEntityConcurrentNoCorruption(t *testing.T) {
	r := newTestRunner(t)
	deviceID := "dev-1"
	entityID := "ent-1"

	const workers = 24
	const iterations = 120

	var wg sync.WaitGroup
	wg.Add(workers)
	for w := 0; w < workers; w++ {
		w := w
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				e := types.Entity{
					ID:        entityID,
					DeviceID:  deviceID,
					Domain:    "switch",
					LocalName: "entity",
					Data: types.EntityData{
						SyncStatus: types.SyncStatusSynced,
					},
				}
				if (w+i)%2 == 0 {
					e.Data.SyncStatus = types.SyncStatusPending
				}
				r.saveEntity(e)
				_ = r.loadEntity(deviceID, entityID)
				_ = r.loadEntities(deviceID)
			}
		}()
	}
	wg.Wait()

	root := filepath.Join(r.dataDir, "devices", deviceID, "entities")
	files, err := filepath.Glob(filepath.Join(root, "*.json"))
	if err != nil {
		t.Fatalf("glob failed: %v", err)
	}
	if len(files) != 1 {
		t.Fatalf("expected exactly one entity file, got %d (%v)", len(files), files)
	}

	data, err := os.ReadFile(files[0])
	if err != nil {
		t.Fatalf("read entity file failed: %v", err)
	}
	var ent types.Entity
	if err := json.Unmarshal(data, &ent); err != nil {
		t.Fatalf("entity file is corrupted JSON: %v", err)
	}
	if ent.ID != entityID || ent.DeviceID != deviceID {
		t.Fatalf("unexpected entity content: id=%q device=%q", ent.ID, ent.DeviceID)
	}
}
