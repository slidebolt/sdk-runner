package runner

import (
	"os"
	"path/filepath"
	"sync"
	"testing"

	types "github.com/slidebolt/sdk-types"
)
func TestMemoryStateStore_DeviceEntityRoundTrip(t *testing.T) {
	r := newTestRunner(t)
	r.stateStore = newMemoryStateStore()

	r.saveDevice(types.Device{ID: "dev-1", SourceID: "src-dev-1", LocalName: "Device 1"})
	r.saveEntity(types.Entity{ID: "ent-1", SourceID: "src-ent-1", DeviceID: "dev-1", Domain: "switch", LocalName: "Switch 1"})

	gotDev := r.loadDeviceByID("dev-1")
	if gotDev.ID != "dev-1" || gotDev.SourceID != "src-dev-1" {
		t.Fatalf("unexpected device: %#v", gotDev)
	}
	gotEnt := r.loadEntity("dev-1", "ent-1")
	if gotEnt.ID != "ent-1" || gotEnt.DeviceID != "dev-1" || gotEnt.SourceID != "src-ent-1" {
		t.Fatalf("unexpected entity: %#v", gotEnt)
	}
}

func TestMemoryStateStore_DoesNotWriteCanonicalFiles(t *testing.T) {
	r := newTestRunner(t)
	r.stateStore = newMemoryStateStore()

	r.saveDevice(types.Device{ID: "dev-1", SourceID: "src-dev-1", LocalName: "Device 1"})
	r.saveEntity(types.Entity{ID: "ent-1", SourceID: "src-ent-1", DeviceID: "dev-1", Domain: "switch", LocalName: "Switch 1"})
	r.saveStateSynced("default", types.Storage{Meta: "m", Data: []byte(`{"ok":true}`)})

	if _, err := os.Stat(filepath.Join(r.dataDir, "devices", "dev-1.json")); !os.IsNotExist(err) {
		t.Fatalf("expected no canonical device file in memory mode, err=%v", err)
	}
		if _, err := os.Stat(filepath.Join(r.dataDir, "default.json")); !os.IsNotExist(err) {
			t.Fatalf("expected no canonical state file in memory mode, err=%v", err)
		}
	}
	
	func TestEnsureStateStore_DefaultsToFile(t *testing.T) {
		r := &Runner{
			dataDir: t.TempDir(),
		}
		r.deviceLocks = make(map[string]*sync.Mutex)
		r.fileHash = make(map[string]string)
		// r.stateStore is nil here, so ensureStateStore() will initialize it.
			r.saveDevice(types.Device{ID: "persist-dev"})
	
		path := filepath.Join(r.dataDir, "devices", "persist-dev.json")
		if _, err := os.Stat(path); os.IsNotExist(err) {
			t.Errorf("BUG: ensureStateStore defaulted to memory, file %s was not written", path)
		}
	}
	
