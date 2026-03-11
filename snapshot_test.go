package runner

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	types "github.com/slidebolt/sdk-types"
)

func TestLoadSnapshotInterval_Default30s(t *testing.T) {
	prev := os.Getenv(EnvStateSnapshotIntervalSec)
	t.Cleanup(func() { _ = os.Setenv(EnvStateSnapshotIntervalSec, prev) })
	_ = os.Unsetenv(EnvStateSnapshotIntervalSec)
	if got := loadSnapshotInterval(); got != 30*time.Second {
		t.Fatalf("interval=%v want=30s", got)
	}
}

// TestSaveDevice_NotWrittenToFileBeforeSnapshotFlush replicates the integration
// failure TestDeviceFileWrittenOnCreate. Production NewRunner() uses a memory
// primary store; files only appear after flushSnapshot() fires (every 30s by
// default). This test FAILS until synchronous or write-through persistence is
// implemented.
func TestSaveDevice_NotWrittenToFileBeforeSnapshotFlush(t *testing.T) {
	r := newTestRunner(t)
	r.stateStore = newMemoryStateStore()       // mirrors production NewRunner()
	r.snapshotStore = newStateStore(r, "file") // mirrors production NewRunner()

	r.saveDevice(types.Device{ID: "dev-snap", SourceID: "src-snap", LocalName: "Snap Device"})
	r.saveEntity(types.Entity{ID: "ent-snap", SourceID: "src-ent-snap", DeviceID: "dev-snap", Domain: "switch"})

	deviceFile := filepath.Join(r.dataDir, "devices", "dev-snap.json")
	if _, err := os.Stat(deviceFile); err != nil {
		t.Fatalf("BUG: device file not written synchronously (mirrors TestDeviceFileWrittenOnCreate): %v", err)
	}
	entityFile := filepath.Join(r.dataDir, "devices", "dev-snap", "entities", "ent-snap.json")
	if _, err := os.Stat(entityFile); err != nil {
		t.Fatalf("BUG: entity file not written synchronously (mirrors TestDeviceFileWrittenWhenEntityCreated): %v", err)
	}
}

func TestFlushSnapshot_WritesCanonicalMemoryToFileStore(t *testing.T) {
	r := newTestRunner(t)
	r.stateStore = newMemoryStateStore()
	r.snapshotStore = newStateStore(r, "file")

	r.saveDevice(types.Device{ID: "dev-1", SourceID: "src-dev-1", LocalName: "Device 1"})
	r.saveEntity(types.Entity{ID: "ent-1", SourceID: "src-ent-1", DeviceID: "dev-1", Domain: "switch", LocalName: "Switch 1"})
	r.saveStateSynced("default", types.Storage{Meta: "m", Data: []byte(`{"ok":true}`)})

	r.flushSnapshot()

	fileStore := newStateStore(r, "file")
	dev, err := fileStore.LoadDeviceByID("dev-1")
	if err != nil || dev.ID != "dev-1" {
		t.Fatalf("expected snapshot device, err=%v dev=%#v", err, dev)
	}
	ent, err := fileStore.LoadEntity("dev-1", "ent-1")
	if err != nil || ent.ID != "ent-1" {
		t.Fatalf("expected snapshot entity, err=%v ent=%#v", err, ent)
	}
	st, err := fileStore.LoadState("default")
	if err != nil || st.Meta != "m" {
		t.Fatalf("expected snapshot state, err=%v state=%#v", err, st)
	}
}
