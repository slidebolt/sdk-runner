package runner

import (
	"os"
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
