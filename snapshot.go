package runner

import (
	"os"
	"strconv"
	"strings"
	"time"
)

const defaultSnapshotInterval = 30 * time.Second

func loadSnapshotInterval() time.Duration {
	raw := strings.TrimSpace(os.Getenv(EnvStateSnapshotIntervalSec))
	if raw == "" {
		return defaultSnapshotInterval
	}
	sec, err := strconv.Atoi(raw)
	if err != nil || sec <= 0 {
		return defaultSnapshotInterval
	}
	return time.Duration(sec) * time.Second
}

func (r *Runner) hydrateCanonicalFromSnapshot() {
	if r.stateStore == nil || r.snapshotStore == nil {
		return
	}
	devices, err := r.snapshotStore.LoadDevices()
	if err == nil {
		for _, dev := range devices {
			_ = r.stateStore.SaveDevice(dev)
			entities, eerr := r.snapshotStore.LoadEntities(dev.ID)
			if eerr != nil {
				continue
			}
			for _, ent := range entities {
				_ = r.stateStore.SaveEntity(ent)
			}
		}
	}
	if st, err := r.snapshotStore.LoadState("default"); err == nil {
		_ = r.stateStore.SaveState("default", st)
	}
}

func (r *Runner) startSnapshotLoop() {
	if r.snapshotStore == nil || r.snapshotInterval <= 0 {
		return
	}
	if r.snapshotStop != nil {
		return
	}
	r.snapshotStop = make(chan struct{})
	r.snapshotDone = make(chan struct{})
	go func() {
		defer close(r.snapshotDone)
		ticker := time.NewTicker(r.snapshotInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				r.flushSnapshot()
			case <-r.snapshotStop:
				return
			}
		}
	}()
}

func (r *Runner) stopSnapshotLoop() {
	if r.snapshotStop == nil {
		return
	}
	close(r.snapshotStop)
	<-r.snapshotDone
	r.snapshotStop = nil
	r.snapshotDone = nil
}

func (r *Runner) flushSnapshot() {
	if r.stateStore == nil || r.snapshotStore == nil {
		return
	}
	devices, err := r.stateStore.LoadDevices()
	if err == nil {
		for _, dev := range devices {
			_ = r.snapshotStore.SaveDevice(dev)
			entities, eerr := r.stateStore.LoadEntities(dev.ID)
			if eerr != nil {
				continue
			}
			for _, ent := range entities {
				_ = r.snapshotStore.SaveEntity(ent)
			}
		}
	}
	if st, err := r.stateStore.LoadState("default"); err == nil {
		_ = r.snapshotStore.SaveState("default", st)
	}
}
