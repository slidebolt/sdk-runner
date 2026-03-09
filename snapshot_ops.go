package runner

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	types "github.com/slidebolt/sdk-types"
)

// compactJSON normalizes a JSON blob to compact form. Returns a copy of the
// input unchanged if it is not valid JSON.
func compactJSON(raw json.RawMessage) json.RawMessage {
	var buf bytes.Buffer
	if err := json.Compact(&buf, raw); err != nil {
		return append([]byte(nil), raw...)
	}
	return buf.Bytes()
}

// newSnapshotID generates a random UUID v4.
func newSnapshotID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40 // version 4
	b[8] = (b[8] & 0x3f) | 0x80 // variant bits
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// CreateSnapshot captures the current Effective state of an entity as a named
// snapshot. Returns the created EntitySnapshot with its assigned UUID.
func (r *Runner) CreateSnapshot(deviceID, entityID, name string, labels map[string][]string) (types.EntitySnapshot, error) {
	ent := r.loadEntity(deviceID, entityID)
	if ent.ID == "" {
		return types.EntitySnapshot{}, fmt.Errorf("entity not found: %s/%s", deviceID, entityID)
	}

	state := compactJSON(ent.Data.Effective)

	snap := types.EntitySnapshot{
		ID:        newSnapshotID(),
		Name:      name,
		State:     state,
		Labels:    labels,
		CreatedAt: time.Now().UTC(),
	}

	if ent.Snapshots == nil {
		ent.Snapshots = make(map[string]types.EntitySnapshot)
	}
	ent.Snapshots[snap.ID] = snap
	r.saveEntity(ent)
	return snap, nil
}

// ListSnapshots returns all snapshots for an entity, ordered by creation time.
func (r *Runner) ListSnapshots(deviceID, entityID string) ([]types.EntitySnapshot, error) {
	ent := r.loadEntity(deviceID, entityID)
	if ent.ID == "" {
		return nil, fmt.Errorf("entity not found: %s/%s", deviceID, entityID)
	}

	out := make([]types.EntitySnapshot, 0, len(ent.Snapshots))
	for _, s := range ent.Snapshots {
		out = append(out, s)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].CreatedAt.Before(out[j].CreatedAt)
	})
	return out, nil
}

// DeleteSnapshot removes a snapshot by ID from an entity.
func (r *Runner) DeleteSnapshot(deviceID, entityID, snapshotID string) error {
	ent := r.loadEntity(deviceID, entityID)
	if ent.ID == "" {
		return fmt.Errorf("entity not found: %s/%s", deviceID, entityID)
	}
	if _, ok := ent.Snapshots[snapshotID]; !ok {
		return fmt.Errorf("snapshot not found: %s", snapshotID)
	}
	delete(ent.Snapshots, snapshotID)
	r.saveEntity(ent)
	return nil
}

// RestoreSnapshot replays the saved state as commands through the normal
// OnCommand path. The domain must have a registered StateToCommands handler
// (registered by entity packages at init time via types.RegisterStateToCommands).
func (r *Runner) RestoreSnapshot(deviceID, entityID, snapshotID string) error {
	ent := r.loadEntity(deviceID, entityID)
	if ent.ID == "" {
		return fmt.Errorf("entity not found: %s/%s", deviceID, entityID)
	}
	snap, ok := ent.Snapshots[snapshotID]
	if !ok {
		return fmt.Errorf("snapshot not found: %s", snapshotID)
	}
	if len(snap.State) == 0 {
		return nil
	}

	payloads, err := types.StateToCommands(ent.Domain, snap.State)
	if err != nil {
		return fmt.Errorf("state-to-commands for domain %q: %w", ent.Domain, err)
	}

	for _, payload := range payloads {
		p, err := json.Marshal(json.RawMessage(payload))
		if err != nil {
			return err
		}
		if _, err := r.createCommand(deviceID, entityID, p); err != nil {
			return err
		}
	}
	return nil
}
