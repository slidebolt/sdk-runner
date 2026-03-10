package runner

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	types "github.com/slidebolt/sdk-types"
)

// luaEnv holds the external capabilities that Lua scripts can access via the Ctx object.
// Using a concrete struct with func fields makes it straightforward to construct
// in tests without a full Runner.
type luaEnv struct {
	findEntities    func(q scriptQuery) []foundEntity
	findDevices     func(q scriptQuery) []foundDevice
	getEntity       func(pluginID, deviceID, entityID string) (types.Entity, error)
	getDevice       func(pluginID, deviceID string) (types.Device, error)
	gatewayFind     func(queryStr string) ([]foundEntity, error)
	sendCommand     func(pluginID, deviceID, entityID string, payload json.RawMessage) (types.CommandStatus, error)
	emitEvent       func(evt types.InboundEvent) error
	saveScriptState func(deviceID, entityID string, state map[string]any)
	loadEntity      func(deviceID, entityID string) types.Entity
	restoreSnapshot func(ent foundEntity, nameOrID string) (string, error)
	logger          *slog.Logger
	pluginID        string
}

// newLuaEnv constructs a luaEnv wired to the given Runner.
// Called from loadScriptRuntime;
func (r *Runner) newLuaEnv() *luaEnv {
	return &luaEnv{
		findEntities:    r.findEntitiesRPC,
		findDevices:     r.findDevicesRPC,
		getEntity:       r.getEntityRPC,
		getDevice:       r.getDeviceRPC,
		gatewayFind:     r.gatewayFindRPC,
		sendCommand:     r.callCreateCommand,
		emitEvent:       r.EmitEvent,
		saveScriptState: func(deviceID, entityID string, state map[string]any) {
			r.store.saveState(deviceID, entityID, state, r.writeIfChanged)
		},
		loadEntity:      r.loadEntity,
		restoreSnapshot: r.restoreSnapshotForEntity,
		logger:          r.logger,
		pluginID:        r.manifest.ID,
	}
}

// restoreSnapshotForEntity provides unified local/remote snapshot restoration for Lua scripts.
// Returns the resolved snapshotID and any error.
func (r *Runner) restoreSnapshotForEntity(ent foundEntity, nameOrID string) (string, error) {
	nameOrID = strings.TrimSpace(nameOrID)
	if nameOrID == "" {
		return "", fmt.Errorf("snapshot name/id is required")
	}
	if strings.TrimSpace(ent.PluginID) == r.manifest.ID {
		snapshotID, err := r.resolveSnapshotID(ent.DeviceID, ent.EntityID, nameOrID)
		if err != nil {
			return "", err
		}
		return snapshotID, r.RestoreSnapshot(ent.DeviceID, ent.EntityID, snapshotID)
	}
	// Remote entity: resolve snapshot name via RPC then restore.
	if r.nc == nil {
		return "", fmt.Errorf("nats unavailable")
	}
	entity, err := r.getEntityRPC(ent.PluginID, ent.DeviceID, ent.EntityID)
	if err != nil {
		return "", err
	}
	remoteID := nameOrID
	if _, ok := entity.Snapshots[nameOrID]; !ok {
		for sid, snap := range entity.Snapshots {
			if strings.TrimSpace(snap.Name) == nameOrID {
				remoteID = sid
				break
			}
		}
	}
	if _, err := r.callRPC(ent.PluginID, "entities/snapshots/restore", map[string]any{
		"device_id":   ent.DeviceID,
		"entity_id":   ent.EntityID,
		"snapshot_id": remoteID,
	}, 2*time.Second); err != nil {
		return "", err
	}
	return remoteID, nil
}
