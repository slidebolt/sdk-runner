package runner

import (
	"context"
	"encoding/json"
	"log/slog"

	"github.com/slidebolt/sdk-types"
)

// RawStore provides plugin-private raw data storage.
// Only the plugin that owns a device/entity may read or write its raw data.
// Raw files live under {dataDir}/raw/ and are invisible to the canonical store.
type RawStore interface {
	ReadRawDevice(deviceID string) (json.RawMessage, error)
	WriteRawDevice(deviceID string, data json.RawMessage) error
	ReadRawEntity(deviceID, entityID string) (json.RawMessage, error)
	WriteRawEntity(deviceID, entityID string, data json.RawMessage) error
}

// Config is passed to a plugin during OnInitialize.
type Config struct {
	DataDir   string
	EventSink EventSink
	RawStore  RawStore
	Logger    *slog.Logger
}

// EventSink allows plugin code to emit device-originated events back into
// the system after completing last-mile work (e.g. after an HTTP call to
// a real device confirms a command was applied).
type EventSink interface {
	EmitEvent(evt types.InboundEvent) error
}

// Plugin is the interface every plugin must implement.
// The runner handles all NATS wiring, storage, and command/event lifecycle —
// the plugin only implements the domain logic for each hook.
type Plugin interface {
	// Lifecycle
	OnInitialize(config Config, state types.Storage) (types.Manifest, types.Storage)
	OnReady()
	// WaitReady blocks until the plugin is fully initialised and ready to serve
	// traffic. Unlike the other lifecycle hooks it is not a callback — the runner
	// calls it synchronously after OnReady and will not proceed until it returns.
	WaitReady(ctx context.Context) error
	OnShutdown()
	OnHealthCheck() (string, error)
	OnConfigUpdate(current types.Storage) (types.Storage, error)

	// Devices
	OnDeviceCreate(device types.Device) (types.Device, error)
	OnDeviceUpdate(device types.Device) (types.Device, error)
	OnDeviceDelete(id string) error
	OnDeviceDiscover(current []types.Device) ([]types.Device, error)
	OnDeviceSearch(q types.SearchQuery, results []types.Device) ([]types.Device, error)

	// Entities
	OnEntityCreate(entity types.Entity) (types.Entity, error)
	OnEntityUpdate(entity types.Entity) (types.Entity, error)
	OnEntityDelete(deviceID, entityID string) error
	OnEntityDiscover(deviceID string, current []types.Entity) ([]types.Entity, error)

	// Commands and Events
	OnCommand(req types.Command, entity types.Entity) (types.Entity, error)
	OnEvent(evt types.Event, entity types.Entity) (types.Entity, error)
}
