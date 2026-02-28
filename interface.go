package runner

import "github.com/slidebolt/sdk-types"

// Config is passed to a plugin during OnInitialize.
type Config struct {
	DataDir   string
	EventSink EventSink
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
	OnShutdown()
	OnHealthCheck() (string, error)
	OnStorageUpdate(current types.Storage) (types.Storage, error)

	// Devices
	OnDeviceCreate(device types.Device) (types.Device, error)
	OnDeviceUpdate(device types.Device) (types.Device, error)
	OnDeviceDelete(id string) error
	OnDevicesList(current []types.Device) ([]types.Device, error)
	OnDeviceSearch(q types.SearchQuery, results []types.Device) ([]types.Device, error)

	// Entities
	OnEntityCreate(entity types.Entity) (types.Entity, error)
	OnEntityUpdate(entity types.Entity) (types.Entity, error)
	OnEntityDelete(deviceID, entityID string) error
	OnEntitiesList(deviceID string, current []types.Entity) ([]types.Entity, error)

	// Commands and Events — EntityType is always derived from entity.Domain by
	// the runner. Plugins pattern-match on cmd.EntityType / evt.EntityType and
	// dispatch to whichever entity packages they have imported.
	OnCommand(cmd types.Command, entity types.Entity) (types.Entity, error)
	OnEvent(evt types.Event, entity types.Entity) (types.Entity, error)
}
