package runner

import (
	"context"
	"errors"
	"testing"
	"time"

	types "github.com/slidebolt/sdk-types"
)

type createFallbackPlugin struct {
	deviceErr error
	entityErr error
}

func (p *createFallbackPlugin) OnInitialize(config Config, state types.Storage) (types.Manifest, types.Storage) {
	return types.Manifest{ID: "plugin-fallback"}, state
}
func (p *createFallbackPlugin) OnReady()                            {}
func (p *createFallbackPlugin) WaitReady(ctx context.Context) error { return nil }
func (p *createFallbackPlugin) OnShutdown()                         {}
func (p *createFallbackPlugin) OnHealthCheck() (string, error)      { return "ok", nil }
func (p *createFallbackPlugin) OnConfigUpdate(current types.Storage) (types.Storage, error) {
	return current, nil
}
func (p *createFallbackPlugin) OnDeviceCreate(device types.Device) (types.Device, error) {
	if p.deviceErr != nil {
		return types.Device{}, p.deviceErr
	}
	return device, nil
}
func (p *createFallbackPlugin) OnDeviceUpdate(device types.Device) (types.Device, error) {
	return device, nil
}
func (p *createFallbackPlugin) OnDeviceDelete(id string) error { return nil }
func (p *createFallbackPlugin) OnDeviceDiscover(current []types.Device) ([]types.Device, error) {
	return current, nil
}
func (p *createFallbackPlugin) OnDeviceSearch(q types.SearchQuery, results []types.Device) ([]types.Device, error) {
	return results, nil
}
func (p *createFallbackPlugin) OnEntityCreate(entity types.Entity) (types.Entity, error) {
	if p.entityErr != nil {
		return types.Entity{}, p.entityErr
	}
	return entity, nil
}
func (p *createFallbackPlugin) OnEntityUpdate(entity types.Entity) (types.Entity, error) {
	return entity, nil
}
func (p *createFallbackPlugin) OnEntityDelete(deviceID, entityID string) error { return nil }
func (p *createFallbackPlugin) OnEntityDiscover(deviceID string, current []types.Entity) ([]types.Entity, error) {
	return current, nil
}
func (p *createFallbackPlugin) OnCommand(req types.Command, entity types.Entity) (types.Entity, error) {
	return entity, nil
}
func (p *createFallbackPlugin) OnEvent(evt types.Event, entity types.Entity) (types.Entity, error) {
	return entity, nil
}

func TestCreateDeviceWithFallback_WhenPluginRejects(t *testing.T) {
	r := newTestRunner(t)
	r.plugin = &createFallbackPlugin{deviceErr: errors.New("no synthetic")}

	created, err := r.createDeviceWithFallback(types.Device{
		ID:        "dev-1",
		SourceID:  "src-dev-1",
		LocalName: "Device 1",
	})

	if err == nil {
		t.Fatal("expected error when plugin rejects device create")
	}
	if created.ID != "dev-1" || created.SourceID != "src-dev-1" {
		t.Fatalf("unexpected fallback device: %#v", created)
	}
	if created.LocalName != "Device 1" {
		t.Fatalf("expected local_name preserved, got %#v", created)
	}
}

func TestCreateEntityWithFallback_WhenPluginRejects(t *testing.T) {
	r := newTestRunner(t)
	r.plugin = &createFallbackPlugin{entityErr: errors.New("no synthetic")}

	created, err := r.createEntityWithFallback(types.Entity{
		ID:       "ent-1",
		SourceID: "src-ent-1",
		DeviceID: "dev-1",
		Domain:   "switch",
	})

	if err == nil {
		t.Fatal("expected error when plugin rejects entity create")
	}
	if created.ID != "ent-1" || created.SourceID != "src-ent-1" || created.DeviceID != "dev-1" {
		t.Fatalf("unexpected fallback entity ids: %#v", created)
	}
	if created.LocalName == "" {
		t.Fatalf("expected fallback local_name to be populated: %#v", created)
	}
	if created.Data.SyncStatus != types.SyncStatusSynced {
		t.Fatalf("expected synced status, got %q", created.Data.SyncStatus)
	}
	if created.Data.UpdatedAt.IsZero() || time.Since(created.Data.UpdatedAt) > 10*time.Second {
		t.Fatalf("expected updated_at set near now, got %v", created.Data.UpdatedAt)
	}
}

type startupHydrationPlugin struct {
	createFallbackPlugin
	devices          []types.Device
	entitiesByDevice map[string][]types.Entity
	devicesListCalls int
	entitiesCalls    []string
}

func (p *startupHydrationPlugin) OnDeviceDiscover(current []types.Device) ([]types.Device, error) {
	p.devicesListCalls++
	return p.devices, nil
}

func (p *startupHydrationPlugin) OnEntityDiscover(deviceID string, current []types.Entity) ([]types.Entity, error) {
	p.entitiesCalls = append(p.entitiesCalls, deviceID)
	return p.entitiesByDevice[deviceID], nil
}

func TestBootstrapStartupTopology_HydratesEntitiesForAllDevices(t *testing.T) {
	r := newTestRunner(t)
	p := &startupHydrationPlugin{
		devices: []types.Device{
			{ID: "dev-a", SourceID: "src-a"},
			{ID: "dev-b", SourceID: "src-b"},
		},
		entitiesByDevice: map[string][]types.Entity{
			"dev-a": {{ID: "ent-a1", SourceID: "src-ent-a1", DeviceID: "dev-a", Domain: "switch"}},
			"dev-b": {{ID: "ent-b1", SourceID: "src-ent-b1", DeviceID: "dev-b", Domain: "light"}},
		},
	}
	r.plugin = p

	if err := r.bootstrapStartupTopology(); err != nil {
		t.Fatalf("bootstrapStartupTopology err=%v", err)
	}
	if p.devicesListCalls != 1 {
		t.Fatalf("OnDeviceDiscover calls=%d want=1", p.devicesListCalls)
	}
	if len(p.entitiesCalls) != 2 {
		t.Fatalf("OnEntityDiscover calls=%d want=2 (%v)", len(p.entitiesCalls), p.entitiesCalls)
	}
	called := map[string]bool{}
	for _, d := range p.entitiesCalls {
		called[d] = true
	}
	if !called["dev-a"] || !called["dev-b"] {
		t.Fatalf("OnEntityDiscover missing expected devices: %v", p.entitiesCalls)
	}

	if got := r.loadEntity("dev-a", "ent-a1"); got.ID == "" {
		t.Fatalf("expected persisted entity dev-a/ent-a1, got %#v", got)
	}
	if got := r.loadEntity("dev-b", "ent-b1"); got.ID == "" {
		t.Fatalf("expected persisted entity dev-b/ent-b1, got %#v", got)
	}
}
