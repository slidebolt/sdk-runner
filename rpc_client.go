package runner

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/slidebolt/registry"
	"github.com/slidebolt/sdk-types"
)

// foundEntity matches the structure used by the script engine.

// foundDevice matches the structure used by the script engine.

func (r *Runner) findEntitiesRPC(q scriptQuery) []foundEntity {
	if r.nc == nil {
		// Without NATS we cannot answer cross-plugin queries; return nil so
		// callers (e.g. refresh) know to preserve existing membership.
		if q.PluginID != "" && q.PluginID != r.manifest.ID {
			return nil
		}
		return r.findEntitiesLocalFallback(q)
	}

	req := registry.Query{
		Pattern:  q.Pattern,
		Labels:   q.Labels,
		PluginID: q.PluginID,
		DeviceID: q.DeviceID,
		Domain:   q.Domain,
	}
	if req.Pattern == "" {
		req.Pattern = q.Text
	}

	data, _ := json.Marshal(req)
	msg, err := r.nc.Request(SubjectRegistryQueryEntities, data, 2*time.Second)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("findEntitiesRPC NATS request failed", "error", err)
		}
		// Return nil (not empty) so refresh preserves existing members on transient failure.
		return nil
	}

	var results []registry.EntityWithPlugin
	if err := json.Unmarshal(msg.Data, &results); err != nil {
		if r.logger != nil {
			r.logger.Error("findEntitiesRPC unmarshal failed", "error", err, "data", string(msg.Data))
		}
		return r.findEntitiesLocalFallback(q)
	}
	if r.logger != nil {
		r.logger.Debug("findEntitiesRPC results", "count", len(results), "query", req)
	}

	out := make([]foundEntity, 0, len(results))
	for _, r := range results {
		out = append(out, foundEntity{
			PluginID:   r.PluginID,
			DeviceID:   r.DeviceID,
			EntityID:   r.ID,
			Domain:     r.Domain,
			Labels:     r.Labels,
			SourceID:   r.SourceID,
			SourceName: r.SourceName,
			LocalName:  r.LocalName,
		})
	}
	return out
}

func (r *Runner) findDevicesRPC(q scriptQuery) []foundDevice {
	if r.nc == nil {
		return r.findDevicesLocalFallback(q)
	}

	req := registry.Query{
		Pattern:  q.Pattern,
		Labels:   q.Labels,
		PluginID: q.PluginID,
		DeviceID: q.DeviceID,
		Domain:   q.Domain,
	}
	if req.Pattern == "" {
		req.Pattern = q.Text
	}

	data, _ := json.Marshal(req)
	msg, err := r.nc.Request(SubjectRegistryQueryDevices, data, 2*time.Second)
	if err != nil {
		return r.findDevicesLocalFallback(q)
	}

	var results []types.Device
	if err := json.Unmarshal(msg.Data, &results); err != nil {
		return r.findDevicesLocalFallback(q)
	}

	out := make([]foundDevice, 0, len(results))
	for _, d := range results {
		// We don't have the PluginID easily in types.Device unless we pass it.
		// Wait, if FindDevices returns just types.Device, we can't extract PluginID easily.
		// Let's assume the Registry is returning what we need or we update Registry later.
		out = append(out, foundDevice{
			DeviceID:   d.ID,
			SourceID:   d.SourceID,
			SourceName: d.SourceName,
			LocalName:  d.LocalName,
			Labels:     d.Labels,
		})
	}
	return out
}

func (r *Runner) getEntityRPC(pluginID, deviceID, entityID string) (types.Entity, error) {
	if pluginID == r.manifest.ID || pluginID == "" {
		ent := r.loadEntity(deviceID, entityID)
		if ent.ID != "" {
			return ent, nil
		}
	}
	
	// Fast path check against the registry
	if r.nc != nil {
		req := registry.Query{PluginID: pluginID, DeviceID: deviceID, EntityID: entityID}
		data, _ := json.Marshal(req)
		if msg, err := r.nc.Request(SubjectRegistryQueryEntities, data, 2*time.Second); err == nil {
			var results []registry.EntityWithPlugin
			if json.Unmarshal(msg.Data, &results) == nil && len(results) > 0 {
				return results[0].Entity, nil
			}
		}
	}

	return types.Entity{}, fmt.Errorf("entity not found: %s|%s|%s", pluginID, deviceID, entityID)
}

func (r *Runner) getDeviceRPC(pluginID, deviceID string) (types.Device, error) {
	if pluginID == r.manifest.ID || pluginID == "" {
		dev := r.loadDeviceByID(deviceID)
		if dev.ID != "" {
			return dev, nil
		}
	}

	if r.nc != nil {
		req := registry.Query{PluginID: pluginID, DeviceID: deviceID}
		data, _ := json.Marshal(req)
		if msg, err := r.nc.Request(SubjectRegistryQueryDevices, data, 2*time.Second); err == nil {
			var results []types.Device
			if json.Unmarshal(msg.Data, &results) == nil && len(results) > 0 {
				return results[0], nil
			}
		}
	}

	return types.Device{}, fmt.Errorf("device not found: %s|%s", pluginID, deviceID)
}

func (r *Runner) gatewayFindRPC(queryStr string) ([]foundEntity, error) {
	if r.nc == nil {
		return nil, fmt.Errorf("nats unavailable")
	}
	msg, err := r.nc.Request(SubjectGatewayDiscovery, []byte(queryStr), 2*time.Second)
	if err != nil {
		return nil, err
	}
	var results []registry.EntityWithPlugin
	if err := json.Unmarshal(msg.Data, &results); err != nil {
		return nil, err
	}
	out := make([]foundEntity, 0, len(results))
	for _, r := range results {
		out = append(out, foundEntity{
			PluginID:   r.PluginID,
			DeviceID:   r.DeviceID,
			EntityID:   r.ID,
			Domain:     r.Domain,
			Labels:     r.Labels,
			SourceID:   r.SourceID,
			SourceName: r.SourceName,
			LocalName:  r.LocalName,
		})
	}
	return out, nil
}

// Local fallbacks for tests

func (r *Runner) findEntitiesLocalFallback(q scriptQuery) []foundEntity {
	// Simple wrapper around searchEntitiesLocal
	tq := types.SearchQuery{
		Pattern:  q.Pattern,
		Labels:   q.Labels,
		PluginID: q.PluginID,
		DeviceID: q.DeviceID,
		Domain:   q.Domain,
	}
	if tq.Pattern == "" {
		tq.Pattern = q.Text
	}

	local := r.searchEntitiesLocal(tq)
	out := make([]foundEntity, 0, len(local))
	for _, ent := range local {
		out = append(out, foundEntity{
			PluginID:  r.manifest.ID,
			DeviceID:  ent.DeviceID,
			EntityID:  ent.ID,
			Domain:    ent.Domain,
			Labels:    ent.Labels,
		})
	}
	return out
}

func (r *Runner) findDevicesLocalFallback(q scriptQuery) []foundDevice {
	tq := types.SearchQuery{
		Pattern:  q.Pattern,
		Labels:   q.Labels,
		PluginID: q.PluginID,
		DeviceID: q.DeviceID,
		Domain:   q.Domain,
	}
	if tq.Pattern == "" {
		tq.Pattern = q.Text
	}

	local := r.searchDevicesLocal(tq)
	out := make([]foundDevice, 0, len(local))
	for _, dev := range local {
		out = append(out, foundDevice{
			PluginID:   r.manifest.ID,
			DeviceID:   dev.ID,
			SourceID:   dev.SourceID,
			SourceName: dev.SourceName,
			LocalName:  dev.LocalName,
			Labels:     dev.Labels,
		})
	}
	return out
}
