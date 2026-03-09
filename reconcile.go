package runner

import (
	"github.com/slidebolt/sdk-types"
)

// ReconcileDevice acts as the "Wall" between user data and hardware data.
// It merges a newly discovered device state into the existing persisted state,
// enforcing strict field ownership rules.
func ReconcileDevice(existing types.Device, discovered types.Device) types.Device {
	if discovered.SourceID == "" {
		discovered.SourceID = discovered.ID
	}
	// If it's a completely new device, just return it, but sanitize LocalName
	if existing.ID == "" {
		discovered.LocalName = "" // Hardware cannot set LocalName
		if discovered.Labels == nil {
			discovered.Labels = make(map[string][]string)
		}
		return discovered
	}

	result := existing

	// 1. Hardware owns technical identity (wins)
	result.SourceID = discovered.SourceID
	result.SourceName = discovered.SourceName

	// 2. User owns LocalName (existing always wins, discovered LocalName is ignored)
	// (result.LocalName is already set from existing)

	// 4. Merge Labels (existing user labels win over discovered hardware labels)
	if result.Labels == nil {
		result.Labels = make(map[string][]string)
	}
	for k, v := range discovered.Labels {
		if _, ok := result.Labels[k]; !ok {
			result.Labels[k] = v // Only add if not overridden by user
		}
	}

	return result
}

// ReconcileDevices is a convenience function to reconcile a list of discovered
// devices against a map of existing devices.
func ReconcileDevices(existingMap map[string]types.Device, discovered []types.Device) []types.Device {
	var out []types.Device
	for _, d := range discovered {
		existing, ok := existingMap[d.ID]
		if ok {
			out = append(out, ReconcileDevice(existing, d))
		} else {
			out = append(out, ReconcileDevice(types.Device{}, d))
		}
	}
	return out
}

// ReconcileEntity applies the same ownership wall as ReconcileDevice, but for entities.
func ReconcileEntity(existing types.Entity, discovered types.Entity) types.Entity {
	if discovered.SourceID == "" {
		discovered.SourceID = discovered.ID
	}
	// New entity from discovery: sanitize user-owned fields.
	if existing.ID == "" {
		discovered.LocalName = ""
		if discovered.Labels == nil {
			discovered.Labels = make(map[string][]string)
		}
		return discovered
	}

	result := existing

	// Hardware/plugin-owned capabilities always win.
	result.SourceID = discovered.SourceID
	result.SourceName = discovered.SourceName
	result.Domain = discovered.Domain
	result.Actions = discovered.Actions

	// User-owned LocalName stays from existing.
	// (result.LocalName already comes from existing)

	// Merge labels with user values taking precedence.
	if result.Labels == nil {
		result.Labels = make(map[string][]string)
	}
	for k, v := range discovered.Labels {
		if _, ok := result.Labels[k]; !ok {
			result.Labels[k] = v
		}
	}

	// System-owned state remains from existing.
	result.Data = existing.Data
	return result
}

func ReconcileDevicesAdditive(existing []types.Device, discovered []types.Device) []types.Device {
	existingByID := make(map[string]types.Device, len(existing))
	for _, d := range existing {
		existingByID[d.ID] = d
	}
	out := make([]types.Device, 0, len(existing)+len(discovered))
	seen := make(map[string]struct{}, len(existing)+len(discovered))
	for _, d := range discovered {
		ex := existingByID[d.ID]
		merged := ReconcileDevice(ex, d)
		out = append(out, merged)
		seen[d.ID] = struct{}{}
	}
	for _, d := range existing {
		if _, ok := seen[d.ID]; ok {
			continue
		}
		out = append(out, d)
	}
	return out
}

func ReconcileEntitiesAdditive(existing []types.Entity, discovered []types.Entity, deviceID string) []types.Entity {
	existingByID := make(map[string]types.Entity, len(existing))
	for _, e := range existing {
		existingByID[e.ID] = e
	}
	out := make([]types.Entity, 0, len(existing)+len(discovered))
	seen := make(map[string]struct{}, len(existing)+len(discovered))
	for _, e := range discovered {
		ex := existingByID[e.ID]
		merged := ReconcileEntity(ex, e)
		if merged.DeviceID == "" {
			merged.DeviceID = deviceID
		}
		out = append(out, merged)
		seen[e.ID] = struct{}{}
	}
	for _, e := range existing {
		if _, ok := seen[e.ID]; ok {
			continue
		}
		out = append(out, e)
	}
	return out
}

// EnsureCoreDevice guarantees that the plugin's management device (ID = pluginID) is present
// in the device list. Call this at the end of OnDeviceDiscover.
func EnsureCoreDevice(pluginID string, current []types.Device) []types.Device {
	coreID := types.CoreDeviceID(pluginID)
	for _, d := range current {
		if d.ID == coreID {
			return current
		}
	}
	return append(current, ReconcileDevice(types.Device{}, types.Device{
		ID:         coreID,
		SourceID:   coreID,
		SourceName: pluginID,
	}))
}

// EnsureCoreEntities guarantees that the core health entity is present for the plugin's
// management device. Call this at the start of OnEntityDiscover for every deviceID.
func EnsureCoreEntities(pluginID, deviceID string, current []types.Entity) []types.Entity {
	if deviceID != types.CoreDeviceID(pluginID) {
		return current
	}
	for _, need := range types.CoreEntities(pluginID) {
		found := false
		for _, e := range current {
			if e.ID == need.ID {
				found = true
				break
			}
		}
		if !found {
			current = append(current, need)
		}
	}
	return current
}
