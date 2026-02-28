package runner

import (
	"github.com/slidebolt/sdk-types"
)

// ReconcileDevice acts as the "Wall" between user data and hardware data.
// It merges a newly discovered device state into the existing persisted state,
// enforcing strict field ownership rules.
func ReconcileDevice(existing types.Device, discovered types.Device) types.Device {
	// If it's a completely new device, just return it, but sanitize LocalName
	if existing.ID == "" {
		discovered.LocalName = "" // Hardware cannot set LocalName
		if discovered.Labels == nil {
			discovered.Labels = make(map[string]string)
		}
		return discovered
	}

	result := existing

	// 1. Hardware owns technical identity (wins)
	result.SourceID = discovered.SourceID
	result.SourceName = discovered.SourceName

	// 2. Hardware owns configuration (wins)
	// We assume plugins provide the complete config during discovery
	result.Config = discovered.Config

	// 3. User owns LocalName (existing always wins, discovered LocalName is ignored)
	// (result.LocalName is already set from existing)

	// 4. Merge Labels (existing user labels win over discovered hardware labels)
	if result.Labels == nil {
		result.Labels = make(map[string]string)
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
