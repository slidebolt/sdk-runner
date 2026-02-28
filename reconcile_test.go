package runner

import (
	"testing"

	"github.com/slidebolt/sdk-types"
)

func TestReconcileDevice(t *testing.T) {
	existing := types.Device{
		ID:         "dev-123",
		SourceID:   "hw-mac-001",
		SourceName: "Old HW Name",
		LocalName:  "Basement Bar 01", // User provided
		Labels:     map[string]string{"room": "basement"},
	}

	discovered := types.Device{
		ID:         "dev-123",
		SourceID:   "hw-mac-001",
		SourceName: "New HW Firmware Name",
		LocalName:  "Should Be Ignored", // Hardware trying to set local name
		Labels:     map[string]string{"type": "light"}, // Hardware provided labels
	}

	result := ReconcileDevice(existing, discovered)

	// 1. UUID must remain stable
	if result.ID != "dev-123" {
		t.Errorf("expected ID dev-123, got %q", result.ID)
	}

	// 2. Hardware owns SourceID and SourceName (wins)
	if result.SourceName != "New HW Firmware Name" {
		t.Errorf("expected SourceName 'New HW Firmware Name', got %q", result.SourceName)
	}

	// 3. User owns LocalName (existing wins over discovered)
	if result.LocalName != "Basement Bar 01" {
		t.Errorf("The Wall was breached: expected LocalName 'Basement Bar 01', got %q", result.LocalName)
	}

	// 4. Labels should merge, existing takes precedence on conflict
	if result.Labels["room"] != "basement" {
		t.Errorf("expected room label to be preserved")
	}
	if result.Labels["type"] != "light" {
		t.Errorf("expected type label to be merged in")
	}
}

func TestReconcileDevice_EmptyLocalName(t *testing.T) {
	// If existing has NO LocalName, and discovered provides one (some plugins did this as a fallback),
	// the Wall shouldn't necessarily block it if it's the first time, but actually, 
	// discovered LocalName should be ignored completely. LocalName is ONLY set by the user via API.
	
	existing := types.Device{
		ID:       "dev-123",
		SourceID: "hw-mac-001",
	}

	discovered := types.Device{
		ID:         "dev-123",
		SourceID:   "hw-mac-001",
		SourceName: "HW Name",
		LocalName:  "Hardware Trying to Default LocalName", 
	}

	result := ReconcileDevice(existing, discovered)

	if result.LocalName != "" {
		t.Errorf("Hardware was allowed to write to LocalName: got %q", result.LocalName)
	}
}
