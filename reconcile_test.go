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
		Labels:     map[string][]string{"room": {"basement"}},
	}

	discovered := types.Device{
		ID:         "dev-123",
		SourceID:   "hw-mac-001",
		SourceName: "New HW Firmware Name",
		LocalName:  "Should Be Ignored",                    // Hardware trying to set local name
		Labels:     map[string][]string{"type": {"light"}}, // Hardware provided labels
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
	if len(result.Labels["room"]) == 0 || result.Labels["room"][0] != "basement" {
		t.Errorf("expected room label to be preserved, got %v", result.Labels["room"])
	}
	if len(result.Labels["type"]) == 0 || result.Labels["type"][0] != "light" {
		t.Errorf("expected type label to be merged in, got %v", result.Labels["type"])
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

func TestReconcileEntity(t *testing.T) {
	existing := types.Entity{
		ID:        "ent-123",
		DeviceID:  "dev-1",
		Domain:    "switch",
		LocalName: "Desk Lamp",
		Actions:   []string{"toggle"},
		Data: types.EntityData{
			SyncStatus: "in_sync",
		},
		Labels: map[string][]string{"room": {"office"}},
	}

	discovered := types.Entity{
		ID:        "ent-123",
		DeviceID:  "dev-1",
		Domain:    "light",
		LocalName: "Ignored By Wall",
		Actions:   []string{"turn_on", "turn_off"},
		Data: types.EntityData{
			SyncStatus: "pending",
		},
		Labels: map[string][]string{"type": {"lamp"}},
	}

	result := ReconcileEntity(existing, discovered)
	if result.Domain != "light" {
		t.Fatalf("expected discovered domain to win, got %q", result.Domain)
	}
	if len(result.Actions) != 2 || result.Actions[0] != "turn_on" {
		t.Fatalf("expected discovered actions to win, got %#v", result.Actions)
	}
	if result.LocalName != "Desk Lamp" {
		t.Fatalf("expected user local name to be preserved, got %q", result.LocalName)
	}
	if result.Data.SyncStatus != "in_sync" {
		t.Fatalf("expected existing data to be preserved, got %q", result.Data.SyncStatus)
	}
	if len(result.Labels["room"]) == 0 || result.Labels["room"][0] != "office" {
		t.Fatalf("expected existing labels to be preserved, got %#v", result.Labels)
	}
	if len(result.Labels["type"]) == 0 || result.Labels["type"][0] != "lamp" {
		t.Fatalf("expected discovered labels to be merged, got %#v", result.Labels)
	}
}

func TestReconcileEntitiesAdditive_PreservesMissingFromDiscovery(t *testing.T) {
	existing := []types.Entity{
		{ID: "a", DeviceID: "dev-1", Domain: "switch"},
		{ID: "b", DeviceID: "dev-1", Domain: "switch"},
	}
	discovered := []types.Entity{
		{ID: "a", DeviceID: "dev-1", Domain: "light", Actions: []string{"turn_on"}},
	}

	out := ReconcileEntitiesAdditive(existing, discovered, "dev-1")
	if len(out) != 2 {
		t.Fatalf("expected 2 entities after additive reconcile, got %d", len(out))
	}
	seenA := false
	seenB := false
	for _, e := range out {
		if e.ID == "a" {
			seenA = true
			if e.Domain != "light" {
				t.Fatalf("expected entity a to be refreshed, got domain %q", e.Domain)
			}
		}
		if e.ID == "b" {
			seenB = true
		}
	}
	if !seenA || !seenB {
		t.Fatalf("expected both entities to remain, got %#v", out)
	}
}
