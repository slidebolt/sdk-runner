package runner

import (
	"encoding/json"
	"testing"

	"github.com/slidebolt/sdk-entities/light"
	types "github.com/slidebolt/sdk-types"
)

// seedLightEntity registers a minimal light entity in the runner's registry
// so processInboundEvent can find it.
func seedLightEntity(t *testing.T, r *Runner, deviceID, entityID string) {
	t.Helper()
	dev := types.Device{ID: deviceID, SourceID: deviceID}
	if err := r.reg.SaveDevice(dev); err != nil {
		t.Fatalf("SaveDevice: %v", err)
	}
	ent := types.Entity{
		ID:       entityID,
		DeviceID: deviceID,
		Domain:   light.Type,
		Actions:  []string{light.ActionTurnOn, light.ActionTurnOff, light.ActionSetBrightness, light.ActionSetRGB, light.ActionSetTemperature},
	}
	if err := r.reg.SaveEntity(ent); err != nil {
		t.Fatalf("SaveEntity: %v", err)
	}
}

// TestProcessInboundEvent_AcceptsNewFormatPayload asserts that sdk-runner
// accepts a new-format state payload (power: bool, no type field).
// FAILS today because requiredEventType enforces the legacy "type" field.
func TestProcessInboundEvent_AcceptsNewFormatPayload(t *testing.T) {
	r := newTestRunner(t)
	seedLightEntity(t, r, "dev1", "light")

	newFormat, _ := json.Marshal(map[string]any{
		"power":      true,
		"brightness": 50,
		"rgb":        []int{255, 0, 0},
	})

	updated, err := r.processInboundEvent(types.InboundEvent{
		DeviceID: "dev1",
		EntityID: "light",
		Payload:  json.RawMessage(newFormat),
	})
	if err != nil {
		t.Fatalf("sdk-runner rejected valid new-format payload: %v", err)
	}

	var stored map[string]any
	if err := json.Unmarshal(updated.Data.Reported, &stored); err != nil {
		t.Fatalf("unmarshal reported: %v", err)
	}
	if _, hasPower := stored["power"]; !hasPower {
		t.Errorf("reported state must have 'power' field, got: %v", stored)
	}
	if _, hasType := stored["type"]; hasType {
		t.Errorf("reported state must not have legacy 'type' field, got: %v", stored)
	}
}

// TestProcessInboundEvent_RejectsOldFormatPayload asserts that sdk-runner
// rejects old-format payloads (type: "turn_on") and does not store them.
// FAILS today because the runner requires and accepts the legacy "type" field.
func TestProcessInboundEvent_RejectsOldFormatPayload(t *testing.T) {
	r := newTestRunner(t)
	seedLightEntity(t, r, "dev1", "light")

	oldFormat, _ := json.Marshal(map[string]any{
		"type":       "turn_on",
		"brightness": 50,
		"color_mode": "COLOR_MODE_RGB",
	})

	_, err := r.processInboundEvent(types.InboundEvent{
		DeviceID: "dev1",
		EntityID: "light",
		Payload:  json.RawMessage(oldFormat),
	})
	if err == nil {
		t.Fatal("sdk-runner accepted old-format payload with legacy 'type' field — should be rejected")
	}
}

// TestProcessInboundEvent_RejectsBrightnessOutOfRange proves that sdk-runner
// does not validate field ranges — brightness values outside 0-100 are accepted.
func TestProcessInboundEvent_RejectsBrightnessOutOfRange(t *testing.T) {
	r := newTestRunner(t)
	seedLightEntity(t, r, "dev1", "light")

	// brightness=200 is invalid (0-100 percent), but sdk-runner should reject this
	invalidBrightness, _ := json.Marshal(map[string]any{
		"type":       "turn_on",
		"brightness": 200,
	})

	updated, err := r.processInboundEvent(types.InboundEvent{
		DeviceID: "dev1",
		EntityID: "light",
		Payload:  json.RawMessage(invalidBrightness),
	})
	if err == nil {
		var stored map[string]any
		json.Unmarshal(updated.Data.Reported, &stored)
		t.Errorf("sdk-runner accepted brightness=200 (out of 0-100 range) and stored it: %v — no range validation", stored)
	}
}

// TestProcessInboundEvent_RejectsRawDeviceBrightnessScale proves that sdk-runner
// does not detect when a plugin sends device-native brightness (1-255) instead
// of the normalized percent scale (0-100).
func TestProcessInboundEvent_RejectsRawDeviceBrightnessScale(t *testing.T) {
	r := newTestRunner(t)
	seedLightEntity(t, r, "dev1", "light")

	// brightness=255 looks like a raw device value (0-255 scale), not a percent
	rawDeviceScale, _ := json.Marshal(map[string]any{
		"type":       "turn_on",
		"brightness": 255,
	})

	updated, err := r.processInboundEvent(types.InboundEvent{
		DeviceID: "dev1",
		EntityID: "light",
		Payload:  json.RawMessage(rawDeviceScale),
	})
	if err == nil {
		var stored map[string]any
		json.Unmarshal(updated.Data.Reported, &stored)
		t.Errorf("sdk-runner accepted brightness=255 (raw 0-255 device scale, not 0-100 percent) and stored it: %v — no scale validation", stored)
	}
}
