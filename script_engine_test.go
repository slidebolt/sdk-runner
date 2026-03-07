package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	types "github.com/slidebolt/sdk-types"
)

func newScriptTestRunner(t *testing.T) *Runner {
	t.Helper()
	return &Runner{
		dataDir:     t.TempDir(),
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		scripts:     make(map[scriptKey]*scriptRuntime),
		deviceLocks: make(map[string]*sync.Mutex),
		fileHash:    make(map[string]string),
	}
}

func TestEventSelectors_WithType(t *testing.T) {
	env := types.EntityEventEnvelope{
		PluginID: "plugin-esphome",
		DeviceID: "switch_basement_edison",
		EntityID: "edison_button",
	}
	got := eventSelectors(env, "doorbell_press")
	want := []string{
		"plugin-esphome.switch_basement_edison.edison_button",
		"plugin-esphome.switch_basement_edison.edison_button.doorbell_press",
		"plugin-esphome.doorbell_press",
		"plugin-esphome.edison_button.doorbell_press",
	}
	if len(got) != len(want) {
		t.Fatalf("selectors len=%d want=%d got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("selectors[%d]=%q want=%q (all=%v)", i, got[i], want[i], got)
		}
	}
}

func TestRequiredEventType(t *testing.T) {
	okType, err := requiredEventType(json.RawMessage(`{"type":"press","state":true}`))
	if err != nil {
		t.Fatalf("requiredEventType unexpected err: %v", err)
	}
	if okType != "press" {
		t.Fatalf("type=%q want=press", okType)
	}

	if _, err := requiredEventType(json.RawMessage(`{"state":true}`)); err == nil {
		t.Fatal("expected error for payload without type")
	}
	if _, err := requiredEventType(json.RawMessage(`not-json`)); err == nil {
		t.Fatal("expected error for invalid json payload")
	}
}

func TestDispatchLuaEvent_BaseSelectorMatchesWhenPayloadHasNoType(t *testing.T) {
	r := newScriptTestRunner(t)
	deviceID := "listener-device"
	entityID := "listener-entity"

	script := `
function OnInit(Ctx)
  Ctx:OnEvent("plugin-esphome.switch_basement_edison.edison_button", "OnEventBase")
end

function OnEventBase(Ctx, EventRef)
  Ctx:SetState("base_count", (Ctx:GetState("base_count") or 0) + 1)
end
`
	if _, err := r.putScriptSource(deviceID, entityID, script); err != nil {
		t.Fatalf("put script failed: %v", err)
	}
	statePath := filepath.Join(r.dataDir, "devices", deviceID, "entities", entityID+".state.lua.json")
	_ = os.Remove(statePath)

	payload, _ := json.Marshal(map[string]any{"state": true})
	r.dispatchLuaEvent(types.EntityEventEnvelope{
		EventID:    "evt-1",
		PluginID:   "plugin-esphome",
		DeviceID:   "switch_basement_edison",
		EntityID:   "edison_button",
		EntityType: "binary_sensor",
		Payload:    payload,
		CreatedAt:  time.Now().UTC(),
	})

	state := mustReadState(t, statePath)
	got, _ := state["base_count"].(float64)
	if int(got) != 1 {
		t.Fatalf("base_count=%v want=1 (state=%v)", state["base_count"], state)
	}
}

func TestDispatchLuaEvent_TypedSelectorDoesNotMatchWhenPayloadHasNoType(t *testing.T) {
	r := newScriptTestRunner(t)
	deviceID := "listener-device-typed"
	entityID := "listener-entity-typed"

	script := `
function OnInit(Ctx)
  Ctx:OnEvent("plugin-esphome.switch_basement_edison.edison_button.doorbell_press", "OnDoorbell")
end

function OnDoorbell(Ctx, EventRef)
  Ctx:SetState("typed_count", (Ctx:GetState("typed_count") or 0) + 1)
end
`
	if _, err := r.putScriptSource(deviceID, entityID, script); err != nil {
		t.Fatalf("put script failed: %v", err)
	}
	statePath := filepath.Join(r.dataDir, "devices", deviceID, "entities", entityID+".state.lua.json")
	_ = os.Remove(statePath)

	payload, _ := json.Marshal(map[string]any{"state": true})
	r.dispatchLuaEvent(types.EntityEventEnvelope{
		EventID:    "evt-2",
		PluginID:   "plugin-esphome",
		DeviceID:   "switch_basement_edison",
		EntityID:   "edison_button",
		EntityType: "binary_sensor",
		Payload:    payload,
		CreatedAt:  time.Now().UTC(),
	})

	state := mustReadState(t, statePath)
	if _, exists := state["typed_count"]; exists {
		t.Fatalf("typed_count should not exist for typeless payload, state=%v", state)
	}
}

func mustReadState(t *testing.T, statePath string) map[string]any {
	t.Helper()
	data, err := os.ReadFile(statePath)
	if err != nil {
		// state file may be absent for negative assertions
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil {
		t.Fatalf("unmarshal state failed: %v", err)
	}
	if out == nil {
		return map[string]any{}
	}
	return out
}
