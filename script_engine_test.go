package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	types "github.com/slidebolt/sdk-types"
	lua "github.com/yuin/gopher-lua"
)

func newScriptTestRunner(t *testing.T) *Runner {
	t.Helper()
	r := &Runner{
		dataDir:     t.TempDir(),
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		scripts:     make(map[scriptKey]*scriptRuntime),
		deviceLocks: make(map[string]*sync.Mutex),
		fileHash:    make(map[string]string),
	}
	r.stateStore = newStateStore(r, "file")
	return r
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

func TestQueryFromLuaArg_SnakeCaseAndLabels(t *testing.T) {
	L := lua.NewState()
	defer L.Close()

	q := L.NewTable()
	q.RawSetString("pattern", lua.LString("test"))
	q.RawSetString("plugin_id", lua.LString("plugin-esphome"))
	q.RawSetString("device_id", lua.LString("switch_main_basement"))
	q.RawSetString("entity_id", lua.LString("3558733165"))
	q.RawSetString("domain", lua.LString("binary_sensor"))
	q.RawSetString("limit", lua.LNumber(5))

	labels := L.NewTable()
	location := L.NewTable()
	location.Append(lua.LString("Basement"))
	labels.RawSetString("Location", location)
	q.RawSetString("labels", labels)
	L.Push(q)

	parsed := queryFromLuaArg(L, 1)
	if parsed.Pattern != "test" {
		t.Fatalf("pattern=%q want=test", parsed.Pattern)
	}
	if parsed.PluginID != "plugin-esphome" || parsed.DeviceID != "switch_main_basement" || parsed.EntityID != "3558733165" {
		t.Fatalf("unexpected scope parse: %#v", parsed)
	}
	if parsed.Domain != "binary_sensor" || parsed.Limit != 5 {
		t.Fatalf("unexpected domain/limit: %#v", parsed)
	}
	if len(parsed.Labels["Location"]) != 1 || parsed.Labels["Location"][0] != "Basement" {
		t.Fatalf("labels parse failed: %#v", parsed.Labels)
	}
}

func TestFindEntities_LocalFallbackHonorsLabelsAndScope(t *testing.T) {
	r := newScriptTestRunner(t)
	r.manifest.ID = "plugin-test"

	dev := types.Device{
		ID:        "d1",
		SourceID:  "src-d1",
		LocalName: "device 1",
	}
	r.saveDevice(dev)
	r.saveEntity(types.Entity{
		ID:        "e1",
		SourceID:  "src-e1",
		DeviceID:  "d1",
		Domain:    "light",
		LocalName: "Light One",
		Labels:    map[string][]string{"Location": {"Basement"}},
	})
	r.saveEntity(types.Entity{
		ID:        "e2",
		SourceID:  "src-e2",
		DeviceID:  "d1",
		Domain:    "switch",
		LocalName: "Switch One",
		Labels:    map[string][]string{"Location": {"Main"}},
	})

	got := r.findEntities(scriptQuery{
		Pattern:  "*",
		PluginID: "plugin-test",
		Domain:   "light",
		Labels:   map[string][]string{"Location": {"Basement"}},
		Limit:    10,
	})
	if len(got) != 1 {
		t.Fatalf("len=%d want=1 (%#v)", len(got), got)
	}
	if got[0].PluginID != "plugin-test" || got[0].EntityID != "e1" {
		t.Fatalf("unexpected match: %#v", got[0])
	}
}

func TestEntityDataToMap_ExposesEffectivePower(t *testing.T) {
	in := types.EntityData{
		Effective:     json.RawMessage(`{"power":true}`),
		Reported:      json.RawMessage(`{"power":true}`),
		Desired:       json.RawMessage(`{"power":true}`),
		SyncStatus:    types.SyncStatusSynced,
		LastCommandID: "cmd-1",
		LastEventID:   "evt-1",
		UpdatedAt:     time.Unix(1700000000, 0).UTC(),
	}

	out := entityDataToMap(in)
	effective, ok := out["effective"].(map[string]any)
	if !ok {
		t.Fatalf("effective not a map: %#v", out["effective"])
	}
	power, ok := effective["power"].(bool)
	if !ok || !power {
		t.Fatalf("effective.power=%v ok=%v want true", effective["power"], ok)
	}
	if out["sync_status"] != string(types.SyncStatusSynced) {
		t.Fatalf("sync_status=%v want=%s", out["sync_status"], types.SyncStatusSynced)
	}
	if out["last_command_id"] != "cmd-1" || out["last_event_id"] != "evt-1" {
		t.Fatalf("unexpected ids in map: %#v", out)
	}
	if _, ok := out["updated_at"]; !ok {
		t.Fatalf("updated_at missing in map: %#v", out)
	}
}
