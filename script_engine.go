package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/slidebolt/sdk-types"
	lua "github.com/yuin/gopher-lua"
)

type scriptKey struct {
	DeviceID string
	EntityID string
}

type scriptRuntime struct {
	key scriptKey

	path      string
	statePath string
	mtimeUnix int64

	L *lua.LState

	eventSubs   map[string]string
	commandSubs map[string]string
	state       map[string]any
	mu          sync.Mutex
}

type scriptQuery struct {
	Text     string
	PluginID string
	DeviceID string
	EntityID string
	Domain   string
	Limit    int
}

func (r *Runner) ensureScriptRuntime(deviceID, entityID string) (*scriptRuntime, error) {
	key := scriptKey{DeviceID: deviceID, EntityID: entityID}

	r.scriptsMu.Lock()
	defer r.scriptsMu.Unlock()

	path := r.scriptPath(deviceID, entityID)
	info, err := os.Stat(path)
	if err != nil {
		delete(r.scripts, key)
		return nil, nil
	}

	if existing, ok := r.scripts[key]; ok && existing.mtimeUnix == info.ModTime().UnixNano() {
		return existing, nil
	}
	if existing, ok := r.scripts[key]; ok {
		existing.L.Close()
		delete(r.scripts, key)
	}

	rt, err := r.loadScriptRuntime(key, path, info.ModTime().UnixNano())
	if err != nil {
		return nil, err
	}
	r.scripts[key] = rt
	return rt, nil
}

func (r *Runner) loadScriptRuntime(key scriptKey, path string, mtimeUnix int64) (*scriptRuntime, error) {
	L := lua.NewState()
	rt := &scriptRuntime{
		key:         key,
		path:        path,
		statePath:   r.scriptStatePath(key.DeviceID, key.EntityID),
		mtimeUnix:   mtimeUnix,
		L:           L,
		eventSubs:   map[string]string{},
		commandSubs: map[string]string{},
		state:       r.loadScriptState(key.DeviceID, key.EntityID),
	}
	rt.installCtx(r)

	if err := L.DoFile(path); err != nil {
		L.Close()
		return nil, fmt.Errorf("lua load failed (%s): %w", path, err)
	}
	if fn := L.GetGlobal("OnInit"); fn.Type() == lua.LTFunction {
		if err := L.CallByParam(lua.P{
			Fn:      fn,
			NRet:    0,
			Protect: true,
		}, L.GetGlobal("Ctx")); err != nil {
			L.Close()
			return nil, fmt.Errorf("lua OnInit failed (%s): %w", path, err)
		}
	}
	return rt, nil
}

func (r *Runner) dispatchLuaCommand(cmd types.Command) {
	action := commandAction(cmd.Payload)
	selectors := commandSelectors(cmd, action)

	rt, err := r.ensureScriptRuntime(cmd.DeviceID, cmd.EntityID)
	if err != nil || rt == nil {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	for _, selector := range selectors {
		handler, ok := rt.commandSubs[selector]
		if !ok || handler == "" {
			continue
		}
		r.callLuaHandler(rt, handler, luaCommandFromTypes(cmd, action))
	}
}

func (r *Runner) dispatchLuaEvent(env types.EntityEventEnvelope) {
	r.ensureKnownScriptRuntimes()

	eventType := eventType(env.Payload)
	selectors := eventSelectors(env, eventType)

	r.scriptsMu.Lock()
	snapshot := make([]*scriptRuntime, 0, len(r.scripts))
	for _, rt := range r.scripts {
		snapshot = append(snapshot, rt)
	}
	r.scriptsMu.Unlock()

	for _, rt := range snapshot {
		rt.mu.Lock()
		for _, selector := range selectors {
			handler, ok := rt.eventSubs[selector]
			if !ok || handler == "" {
				continue
			}
			r.callLuaHandler(rt, handler, luaEventFromEnvelope(env, eventType))
		}
		rt.mu.Unlock()
	}
}

func (r *Runner) ensureKnownScriptRuntimes() {
	paths, _ := filepath.Glob(filepath.Join(r.dataDir, "devices", "*", "entities", "*.lua"))
	for _, path := range paths {
		deviceID := filepath.Base(filepath.Dir(filepath.Dir(path)))
		entityID := strings.TrimSuffix(filepath.Base(path), ".lua")
		if deviceID == "" || entityID == "" {
			continue
		}
		_, _ = r.ensureScriptRuntime(deviceID, entityID)
	}
}

func (r *Runner) callLuaHandler(rt *scriptRuntime, handler string, payload map[string]any) {
	fn := rt.L.GetGlobal(handler)
	if fn.Type() != lua.LTFunction {
		return
	}
	arg := mapToLTable(rt.L, payload)
	if err := rt.L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    0,
		Protect: true,
	}, rt.L.GetGlobal("Ctx"), arg); err != nil {
		fmt.Printf("[lua][handler-error][%s/%s] %s: %v\n", rt.key.DeviceID, rt.key.EntityID, handler, err)
	}
}

func (rt *scriptRuntime) installCtx(r *Runner) {
	ctx := rt.L.NewTable()
	rt.L.SetFuncs(ctx, map[string]lua.LGFunction{
		"OnEvent": func(L *lua.LState) int {
			selector := strings.TrimSpace(L.CheckString(2))
			handler := strings.TrimSpace(L.CheckString(3))
			if selector != "" && handler != "" {
				rt.eventSubs[selector] = handler
			}
			return 0
		},
		"OnCommand": func(L *lua.LState) int {
			selector := strings.TrimSpace(L.CheckString(2))
			handler := strings.TrimSpace(L.CheckString(3))
			if selector != "" && handler != "" {
				rt.commandSubs[selector] = handler
			}
			return 0
		},
		"GetState": func(L *lua.LState) int {
			key := strings.TrimSpace(L.CheckString(2))
			if key == "" {
				L.Push(lua.LNil)
				return 1
			}
			v, ok := rt.state[key]
			if !ok {
				L.Push(lua.LNil)
				return 1
			}
			L.Push(anyToLValue(L, v))
			return 1
		},
		"SetState": func(L *lua.LState) int {
			key := strings.TrimSpace(L.CheckString(2))
			if key == "" {
				return 0
			}
			rt.state[key] = lValueToAny(L.CheckAny(3))
			r.saveScriptState(rt.key.DeviceID, rt.key.EntityID, rt.state)
			return 0
		},
		"DeleteState": func(L *lua.LState) int {
			key := strings.TrimSpace(L.CheckString(2))
			if key == "" {
				return 0
			}
			delete(rt.state, key)
			r.saveScriptState(rt.key.DeviceID, rt.key.EntityID, rt.state)
			return 0
		},
		"NowUnixMilli": func(L *lua.LState) int {
			L.Push(lua.LNumber(time.Now().UnixMilli()))
			return 1
		},
		"RandomInt": func(L *lua.LState) int {
			min := L.CheckInt(2)
			max := L.CheckInt(3)
			if max <= min {
				L.Push(lua.LNumber(min))
				return 1
			}
			v := min + int(time.Now().UnixNano()%int64(max-min+1))
			L.Push(lua.LNumber(v))
			return 1
		},
		"FindEntities": func(L *lua.LState) int {
			query := queryFromLuaArg(L, 2)
			out := L.NewTable()
			items := r.findEntities(query)
			for _, ent := range items {
				out.Append(mapToLTable(L, map[string]any{
					"PluginID":  ent.PluginID,
					"DeviceID":  ent.DeviceID,
					"EntityID":  ent.EntityID,
					"Domain":    ent.Domain,
					"LocalName": ent.LocalName,
				}))
			}
			L.Push(out)
			return 1
		},
		"SendCommand": func(L *lua.LState) int {
			pluginID, deviceID, entityID, payload := sendCommandArgs(L)
			if pluginID == "" || deviceID == "" || entityID == "" {
				L.Push(lua.LNil)
				L.Push(lua.LString("missing PluginID/DeviceID/EntityID"))
				return 2
			}
			body, _ := json.Marshal(payload)
			status, err := r.callCreateCommand(pluginID, deviceID, entityID, body)
			if err != nil {
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(mapToLTable(L, map[string]any{
				"CommandID": status.CommandID,
				"State":     string(status.State),
				"Error":     status.Error,
			}))
			L.Push(lua.LNil)
			return 2
		},
		"EmitEvent": func(L *lua.LState) int {
			deviceID, entityID, payload, correlationID := emitEventArgs(L)
			if deviceID == "" || entityID == "" {
				L.Push(lua.LNil)
				L.Push(lua.LString("missing DeviceID/EntityID"))
				return 2
			}
			body, _ := json.Marshal(payload)
			err := r.EmitEvent(types.InboundEvent{
				DeviceID:      deviceID,
				EntityID:      entityID,
				CorrelationID: correlationID,
				Payload:       body,
			})
			if err != nil {
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(lua.LTrue)
			L.Push(lua.LNil)
			return 2
		},
		"LogInfo": func(L *lua.LState) int {
			msg := L.OptString(2, "")
			if msg != "" {
				fmt.Printf("[lua][%s/%s] %s\n", rt.key.DeviceID, rt.key.EntityID, msg)
			}
			return 0
		},
		"LogDebug": func(L *lua.LState) int {
			msg := L.OptString(2, "")
			if msg != "" {
				fmt.Printf("[lua][debug][%s/%s] %s\n", rt.key.DeviceID, rt.key.EntityID, msg)
			}
			return 0
		},
		"LogWarn": func(L *lua.LState) int {
			msg := L.OptString(2, "")
			if msg != "" {
				fmt.Printf("[lua][warn][%s/%s] %s\n", rt.key.DeviceID, rt.key.EntityID, msg)
			}
			return 0
		},
		"LogError": func(L *lua.LState) int {
			msg := L.OptString(2, "")
			if msg != "" {
				fmt.Printf("[lua][error][%s/%s] %s\n", rt.key.DeviceID, rt.key.EntityID, msg)
			}
			return 0
		},
		"FindDevices": func(L *lua.LState) int {
			query := queryFromLuaArg(L, 2)
			out := L.NewTable()
			items := r.findDevices(query)
			for _, dev := range items {
				out.Append(mapToLTable(L, map[string]any{
					"PluginID":   dev.PluginID,
					"DeviceID":   dev.DeviceID,
					"SourceID":   dev.SourceID,
					"SourceName": dev.SourceName,
					"LocalName":  dev.LocalName,
				}))
			}
			L.Push(out)
			return 1
		},
		"GetEntity": func(L *lua.LState) int {
			pluginID := strings.TrimSpace(L.CheckString(2))
			deviceID := strings.TrimSpace(L.CheckString(3))
			entityID := strings.TrimSpace(L.CheckString(4))
			ent, err := r.getEntity(pluginID, deviceID, entityID)
			if err != nil {
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(mapToLTable(L, map[string]any{
				"PluginID":  pluginID,
				"DeviceID":  ent.DeviceID,
				"EntityID":  ent.ID,
				"Domain":    ent.Domain,
				"LocalName": ent.LocalName,
				"Actions":   toAnySlice(ent.Actions),
			}))
			L.Push(lua.LNil)
			return 2
		},
		"GetDevice": func(L *lua.LState) int {
			pluginID := strings.TrimSpace(L.CheckString(2))
			deviceID := strings.TrimSpace(L.CheckString(3))
			dev, err := r.getDevice(pluginID, deviceID)
			if err != nil {
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(mapToLTable(L, map[string]any{
				"PluginID":   pluginID,
				"DeviceID":   dev.ID,
				"SourceID":   dev.SourceID,
				"SourceName": dev.SourceName,
				"LocalName":  dev.LocalName,
			}))
			L.Push(lua.LNil)
			return 2
		},
	})
	rt.L.SetGlobal("Ctx", ctx)
}

type foundDevice struct {
	PluginID   string
	DeviceID   string
	SourceID   string
	SourceName string
	LocalName  string
}

type foundEntity struct {
	PluginID  string
	DeviceID  string
	EntityID  string
	Domain    string
	LocalName string
}

func (r *Runner) findDevices(query scriptQuery) []foundDevice {
	filter := strings.TrimSpace(strings.ToLower(query.Text))
	results := make([]foundDevice, 0)

	registry := r.snapshotRegistry()
	for pluginID := range registry {
		if query.PluginID != "" && pluginID != query.PluginID {
			continue
		}
		devices, err := r.callListDevices(pluginID)
		if err != nil {
			continue
		}
		for _, dev := range devices {
			if query.DeviceID != "" && dev.ID != query.DeviceID {
				continue
			}
			if filter != "" &&
				!strings.Contains(strings.ToLower(pluginID), filter) &&
				!strings.Contains(strings.ToLower(dev.ID), filter) &&
				!strings.Contains(strings.ToLower(dev.SourceID), filter) &&
				!strings.Contains(strings.ToLower(dev.LocalName), filter) {
				continue
			}
			results = append(results, foundDevice{
				PluginID:   pluginID,
				DeviceID:   dev.ID,
				SourceID:   dev.SourceID,
				SourceName: dev.SourceName,
				LocalName:  dev.LocalName,
			})
			if query.Limit > 0 && len(results) >= query.Limit {
				return results
			}
		}
	}
	return results
}

func (r *Runner) findEntities(query scriptQuery) []foundEntity {
	filter := strings.TrimSpace(strings.ToLower(query.Text))
	results := make([]foundEntity, 0)

	registry := r.snapshotRegistry()
	for pluginID := range registry {
		if query.PluginID != "" && pluginID != query.PluginID {
			continue
		}
		devices, err := r.callListDevices(pluginID)
		if err != nil {
			continue
		}
		for _, dev := range devices {
			if query.DeviceID != "" && dev.ID != query.DeviceID {
				continue
			}
			entities, err := r.callListEntities(pluginID, dev.ID)
			if err != nil {
				continue
			}
			for _, ent := range entities {
				if query.EntityID != "" && ent.ID != query.EntityID {
					continue
				}
				if query.Domain != "" && ent.Domain != query.Domain {
					continue
				}
				if filter != "" && !strings.Contains(strings.ToLower(pluginID), filter) && !strings.Contains(strings.ToLower(ent.Domain), filter) {
					continue
				}
				results = append(results, foundEntity{
					PluginID:  pluginID,
					DeviceID:  dev.ID,
					EntityID:  ent.ID,
					Domain:    ent.Domain,
					LocalName: ent.LocalName,
				})
				if query.Limit > 0 && len(results) >= query.Limit {
					return results
				}
			}
		}
	}
	return results
}

func (r *Runner) getDevice(pluginID, deviceID string) (types.Device, error) {
	devices, err := r.callListDevices(pluginID)
	if err != nil {
		return types.Device{}, err
	}
	for _, d := range devices {
		if d.ID == deviceID {
			return d, nil
		}
	}
	return types.Device{}, fmt.Errorf("device not found: %s/%s", pluginID, deviceID)
}

func (r *Runner) getEntity(pluginID, deviceID, entityID string) (types.Entity, error) {
	entities, err := r.callListEntities(pluginID, deviceID)
	if err != nil {
		return types.Entity{}, err
	}
	for _, e := range entities {
		if e.ID == entityID {
			return e, nil
		}
	}
	return types.Entity{}, fmt.Errorf("entity not found: %s/%s/%s", pluginID, deviceID, entityID)
}

func toAnySlice(items []string) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

func queryFromLuaArg(L *lua.LState, idx int) scriptQuery {
	v := L.Get(idx)
	switch x := v.(type) {
	case lua.LString:
		return scriptQuery{Text: strings.TrimSpace(string(x))}
	case *lua.LTable:
		raw := lValueToAny(x)
		obj, _ := raw.(map[string]any)
		if obj == nil {
			return scriptQuery{}
		}
		return scriptQuery{
			Text:     getString(obj, "Text"),
			PluginID: getString(obj, "PluginID"),
			DeviceID: getString(obj, "DeviceID"),
			EntityID: getString(obj, "EntityID"),
			Domain:   getString(obj, "Domain"),
			Limit:    getInt(obj, "Limit"),
		}
	default:
		return scriptQuery{}
	}
}

func sendCommandArgs(L *lua.LState) (pluginID, deviceID, entityID string, payload any) {
	payload = map[string]any{}
	v := L.Get(2)
	if tbl, ok := v.(*lua.LTable); ok {
		raw := lValueToAny(tbl)
		obj, _ := raw.(map[string]any)
		if obj == nil {
			return "", "", "", payload
		}
		return getString(obj, "PluginID"), getString(obj, "DeviceID"), getString(obj, "EntityID"), getMapAny(obj, "Payload")
	}
	return strings.TrimSpace(L.CheckString(2)), strings.TrimSpace(L.CheckString(3)), strings.TrimSpace(L.CheckString(4)), lValueToAny(L.CheckAny(5))
}

func emitEventArgs(L *lua.LState) (deviceID, entityID string, payload any, correlationID string) {
	payload = map[string]any{}
	v := L.Get(2)
	if tbl, ok := v.(*lua.LTable); ok {
		raw := lValueToAny(tbl)
		obj, _ := raw.(map[string]any)
		if obj == nil {
			return "", "", payload, ""
		}
		return getString(obj, "DeviceID"), getString(obj, "EntityID"), getMapAny(obj, "Payload"), getString(obj, "CorrelationID")
	}
	return strings.TrimSpace(L.CheckString(2)), strings.TrimSpace(L.CheckString(3)), lValueToAny(L.CheckAny(4)), strings.TrimSpace(L.OptString(5, ""))
}

func getString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func getInt(m map[string]any, key string) int {
	v, ok := m[key]
	if !ok {
		return 0
	}
	switch x := v.(type) {
	case float64:
		return int(x)
	case int:
		return x
	case int64:
		return int(x)
	default:
		return 0
	}
}

func getMapAny(m map[string]any, key string) map[string]any {
	v, ok := m[key]
	if !ok || v == nil {
		return map[string]any{}
	}
	switch x := v.(type) {
	case map[string]any:
		return x
	default:
		b, _ := json.Marshal(x)
		var out map[string]any
		if err := json.Unmarshal(b, &out); err == nil && out != nil {
			return out
		}
		return map[string]any{}
	}
}

func (r *Runner) scriptPath(deviceID, entityID string) string {
	return filepath.Join(r.dataDir, "devices", deviceID, "entities", entityID+".lua")
}

func (r *Runner) scriptStatePath(deviceID, entityID string) string {
	return filepath.Join(r.dataDir, "devices", deviceID, "entities", entityID+".state.lua.json")
}

func (r *Runner) loadScriptState(deviceID, entityID string) map[string]any {
	path := r.scriptStatePath(deviceID, entityID)
	data, err := os.ReadFile(path)
	if err != nil || len(data) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

func (r *Runner) saveScriptState(deviceID, entityID string, state map[string]any) {
	path := r.scriptStatePath(deviceID, entityID)
	_ = os.MkdirAll(filepath.Dir(path), 0o755)
	data, _ := json.MarshalIndent(state, "", "  ")
	_ = os.WriteFile(path, data, 0o644)
}

func (r *Runner) invalidateScriptRuntime(deviceID, entityID string) {
	key := scriptKey{DeviceID: deviceID, EntityID: entityID}
	r.scriptsMu.Lock()
	defer r.scriptsMu.Unlock()
	if rt, ok := r.scripts[key]; ok {
		rt.L.Close()
		delete(r.scripts, key)
	}
}

func (r *Runner) getScriptSource(deviceID, entityID string) (string, string, error) {
	path := r.scriptPath(deviceID, entityID)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", path, fmt.Errorf("script not found")
	}
	return string(data), path, nil
}

func (r *Runner) putScriptSource(deviceID, entityID, source string) (string, error) {
	path := r.scriptPath(deviceID, entityID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return path, err
	}
	if err := os.WriteFile(path, []byte(source), 0o644); err != nil {
		return path, err
	}
	r.invalidateScriptRuntime(deviceID, entityID)
	return path, nil
}

func (r *Runner) deleteScriptSource(deviceID, entityID string, purgeState bool) error {
	path := r.scriptPath(deviceID, entityID)
	_ = os.Remove(path)
	r.invalidateScriptRuntime(deviceID, entityID)
	if purgeState {
		_ = os.Remove(r.scriptStatePath(deviceID, entityID))
	}
	return nil
}

func (r *Runner) getScriptState(deviceID, entityID string) (map[string]any, string) {
	path := r.scriptStatePath(deviceID, entityID)
	return r.loadScriptState(deviceID, entityID), path
}

func (r *Runner) putScriptState(deviceID, entityID string, state map[string]any) (string, error) {
	if state == nil {
		state = map[string]any{}
	}
	path := r.scriptStatePath(deviceID, entityID)
	r.saveScriptState(deviceID, entityID, state)
	return path, nil
}

func (r *Runner) deleteScriptState(deviceID, entityID string) (string, error) {
	path := r.scriptStatePath(deviceID, entityID)
	_ = os.Remove(path)
	return path, nil
}

func mapToLTable(L *lua.LState, m map[string]any) *lua.LTable {
	t := L.NewTable()
	for k, v := range m {
		t.RawSetString(k, anyToLValue(L, v))
	}
	return t
}

func anyToLValue(L *lua.LState, v any) lua.LValue {
	switch x := v.(type) {
	case nil:
		return lua.LNil
	case string:
		return lua.LString(x)
	case bool:
		return lua.LBool(x)
	case float64:
		return lua.LNumber(x)
	case int:
		return lua.LNumber(x)
	case int64:
		return lua.LNumber(x)
	case json.Number:
		f, _ := x.Float64()
		return lua.LNumber(f)
	case map[string]any:
		return mapToLTable(L, x)
	case []any:
		arr := L.NewTable()
		for _, item := range x {
			arr.Append(anyToLValue(L, item))
		}
		return arr
	default:
		b, _ := json.Marshal(x)
		var generic any
		_ = json.Unmarshal(b, &generic)
		return anyToLValue(L, generic)
	}
}

func lValueToAny(v lua.LValue) any {
	switch x := v.(type) {
	case lua.LString:
		return string(x)
	case lua.LNumber:
		return float64(x)
	case lua.LBool:
		return bool(x)
	case *lua.LTable:
		m := map[string]any{}
		arr := []any{}
		isArray := true
		maxIndex := 0
		x.ForEach(func(k lua.LValue, v lua.LValue) {
			if kn, ok := k.(lua.LNumber); ok {
				i := int(kn)
				if i > maxIndex {
					maxIndex = i
				}
			} else {
				isArray = false
			}
			m[k.String()] = lValueToAny(v)
		})
		if isArray && maxIndex > 0 {
			for i := 1; i <= maxIndex; i++ {
				arr = append(arr, m[fmt.Sprintf("%d", i)])
			}
			return arr
		}
		return m
	default:
		return nil
	}
}

func commandAction(payload json.RawMessage) string {
	var probe struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(payload, &probe)
	return strings.TrimSpace(probe.Type)
}

func eventType(payload json.RawMessage) string {
	var probe struct {
		Type string `json:"type"`
	}
	_ = json.Unmarshal(payload, &probe)
	return strings.TrimSpace(probe.Type)
}

func commandSelectors(cmd types.Command, action string) []string {
	base := fmt.Sprintf("%s.%s.%s", cmd.PluginID, cmd.DeviceID, cmd.EntityID)
	out := []string{base}
	if action != "" {
		out = append(out, base+"."+action, fmt.Sprintf("%s.%s", cmd.PluginID, action))
	}
	return out
}

func eventSelectors(env types.EntityEventEnvelope, eventType string) []string {
	base := fmt.Sprintf("%s.%s.%s", env.PluginID, env.DeviceID, env.EntityID)
	out := []string{base}
	if eventType != "" {
		out = append(out, base+"."+eventType, fmt.Sprintf("%s.%s", env.PluginID, eventType), fmt.Sprintf("%s.%s.%s", env.PluginID, env.EntityID, eventType))
	}
	return out
}

func luaCommandFromTypes(cmd types.Command, action string) map[string]any {
	return map[string]any{
		"ID":         cmd.ID,
		"PluginID":   cmd.PluginID,
		"DeviceID":   cmd.DeviceID,
		"EntityID":   cmd.EntityID,
		"EntityType": cmd.EntityType,
		"Action":     action,
		"Payload":    jsonRawToAny(cmd.Payload),
	}
}

func luaEventFromEnvelope(env types.EntityEventEnvelope, eventType string) map[string]any {
	return map[string]any{
		"EventID":       env.EventID,
		"PluginID":      env.PluginID,
		"DeviceID":      env.DeviceID,
		"EntityID":      env.EntityID,
		"EntityType":    env.EntityType,
		"CorrelationID": env.CorrelationID,
		"Type":          eventType,
		"Payload":       jsonRawToAny(env.Payload),
		"CreatedAt":     env.CreatedAt.UnixMilli(),
	}
}

func jsonRawToAny(raw json.RawMessage) any {
	if len(raw) == 0 {
		return map[string]any{}
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return map[string]any{}
	}
	return out
}
