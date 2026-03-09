package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
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
	closed      bool
}

type scriptQuery struct {
	Text     string
	Pattern  string
	Labels   map[string][]string
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
		existing.mu.Lock()
		existing.L.Close()
		existing.closed = true
		existing.mu.Unlock()
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

func (r *Runner) handleCommand(cmd types.Command) {
	action := payloadType(cmd.Payload)
	selectors := commandSelectors(cmd, action)

	rt, err := r.ensureScriptRuntime(cmd.DeviceID, cmd.EntityID)
	if err != nil || rt == nil {
		return
	}

	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return
	}
	for _, selector := range selectors {
		handler, ok := rt.commandSubs[selector]
		if !ok || handler == "" {
			continue
		}
		r.callLuaHandler(rt, handler, luaCommandFromTypes(cmd, action))
	}
}

func (r *Runner) handleEvent(env types.EntityEventEnvelope) {
	r.ensureKnownScriptRuntimes()

		eventType := payloadType(env.Payload)
		selectors := eventSelectors(env, eventType)
	
		if r.logger != nil {
		r.logger.Info("dispatching Lua event", "plugin", env.PluginID, "device", env.DeviceID, "entity", env.EntityID, "type", eventType, "selectors", selectors)
	}
		r.scriptsMu.Lock()
	snapshot := make([]*scriptRuntime, 0, len(r.scripts))
	for _, rt := range r.scripts {
		snapshot = append(snapshot, rt)
	}
	r.scriptsMu.Unlock()

	for _, rt := range snapshot {
		rt.mu.Lock()
		if rt.closed {
			rt.mu.Unlock()
			continue
		}
		for _, selector := range selectors {
			handler, ok := rt.eventSubs[selector]
			if !ok || handler == "" {
				continue
			}
			if r.logger != nil {
				r.logger.Info("Lua event match found", "selector", selector, "handler", handler, "script", rt.key.EntityID)
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
		if r.logger != nil {
			r.logger.Error("lua handler error", "device", rt.key.DeviceID, "entity", rt.key.EntityID, "handler", handler, "error", err)
		}
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
		"OnEntityCommand": func(L *lua.LState) int {
			pluginID, deviceID, entityID := entityRefFromLuaArg(L, 2)
			action := strings.TrimSpace(L.CheckString(3))
			handler := strings.TrimSpace(L.CheckString(4))
			if pluginID == "" || deviceID == "" || entityID == "" || action == "" || handler == "" {
				return 0
			}
			selector := fmt.Sprintf("%s.%s.%s.%s", pluginID, deviceID, entityID, action)
			rt.commandSubs[selector] = handler
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
					"PluginID":   ent.PluginID,
					"DeviceID":   ent.DeviceID,
					"EntityID":   ent.EntityID,
					"Domain":     ent.Domain,
					"SourceID":   ent.SourceID,
					"SourceName": ent.SourceName,
					"LocalName":  ent.LocalName,
					"Labels":     ent.Labels,
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
		"SendBatchCommands": func(L *lua.LState) int {
			items, parseErr := sendBatchCommandArgs(L, 2)
			if parseErr != "" {
				L.Push(lua.LNil)
				L.Push(lua.LString(parseErr))
				return 2
			}
			batchStart := time.Now()
			if r.logger != nil {
				r.logger.Log(context.Background(), LevelTrace, "lua batch start",
					"device_id", rt.key.DeviceID,
					"entity_id", rt.key.EntityID,
					"count", len(items),
					"start_unix_ms", batchStart.UnixMilli())
			}
			out := L.NewTable()
			for idx, item := range items {
				itemStart := time.Now()
				if r.logger != nil {
					r.logger.Log(context.Background(), LevelTrace, "lua batch item dispatch",
						"device_id", rt.key.DeviceID,
						"entity_id", rt.key.EntityID,
						"index", idx,
						"plugin_id", item.PluginID,
						"target_device_id", item.DeviceID,
						"target_entity_id", item.EntityID,
						"start_unix_ms", itemStart.UnixMilli())
				}
				body, _ := json.Marshal(item.Payload)
				status, err := r.callCreateCommand(item.PluginID, item.DeviceID, item.EntityID, body)
				result := map[string]any{
					"PluginID": item.PluginID,
					"DeviceID": item.DeviceID,
					"EntityID": item.EntityID,
				}
				if err != nil {
					result["OK"] = false
					result["Error"] = err.Error()
					if r.logger != nil {
						r.logger.Log(context.Background(), LevelTrace, "lua batch item result",
							"device_id", rt.key.DeviceID,
							"entity_id", rt.key.EntityID,
							"index", idx,
							"plugin_id", item.PluginID,
							"target_device_id", item.DeviceID,
							"target_entity_id", item.EntityID,
							"ok", false,
							"error", err.Error(),
							"elapsed_ms", time.Since(itemStart).Milliseconds())
					}
				} else {
					result["OK"] = true
					result["CommandID"] = status.CommandID
					result["State"] = string(status.State)
					result["Error"] = status.Error
					if r.logger != nil {
						r.logger.Log(context.Background(), LevelTrace, "lua batch item result",
							"device_id", rt.key.DeviceID,
							"entity_id", rt.key.EntityID,
							"index", idx,
							"plugin_id", item.PluginID,
							"target_device_id", item.DeviceID,
							"target_entity_id", item.EntityID,
							"command_id", status.CommandID,
							"state", string(status.State),
							"ok", true,
							"elapsed_ms", time.Since(itemStart).Milliseconds())
					}
				}
				out.Append(mapToLTable(L, result))
			}
			if r.logger != nil {
				r.logger.Log(context.Background(), LevelTrace, "lua batch done",
					"device_id", rt.key.DeviceID,
					"entity_id", rt.key.EntityID,
					"count", len(items),
					"elapsed_ms", time.Since(batchStart).Milliseconds())
			}
			L.Push(out)
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
			rawPayload, _ := json.Marshal(payload)
			err := r.EmitEvent(types.InboundEvent{
				DeviceID:      deviceID,
				EntityID:      entityID,
				CorrelationID: correlationID,
				Payload:       rawPayload,
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
			if msg != "" && r.logger != nil {
				r.logger.Info(msg, "device", rt.key.DeviceID, "entity", rt.key.EntityID)
			}
			return 0
		},
		"LogDebug": func(L *lua.LState) int {
			msg := L.OptString(2, "")
			if msg != "" && r.logger != nil {
				r.logger.Debug(msg, "device", rt.key.DeviceID, "entity", rt.key.EntityID)
			}
			return 0
		},
		"LogWarn": func(L *lua.LState) int {
			msg := L.OptString(2, "")
			if msg != "" && r.logger != nil {
				r.logger.Warn(msg, "device", rt.key.DeviceID, "entity", rt.key.EntityID)
			}
			return 0
		},
		"LogError": func(L *lua.LState) int {
			msg := L.OptString(2, "")
			if msg != "" && r.logger != nil {
				r.logger.Error(msg, "device", rt.key.DeviceID, "entity", rt.key.EntityID)
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
					"Labels":     dev.Labels,
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
				"PluginID":   pluginID,
				"DeviceID":   ent.DeviceID,
				"EntityID":   ent.ID,
				"Domain":     ent.Domain,
				"SourceID":   ent.SourceID,
				"SourceName": ent.SourceName,
				"LocalName":  ent.LocalName,
				"Actions":    toAnySlice(ent.Actions),
				"Labels":     ent.Labels,
				"Data":       entityDataToMap(ent.Data),
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
		"Find": func(L *lua.LState) int {
			queryStr := strings.TrimSpace(L.CheckString(2))
			if queryStr == "" {
				queryStr = "?q=*"
			}

			if r.nc == nil {
				if r.logger != nil {
					r.logger.Error("Ctx:Find failed: NATS connection unavailable")
				}
				L.Push(r.newEntityCollection(L, rt, nil))
				return 1
			}

			if r.logger != nil {
				r.logger.Info("Ctx:Find dispatching NATS request", "subject", SubjectGatewayDiscovery, "query", queryStr)
			}
			msg, err := r.nc.Request(SubjectGatewayDiscovery, []byte(queryStr), 5*time.Second)
			if err != nil {
				if r.logger != nil {
					r.logger.Error("Ctx:Find failed", "query", queryStr, "error", err)
				}
				L.Push(r.newEntityCollection(L, rt, nil))
				return 1
			}
			if r.logger != nil {
				r.logger.Info("Ctx:Find received NATS response", "bytes", len(msg.Data))
			}

			var matches []struct {
				PluginID string `json:"plugin_id"`
				DeviceID string `json:"device_id"`
				ID       string `json:"id"`
				Domain   string `json:"domain"`
			}
			if err := json.Unmarshal(msg.Data, &matches); err != nil {
				if r.logger != nil {
					r.logger.Error("Ctx:Find decode failed", "error", err)
				}
				L.Push(r.newEntityCollection(L, rt, nil))
				return 1
			}

			found := make([]foundEntity, len(matches))
			for i, m := range matches {
				found[i] = foundEntity{
					PluginID: m.PluginID,
					DeviceID: m.DeviceID,
					EntityID: m.ID,
					Domain:   m.Domain,
				}
			}

			L.Push(r.newEntityCollection(L, rt, found))
			return 1
		},
	})
	rt.L.SetGlobal("Ctx", ctx)
}

func (r *Runner) newEntityCollection(L *lua.LState, rt *scriptRuntime, entities []foundEntity) *lua.LTable {
	t := L.NewTable()
	// Expose raw list for iteration if needed
	list := L.NewTable()
	for _, ent := range entities {
		list.Append(mapToLTable(L, map[string]any{
			"PluginID": ent.PluginID,
			"DeviceID": ent.DeviceID,
			"EntityID": ent.EntityID,
			"Domain":   ent.Domain,
		}))
	}
	t.RawSetString("Entities", list)

	// Add bulk methods
	L.SetFuncs(t, map[string]lua.LGFunction{
		"OnEvent": func(L *lua.LState) int {
			eventType := strings.TrimSpace(L.CheckString(2))
			handler := strings.TrimSpace(L.CheckString(3))
			if eventType == "" || handler == "" {
				return 0
			}
			if r.logger != nil {
				r.logger.Info("Collection:OnEvent bulk registering", "count", len(entities), "type", eventType, "handler", handler)
			}
			for _, ent := range entities {
				selector := fmt.Sprintf("%s.%s.%s.%s", ent.PluginID, ent.DeviceID, ent.EntityID, eventType)
				if r.logger != nil {
					r.logger.Info("Collection:OnEvent registering selector", "selector", selector)
				}
				rt.eventSubs[selector] = handler
			}
			return 0
		},
		"SendCommand": func(L *lua.LState) int {
			payload := lValueToAny(L.CheckAny(2))
			body, _ := json.Marshal(payload)
			results := L.NewTable()
			for _, ent := range entities {
				status, err := r.callCreateCommand(ent.PluginID, ent.DeviceID, ent.EntityID, body)
				res := map[string]any{
					"PluginID": ent.PluginID,
					"DeviceID": ent.DeviceID,
					"EntityID": ent.EntityID,
				}
				if err != nil {
					res["OK"] = false
					res["Error"] = err.Error()
				} else {
					res["OK"] = true
					res["CommandID"] = status.CommandID
				}
				results.Append(mapToLTable(L, res))
			}
			L.Push(results)
			return 1
		},
		"EmitEvent": func(L *lua.LState) int {
			payload := lValueToAny(L.CheckAny(2))
			rawPayload, _ := json.Marshal(payload)
			correlationID := L.OptString(3, "")
			for _, ent := range entities {
				_ = r.EmitEvent(types.InboundEvent{
					DeviceID:      ent.DeviceID,
					EntityID:      ent.EntityID,
					CorrelationID: correlationID,
					Payload:       rawPayload,
				})
			}
			return 0
		},
		"Count": func(L *lua.LState) int {
			L.Push(lua.LNumber(len(entities)))
			return 1
		},
	})

	return t
}

type foundDevice struct {
	PluginID   string
	DeviceID   string
	SourceID   string
	SourceName string
	LocalName  string
	Labels     map[string][]string
}

type foundEntity struct {
	PluginID   string
	DeviceID   string
	EntityID   string
	Domain     string
	SourceID   string
	SourceName string
	LocalName  string
	Labels     map[string][]string
}
func (r *Runner) findDevices(query scriptQuery) []foundDevice {
	pattern := strings.TrimSpace(query.Pattern)
	if pattern == "" {
		pattern = strings.TrimSpace(query.Text)
	}
	if pattern == "" {
		pattern = "*"
	}
	q := types.SearchQuery{
		Pattern:  pattern,
		Labels:   query.Labels,
		PluginID: query.PluginID,
		DeviceID: query.DeviceID,
		Limit:    query.Limit,
	}
	return r.searchDevicesDistributed(q)
}

func (r *Runner) findEntities(query scriptQuery) []foundEntity {
	pattern := strings.TrimSpace(query.Pattern)
	if pattern == "" {
		pattern = strings.TrimSpace(query.Text)
	}
	if pattern == "" {
		pattern = "*"
	}
	q := types.SearchQuery{
		Pattern:  pattern,
		Labels:   query.Labels,
		PluginID: query.PluginID,
		DeviceID: query.DeviceID,
		EntityID: query.EntityID,
		Domain:   query.Domain,
		Limit:    query.Limit,
	}
	return r.searchEntitiesDistributed(q)
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
			Text:     getStringAny(obj, "Text", "text"),
			Pattern:  getStringAny(obj, "Pattern", "pattern"),
			Labels:   getLabelsAny(obj, "Labels", "labels"),
			PluginID: getStringAny(obj, "PluginID", "plugin_id"),
			DeviceID: getStringAny(obj, "DeviceID", "device_id"),
			EntityID: getStringAny(obj, "EntityID", "entity_id"),
			Domain:   getStringAny(obj, "Domain", "domain"),
			Limit:    getIntAny(obj, "Limit", "limit"),
		}
	default:
		return scriptQuery{}
	}
}

func (r *Runner) searchDevicesDistributed(q types.SearchQuery) []foundDevice {
	if r.nc == nil {
		local := r.searchDevicesLocal(q)
		out := make([]foundDevice, 0, len(local))
		for _, d := range local {
			out = append(out, foundDevice{
				PluginID:   r.manifest.ID,
				DeviceID:   d.ID,
				SourceID:   d.SourceID,
				SourceName: d.SourceName,
				LocalName:  d.LocalName,
				Labels:     d.Labels,
			})
		}
		return out
	}
	data, _ := json.Marshal(q)
	sub, err := r.nc.SubscribeSync(nats.NewInbox())
	if err != nil {
		local := r.searchDevicesLocal(q)
		out := make([]foundDevice, 0, len(local))
		for _, d := range local {
			out = append(out, foundDevice{
				PluginID:   r.manifest.ID,
				DeviceID:   d.ID,
				SourceID:   d.SourceID,
				SourceName: d.SourceName,
				LocalName:  d.LocalName,
				Labels:     d.Labels,
			})
		}
		return out
	}
	defer sub.Unsubscribe()
	r.nc.PublishRequest(SubjectSearchDevices, sub.Subject, data)

	results := make([]foundDevice, 0)
	expected := r.expectedSearchPlugins(q.PluginID)
	timeout := time.After(2 * time.Second)
	for len(expected) > 0 {
		select {
		case <-timeout:
			return applyFoundDeviceLimit(results, q.Limit)
		default:
			msg, err := sub.NextMsg(100 * time.Millisecond)
			if err != nil {
				continue
			}
			var res types.SearchDevicesResponse
			if err := json.Unmarshal(msg.Data, &res); err != nil {
				continue
			}
			delete(expected, res.PluginID)
			for _, d := range res.Matches {
				results = append(results, foundDevice{
					PluginID:   res.PluginID,
					DeviceID:   d.ID,
					SourceID:   d.SourceID,
					SourceName: d.SourceName,
					LocalName:  d.LocalName,
					Labels:     d.Labels,
				})
			}
		}
	}
	return applyFoundDeviceLimit(results, q.Limit)
}

func (r *Runner) searchEntitiesDistributed(q types.SearchQuery) []foundEntity {
	if r.nc == nil {
		local := r.searchEntitiesLocal(q)
		out := make([]foundEntity, 0, len(local))
		for _, e := range local {
			out = append(out, foundEntity{
				PluginID:   r.manifest.ID,
				DeviceID:   e.DeviceID,
				EntityID:   e.ID,
				Domain:     e.Domain,
				SourceID:   e.SourceID,
				SourceName: e.SourceName,
				LocalName:  e.LocalName,
				Labels:     e.Labels,
			})
		}
		return out
	}
	data, _ := json.Marshal(q)
	sub, err := r.nc.SubscribeSync(nats.NewInbox())
	if err != nil {
		local := r.searchEntitiesLocal(q)
		out := make([]foundEntity, 0, len(local))
		for _, e := range local {
			out = append(out, foundEntity{
				PluginID:   r.manifest.ID,
				DeviceID:   e.DeviceID,
				EntityID:   e.ID,
				Domain:     e.Domain,
				SourceID:   e.SourceID,
				SourceName: e.SourceName,
				LocalName:  e.LocalName,
				Labels:     e.Labels,
			})
		}
		return out
	}
	defer sub.Unsubscribe()
	r.nc.PublishRequest(SubjectSearchEntities, sub.Subject, data)

	results := make([]foundEntity, 0)
	expected := r.expectedSearchPlugins(q.PluginID)
	timeout := time.After(2 * time.Second)
	for len(expected) > 0 {
		select {
		case <-timeout:
			return applyFoundEntityLimit(results, q.Limit)
		default:
			msg, err := sub.NextMsg(100 * time.Millisecond)
			if err != nil {
				continue
			}
			var res types.SearchEntitiesResponse
			if err := json.Unmarshal(msg.Data, &res); err != nil {
				continue
			}
			delete(expected, res.PluginID)
			for _, e := range res.Matches {
				results = append(results, foundEntity{
					PluginID:   res.PluginID,
					DeviceID:   e.DeviceID,
					EntityID:   e.ID,
					Domain:     e.Domain,
					SourceID:   e.SourceID,
					SourceName: e.SourceName,
					LocalName:  e.LocalName,
					Labels:     e.Labels,
				})
			}
		}
	}
	return applyFoundEntityLimit(results, q.Limit)
}

func (r *Runner) expectedSearchPlugins(pluginID string) map[string]bool {
	expected := make(map[string]bool)
	reg := r.snapshotRegistry()
	for id := range reg {
		if pluginID != "" && pluginID != id {
			continue
		}
		expected[id] = true
	}
	if len(expected) == 0 && pluginID != "" {
		expected[pluginID] = true
	}
	if len(expected) == 0 && pluginID == "" && r.manifest.ID != "" {
		expected[r.manifest.ID] = true
	}
	return expected
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

func entityRefFromLuaArg(L *lua.LState, idx int) (pluginID, deviceID, entityID string) {
	v := L.Get(idx)
	tbl, ok := v.(*lua.LTable)
	if !ok {
		return "", "", ""
	}
	raw := lValueToAny(tbl)
	obj, _ := raw.(map[string]any)
	if obj == nil {
		return "", "", ""
	}
	return getString(obj, "PluginID"), getString(obj, "DeviceID"), getString(obj, "EntityID")
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

type luaBatchCommand struct {
	PluginID string
	DeviceID string
	EntityID string
	Payload  any
}

func sendBatchCommandArgs(L *lua.LState, idx int) ([]luaBatchCommand, string) {
	v := L.Get(idx)
	tbl, ok := v.(*lua.LTable)
	if !ok {
		return nil, "batch commands must be a table"
	}
	raw := lValueToAny(tbl)
	items, ok := raw.([]any)
	if !ok {
		return nil, "batch commands must be an array"
	}
	out := make([]luaBatchCommand, 0, len(items))
	for i, item := range items {
		obj, _ := item.(map[string]any)
		if obj == nil {
			return nil, fmt.Sprintf("batch command %d must be an object", i+1)
		}
		pluginID := getStringAny(obj, "PluginID", "plugin_id")
		deviceID := getStringAny(obj, "DeviceID", "device_id")
		entityID := getStringAny(obj, "EntityID", "entity_id")
		if pluginID == "" || deviceID == "" || entityID == "" {
			return nil, fmt.Sprintf("batch command %d missing PluginID/DeviceID/EntityID", i+1)
		}
		payload := obj["Payload"]
		if payload == nil {
			payload = obj["payload"]
		}
		if payload == nil {
			payload = map[string]any{}
		}
		out = append(out, luaBatchCommand{
			PluginID: pluginID,
			DeviceID: deviceID,
			EntityID: entityID,
			Payload:  payload,
		})
	}
	return out, ""
}

func getString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok {
		return ""
	}
	s, _ := v.(string)
	return strings.TrimSpace(s)
}

func getStringAny(m map[string]any, keys ...string) string {
	for _, key := range keys {
		if v := getString(m, key); v != "" {
			return v
		}
	}
	return ""
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

func getIntAny(m map[string]any, keys ...string) int {
	for _, key := range keys {
		if v := getInt(m, key); v != 0 {
			return v
		}
	}
	return 0
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

func getLabelsAny(m map[string]any, keys ...string) map[string][]string {
	for _, key := range keys {
		raw, ok := m[key]
		if !ok || raw == nil {
			continue
		}
		out := normalizeLabels(raw)
		if len(out) > 0 {
			return out
		}
		return map[string][]string{}
	}
	return nil
}

func normalizeLabels(raw any) map[string][]string {
	switch x := raw.(type) {
	case map[string][]string:
		return x
	case map[string]any:
		out := make(map[string][]string, len(x))
		for k, v := range x {
			switch vv := v.(type) {
			case string:
				if strings.TrimSpace(vv) != "" {
					out[k] = []string{strings.TrimSpace(vv)}
				}
			case []string:
				if len(vv) > 0 {
					out[k] = vv
				}
			case []any:
				values := make([]string, 0, len(vv))
				for _, item := range vv {
					if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
						values = append(values, strings.TrimSpace(s))
					}
				}
				if len(values) > 0 {
					out[k] = values
				}
			}
		}
		return out
	default:
		return nil
	}
}

func applyFoundDeviceLimit(items []foundDevice, limit int) []foundDevice {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
}

func applyFoundEntityLimit(items []foundEntity, limit int) []foundEntity {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
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
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		log.Printf("plugin-runner: failed to marshal script state: %v", err)
		return
	}
	path := r.scriptStatePath(deviceID, entityID)
	if err := r.writeIfChanged(path, data); err != nil {
		log.Printf("plugin-runner: failed to write script state: %v", err)
	}
}

func (r *Runner) invalidateScriptRuntime(deviceID, entityID string) {
	key := scriptKey{DeviceID: deviceID, EntityID: entityID}
	r.scriptsMu.Lock()
	defer r.scriptsMu.Unlock()
	if rt, ok := r.scripts[key]; ok {
		rt.mu.Lock()
		rt.L.Close()
		rt.closed = true
		rt.mu.Unlock()
		delete(r.scripts, key)
	}
}

func (r *Runner) ScriptGet(deviceID, entityID string) (string, string, error) {
	path := r.scriptPath(deviceID, entityID)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", path, fmt.Errorf("script not found")
	}
	return string(data), path, nil
}

func (r *Runner) ScriptPut(deviceID, entityID, source string) (string, error) {
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

func (r *Runner) ScriptDelete(deviceID, entityID string, purgeState bool) error {
	path := r.scriptPath(deviceID, entityID)
	_ = os.Remove(path)
	r.invalidateScriptRuntime(deviceID, entityID)
	if purgeState {
		_ = os.Remove(r.scriptStatePath(deviceID, entityID))
	}
	return nil
}

func (r *Runner) ScriptStateGet(deviceID, entityID string) (map[string]any, string) {
	path := r.scriptStatePath(deviceID, entityID)
	return r.loadScriptState(deviceID, entityID), path
}

func (r *Runner) ScriptStatePut(deviceID, entityID string, state map[string]any) (string, error) {
	if state == nil {
		state = map[string]any{}
	}
	path := r.scriptStatePath(deviceID, entityID)
	r.saveScriptState(deviceID, entityID, state)
	return path, nil
}

func (r *Runner) ScriptStateDelete(deviceID, entityID string) (string, error) {
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

func payloadType(payload json.RawMessage) string {
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

func entityDataToMap(data types.EntityData) map[string]any {
	out := map[string]any{
		"desired":         jsonRawToAny(data.Desired),
		"reported":        jsonRawToAny(data.Reported),
		"effective":       jsonRawToAny(data.Effective),
		"sync_status":     string(data.SyncStatus),
		"last_command_id": data.LastCommandID,
		"last_event_id":   data.LastEventID,
	}
	if !data.UpdatedAt.IsZero() {
		out["updated_at"] = data.UpdatedAt.UnixMilli()
	}
	return out
}
