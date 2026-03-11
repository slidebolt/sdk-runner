package runner

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	types "github.com/slidebolt/sdk-types"
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
	dynamicSubs map[string]*dynamicQuery
	state       map[string]any
	mu          sync.Mutex
	closed      bool
}

type dynamicQuery struct {
	id            string
	query         scriptQuery
	eventSubs     map[string]string
	changeHandler string
	members       map[string]foundEntity
	revision      int64
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

// refresh re-queries the entity set and fires changeHandler for membership changes.
// The caller must hold rt.mu.
// If find returns nil (as opposed to an empty slice), it signals a transient query
// failure; in that case refresh is a no-op to avoid clearing valid members.
func (dq *dynamicQuery) refresh(find func(scriptQuery) []foundEntity, rt *scriptRuntime, call func(*scriptRuntime, string, map[string]any)) {
	if isEmptyScriptQuery(dq.query) {
		dq.members = map[string]foundEntity{}
		return
	}
	current := find(dq.query)
	if current == nil {
		return // transient failure — preserve existing members
	}
	next := make(map[string]foundEntity, len(current))
	for _, ent := range current {
		next[entityMemberKey(ent.PluginID, ent.DeviceID, ent.EntityID)] = ent
	}
	if dq.members == nil {
		dq.members = map[string]foundEntity{}
	}
	if dq.changeHandler == "" {
		dq.members = next
		return
	}
	for key, ent := range next {
		prev, ok := dq.members[key]
		if !ok {
			dq.revision++
			call(rt, dq.changeHandler, dynamicQueryChangePayload(dq, "added", ent, len(next)))
			continue
		}
		if !foundEntityEqual(prev, ent) {
			dq.revision++
			call(rt, dq.changeHandler, dynamicQueryChangePayload(dq, "updated", ent, len(next)))
		}
	}
	for key, ent := range dq.members {
		if _, ok := next[key]; !ok {
			dq.revision++
			call(rt, dq.changeHandler, dynamicQueryChangePayload(dq, "removed", ent, len(next)))
		}
	}
	dq.members = next
}

// dispatch routes an incoming event to this query's event subscriptions,
// but only if the event source is a current member of the query.
// The caller must hold rt.mu.
func (dq *dynamicQuery) dispatch(rt *scriptRuntime, env types.EntityEventEnvelope, selectors []string, router *subscriptionRouter, call func(*scriptRuntime, string, map[string]any), payloadAction string) {
	if len(dq.eventSubs) == 0 {
		return
	}
	if _, ok := dq.members[entityMemberKey(env.PluginID, env.DeviceID, env.EntityID)]; !ok {
		return
	}
	for handler := range router.match(dq.eventSubs, selectors) {
		payload := luaEventFromEnvelope(env, payloadAction)
		payload["QueryID"] = dq.id
		call(rt, handler, payload)
	}
}

// --- Runner event/command dispatch ---

func (r *Runner) handleEvent(env types.EntityEventEnvelope) {
	// Ensure listener scripts written at runtime are loaded before event match.
	// This keeps cross-plugin and listener-only event flows deterministic.
	r.scriptCache.sync()

	payloadAction := payloadType(env.Payload)
	systemAction := "state"
	if env.EntityType == "automation" {
		systemAction = "tick"
	}
	selectors := eventSelectors(env, systemAction, payloadAction)

	if r.logger != nil {
		r.logger.Debug("dispatching Lua event",
			"plugin", env.PluginID, "device", env.DeviceID, "entity", env.EntityID,
			"system", systemAction, "payload", payloadAction, "selectors", selectors)
	}

	for _, rt := range r.scriptCache.snapshot() {
		rt.mu.Lock()
		if rt.closed {
			rt.mu.Unlock()
			continue
		}
		for handler, selector := range r.router.match(rt.eventSubs, selectors) {
			if r.logger != nil {
				r.logger.Debug("Lua event match found", "selector", selector, "handler", handler, "script", rt.key.EntityID)
			}
			r.callLuaHandler(rt, handler, luaEventFromEnvelope(env, payloadAction))
		}
		for _, dq := range rt.dynamicSubs {
			dq.dispatch(rt, env, selectors, r.router, r.callLuaHandler, payloadAction)
		}
		rt.mu.Unlock()
	}
}

func (r *Runner) handleCommand(cmd types.Command) {
	action := payloadType(cmd.Payload)
	selectors := commandSelectors(cmd, action)

	rt, err := r.scriptCache.ensure(cmd.DeviceID, cmd.EntityID)
	if err != nil || rt == nil {
		return
	}
	rt.mu.Lock()
	defer rt.mu.Unlock()
	if rt.closed {
		return
	}
	for handler := range r.router.match(rt.commandSubs, selectors) {
		r.callLuaHandler(rt, handler, luaCommandFromTypes(cmd, action))
	}
}

func (r *Runner) refreshAllDynamicQueries() {
	findFn := r.findEntitiesRPC
	for _, rt := range r.scriptCache.snapshot() {
		rt.mu.Lock()
		if rt.closed {
			rt.mu.Unlock()
			continue
		}
		for _, dq := range rt.dynamicSubs {
			dq.refresh(findFn, rt, r.callLuaHandler)
		}
		rt.mu.Unlock()
	}
}

// --- Thin wrappers to preserve test-facing API ---

// ensureScriptRuntime delegates to scriptCache.ensure (called by tests).
func (r *Runner) ensureScriptRuntime(deviceID, entityID string) (*scriptRuntime, error) {
	return r.scriptCache.ensure(deviceID, entityID)
}

// invalidateScriptRuntime delegates to scriptCache.invalidate (called by tests).
func (r *Runner) invalidateScriptRuntime(deviceID, entityID string) {
	r.scriptCache.invalidate(deviceID, entityID)
}

// scriptPath returns the filesystem path for a script file (called by tests).
func (r *Runner) scriptPath(deviceID, entityID string) string {
	return r.store.sourcePath(deviceID, entityID)
}

// --- Runtime loading ---

func (r *Runner) loadScriptRuntime(key scriptKey, path string, mtimeUnix int64) (*scriptRuntime, error) {
	L := lua.NewState()
	rt := &scriptRuntime{
		key:         key,
		path:        path,
		statePath:   r.store.statePath(key.DeviceID, key.EntityID),
		mtimeUnix:   mtimeUnix,
		L:           L,
		eventSubs:   map[string]string{},
		commandSubs: map[string]string{},
		dynamicSubs: map[string]*dynamicQuery{},
		state:       r.store.loadState(key.DeviceID, key.EntityID),
	}
	rt.installCtx(r.newLuaEnv())

	if err := L.DoFile(path); err != nil {
		L.Close()
		return nil, fmt.Errorf("lua load failed (%s): %w", path, err)
	}
	if fn := L.GetGlobal("OnInit"); fn.Type() == lua.LTFunction {
		if err := L.CallByParam(lua.P{Fn: fn, NRet: 0, Protect: true}, L.GetGlobal("Ctx")); err != nil {
			L.Close()
			return nil, fmt.Errorf("lua OnInit failed (%s): %w", path, err)
		}
	}
	return rt, nil
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
			r.logger.Error("lua handler error",
				"device", rt.key.DeviceID, "entity", rt.key.EntityID,
				"handler", handler, "error", err)
		}
	}
}

// --- Public script/state API ---

func (r *Runner) ScriptGet(deviceID, entityID string) (string, string, error) {
	return r.store.readSource(deviceID, entityID)
}

func (r *Runner) ScriptPut(deviceID, entityID, source string) (string, error) {
	path, err := r.store.writeSource(deviceID, entityID, source)
	if err != nil {
		return path, err
	}
	r.scriptCache.invalidate(deviceID, entityID)
	_, _ = r.scriptCache.ensure(deviceID, entityID)
	return path, nil
}

func (r *Runner) ScriptDelete(deviceID, entityID string, purgeState bool) error {
	r.store.deleteSource(deviceID, entityID)
	r.scriptCache.invalidate(deviceID, entityID)
	if purgeState {
		r.store.deleteState(deviceID, entityID)
	}
	return nil
}

func (r *Runner) ScriptStateGet(deviceID, entityID string) (map[string]any, string) {
	return r.store.loadState(deviceID, entityID), r.store.statePath(deviceID, entityID)
}

func (r *Runner) ScriptStatePut(deviceID, entityID string, state map[string]any) (string, error) {
	if state == nil {
		state = map[string]any{}
	}
	r.store.saveState(deviceID, entityID, state, r.writeIfChanged)
	return r.store.statePath(deviceID, entityID), nil
}

func (r *Runner) ScriptStateDelete(deviceID, entityID string) (string, error) {
	path := r.store.statePath(deviceID, entityID)
	r.store.deleteState(deviceID, entityID)
	return path, nil
}

// --- Ctx installation (Lua API surface) ---

func (rt *scriptRuntime) installCtx(env *luaEnv) {
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
			rt.commandSubs[fmt.Sprintf("%s.%s.%s.%s", pluginID, deviceID, entityID, action)] = handler
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
			if env.saveScriptState != nil {
				env.saveScriptState(rt.key.DeviceID, rt.key.EntityID, rt.state)
			}
			return 0
		},
		"DeleteState": func(L *lua.LState) int {
			key := strings.TrimSpace(L.CheckString(2))
			if key == "" {
				return 0
			}
			delete(rt.state, key)
			if env.saveScriptState != nil {
				env.saveScriptState(rt.key.DeviceID, rt.key.EntityID, rt.state)
			}
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
			if env.findEntities != nil {
				for _, ent := range env.findEntities(query) {
					out.Append(newEntityHandle(L, rt, ent, env))
				}
			}
			L.Push(out)
			return 1
		},
		"FindDevices": func(L *lua.LState) int {
			query := queryFromLuaArg(L, 2)
			out := L.NewTable()
			if env.findDevices != nil {
				for _, dev := range env.findDevices(query) {
					out.Append(mapToLTable(L, map[string]any{
						"PluginID": dev.PluginID, "DeviceID": dev.DeviceID,
						"SourceID": dev.SourceID, "SourceName": dev.SourceName,
						"LocalName": dev.LocalName, "Labels": dev.Labels,
					}))
				}
			}
			L.Push(out)
			return 1
		},
		"GetEntity": func(L *lua.LState) int {
			pluginID := strings.TrimSpace(L.CheckString(2))
			deviceID := strings.TrimSpace(L.CheckString(3))
			entityID := strings.TrimSpace(L.CheckString(4))
			if env.getEntity == nil {
				L.Push(lua.LNil)
				L.Push(lua.LString("getEntity unavailable"))
				return 2
			}
			ent, err := env.getEntity(pluginID, deviceID, entityID)
			if err != nil {
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			handle := newEntityHandle(L, rt, foundEntity{
				PluginID: pluginID, DeviceID: ent.DeviceID, EntityID: ent.ID,
				Domain: ent.Domain, SourceID: ent.SourceID, SourceName: ent.SourceName,
				LocalName: ent.LocalName, Labels: ent.Labels,
			}, env)
			handle.RawSetString("Actions", anyToLValue(L, toAnySlice(ent.Actions)))
			handle.RawSetString("Data", anyToLValue(L, entityDataToMap(ent.Data)))
			L.Push(handle)
			L.Push(lua.LNil)
			return 2
		},
		"GetDevice": func(L *lua.LState) int {
			pluginID := strings.TrimSpace(L.CheckString(2))
			deviceID := strings.TrimSpace(L.CheckString(3))
			if env.getDevice == nil {
				L.Push(lua.LNil)
				L.Push(lua.LString("getDevice unavailable"))
				return 2
			}
			dev, err := env.getDevice(pluginID, deviceID)
			if err != nil {
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(mapToLTable(L, map[string]any{
				"PluginID": pluginID, "DeviceID": dev.ID,
				"SourceID": dev.SourceID, "SourceName": dev.SourceName,
				"LocalName": dev.LocalName,
			}))
			L.Push(lua.LNil)
			return 2
		},
		"CreateDynamicQuery": func(L *lua.LState) int {
			query := queryFromLuaArg(L, 2)
			dq := &dynamicQuery{
				id:        nextID("dynq"),
				query:     query,
				eventSubs: map[string]string{},
				members:   map[string]foundEntity{},
			}
			rt.dynamicSubs[dq.id] = dq
			if !isEmptyScriptQuery(query) && env.findEntities != nil {
				noop := func(*scriptRuntime, string, map[string]any) {}
				dq.refresh(env.findEntities, rt, noop)
			}
			L.Push(newDynamicQueryCollection(L, rt, dq, env))
			return 1
		},
		"SendCommand": func(L *lua.LState) int {
			pluginID, deviceID, entityID, payload := sendCommandArgs(L)
			if pluginID == "" || deviceID == "" || entityID == "" {
				L.Push(lua.LNil)
				L.Push(lua.LString("missing PluginID/DeviceID/EntityID"))
				return 2
			}
			if env.sendCommand == nil {
				L.Push(lua.LNil)
				L.Push(lua.LString("sendCommand unavailable"))
				return 2
			}
			body, _ := json.Marshal(payload)
			status, err := env.sendCommand(pluginID, deviceID, entityID, body)
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
			if env.logger != nil {
				env.logger.Log(context.Background(), LevelTrace, "lua batch start",
					"device_id", rt.key.DeviceID, "entity_id", rt.key.EntityID,
					"count", len(items), "start_unix_ms", batchStart.UnixMilli())
			}
			out := L.NewTable()
			for idx, item := range items {
				itemStart := time.Now()
				if env.logger != nil {
					env.logger.Log(context.Background(), LevelTrace, "lua batch item dispatch",
						"device_id", rt.key.DeviceID, "entity_id", rt.key.EntityID,
						"index", idx, "plugin_id", item.PluginID,
						"target_device_id", item.DeviceID, "target_entity_id", item.EntityID,
						"start_unix_ms", itemStart.UnixMilli())
				}
				body, _ := json.Marshal(item.Payload)
				result := map[string]any{
					"PluginID": item.PluginID, "DeviceID": item.DeviceID, "EntityID": item.EntityID,
				}
				if env.sendCommand != nil {
					status, err := env.sendCommand(item.PluginID, item.DeviceID, item.EntityID, body)
					if err != nil {
						result["OK"] = false
						result["Error"] = err.Error()
						if env.logger != nil {
							env.logger.Log(context.Background(), LevelTrace, "lua batch item result",
								"device_id", rt.key.DeviceID, "entity_id", rt.key.EntityID,
								"index", idx, "ok", false, "error", err.Error(),
								"elapsed_ms", time.Since(itemStart).Milliseconds())
						}
					} else {
						result["OK"] = true
						result["CommandID"] = status.CommandID
						result["State"] = string(status.State)
						result["Error"] = status.Error
						if env.logger != nil {
							env.logger.Log(context.Background(), LevelTrace, "lua batch item result",
								"device_id", rt.key.DeviceID, "entity_id", rt.key.EntityID,
								"index", idx, "command_id", status.CommandID,
								"state", string(status.State), "ok", true,
								"elapsed_ms", time.Since(itemStart).Milliseconds())
						}
					}
				} else {
					result["OK"] = false
					result["Error"] = "sendCommand unavailable"
				}
				out.Append(mapToLTable(L, result))
			}
			if env.logger != nil {
				env.logger.Log(context.Background(), LevelTrace, "lua batch done",
					"device_id", rt.key.DeviceID, "entity_id", rt.key.EntityID,
					"count", len(items), "elapsed_ms", time.Since(batchStart).Milliseconds())
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
			if env.emitEvent == nil {
				L.Push(lua.LNil)
				L.Push(lua.LString("emitEvent unavailable"))
				return 2
			}
			rawPayload, _ := json.Marshal(payload)
			err := env.emitEvent(types.InboundEvent{
				DeviceID: deviceID, EntityID: entityID,
				CorrelationID: correlationID, Payload: rawPayload,
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
			if msg := L.OptString(2, ""); msg != "" && env.logger != nil {
				env.logger.Info(msg, "device", rt.key.DeviceID, "entity", rt.key.EntityID)
			}
			return 0
		},
		"LogDebug": func(L *lua.LState) int {
			if msg := L.OptString(2, ""); msg != "" && env.logger != nil {
				env.logger.Debug(msg, "device", rt.key.DeviceID, "entity", rt.key.EntityID)
			}
			return 0
		},
		"LogWarn": func(L *lua.LState) int {
			if msg := L.OptString(2, ""); msg != "" && env.logger != nil {
				env.logger.Warn(msg, "device", rt.key.DeviceID, "entity", rt.key.EntityID)
			}
			return 0
		},
		"LogError": func(L *lua.LState) int {
			if msg := L.OptString(2, ""); msg != "" && env.logger != nil {
				env.logger.Error(msg, "device", rt.key.DeviceID, "entity", rt.key.EntityID)
			}
			return 0
		},
		"Find": func(L *lua.LState) int {
			queryStr := strings.TrimSpace(L.CheckString(2))
			if queryStr == "" {
				queryStr = "?q=*"
			}
			if env.gatewayFind == nil {
				if env.logger != nil {
					env.logger.Error("Ctx:Find failed: gatewayFind unavailable")
				}
				L.Push(newEntityCollection(L, rt, nil, env))
				return 1
			}
			if env.logger != nil {
				env.logger.Info("Ctx:Find dispatching NATS request", "subject", SubjectGatewayDiscovery, "query", queryStr)
			}
			found, err := env.gatewayFind(queryStr)
			if err != nil {
				if env.logger != nil {
					env.logger.Error("Ctx:Find failed", "query", queryStr, "error", err)
				}
				L.Push(newEntityCollection(L, rt, nil, env))
				return 1
			}
			if env.logger != nil {
				env.logger.Info("Ctx:Find received response", "count", len(found))
			}
			L.Push(newEntityCollection(L, rt, found, env))
			return 1
		},
	})

	thisEntity := foundEntity{
		PluginID: env.pluginID,
		DeviceID: rt.key.DeviceID,
		EntityID: rt.key.EntityID,
	}
	if env.loadEntity != nil {
		if ent := env.loadEntity(rt.key.DeviceID, rt.key.EntityID); ent.ID != "" {
			thisEntity.Domain = ent.Domain
			thisEntity.SourceID = ent.SourceID
			thisEntity.SourceName = ent.SourceName
			thisEntity.LocalName = ent.LocalName
			thisEntity.Labels = ent.Labels
		}
	}
	thisHandle := newEntityHandle(rt.L, rt, thisEntity, env)
	ctx.RawSetString("This", thisHandle)
	rt.L.SetGlobal("Ctx", ctx)
	rt.L.SetGlobal("This", thisHandle)
}

// newEntityCollection creates a Lua table representing a static collection of entities.
func newEntityCollection(L *lua.LState, rt *scriptRuntime, entities []foundEntity, env *luaEnv) *lua.LTable {
	t := L.NewTable()
	list := L.NewTable()
	for _, ent := range entities {
		list.Append(newEntityHandle(L, rt, ent, env))
	}
	t.RawSetString("Entities", list)

	L.SetFuncs(t, map[string]lua.LGFunction{
		"OnEvent": func(L *lua.LState) int {
			eventType := strings.TrimSpace(L.CheckString(2))
			handler := strings.TrimSpace(L.CheckString(3))
			if eventType == "" || handler == "" {
				return 0
			}
			if env != nil && env.logger != nil {
				env.logger.Info("Collection:OnEvent bulk registering", "count", len(entities), "type", eventType, "handler", handler)
			}
			for _, ent := range entities {
				selector := entityScopedSelector(ent.PluginID, ent.DeviceID, ent.EntityID, eventType)
				if env != nil && env.logger != nil {
					env.logger.Info("Collection:OnEvent registering selector", "selector", selector)
				}
				rt.eventSubs[selector] = handler
			}
			return 0
		},
		"OnCommand": func(L *lua.LState) int {
			action := strings.TrimSpace(L.CheckString(2))
			handler := strings.TrimSpace(L.CheckString(3))
			if action == "" || handler == "" {
				return 0
			}
			for _, ent := range entities {
				rt.commandSubs[entityScopedSelector(ent.PluginID, ent.DeviceID, ent.EntityID, action)] = handler
			}
			return 0
		},
		"SendCommand": func(L *lua.LState) int {
			payload := lValueToAny(L.CheckAny(2))
			body, _ := json.Marshal(payload)
			results := L.NewTable()
			start := time.Now()
			okCount, failCount := 0, 0
			if env != nil && env.logger != nil {
				env.logger.Log(context.Background(), LevelTrace, "lua collection send start",
					"count", len(entities), "payload_bytes", len(body))
			}
			for i, ent := range entities {
				itemStart := time.Now()
				if env != nil && env.logger != nil {
					env.logger.Log(context.Background(), LevelTrace, "lua collection send dispatch",
						"index", i, "target_plugin_id", ent.PluginID,
						"target_device_id", ent.DeviceID, "target_entity_id", ent.EntityID)
				}
				res := map[string]any{"PluginID": ent.PluginID, "DeviceID": ent.DeviceID, "EntityID": ent.EntityID}
				if env != nil && env.sendCommand != nil {
					status, err := env.sendCommand(ent.PluginID, ent.DeviceID, ent.EntityID, body)
					if err != nil {
						res["OK"] = false
						res["Error"] = err.Error()
						failCount++
						if env.logger != nil {
							env.logger.Log(context.Background(), LevelTrace, "lua collection send result",
								"index", i, "ok", false, "error", err.Error(),
								"elapsed_ms", time.Since(itemStart).Milliseconds())
						}
					} else {
						res["OK"] = true
						res["CommandID"] = status.CommandID
						okCount++
						if env.logger != nil {
							env.logger.Log(context.Background(), LevelTrace, "lua collection send result",
								"index", i, "ok", true, "command_id", status.CommandID,
								"command_state", status.State, "elapsed_ms", time.Since(itemStart).Milliseconds())
						}
					}
				} else {
					res["OK"] = false
					res["Error"] = "sendCommand unavailable"
					failCount++
				}
				results.Append(mapToLTable(L, res))
			}
			if env != nil && env.logger != nil {
				env.logger.Log(context.Background(), LevelTrace, "lua collection send done",
					"count", len(entities), "ok_count", okCount, "fail_count", failCount,
					"elapsed_ms", time.Since(start).Milliseconds())
			}
			L.Push(results)
			return 1
		},
		"RestoreSnapshot": func(L *lua.LState) int {
			nameOrID := strings.TrimSpace(L.CheckString(2))
			results := L.NewTable()
			if nameOrID == "" {
				L.Push(results)
				return 1
			}
			for _, ent := range entities {
				res := map[string]any{"PluginID": ent.PluginID, "DeviceID": ent.DeviceID, "EntityID": ent.EntityID}
				if env != nil && env.restoreSnapshot != nil {
					snapshotID, err := env.restoreSnapshot(ent, nameOrID)
					if err != nil {
						res["OK"] = false
						res["Error"] = err.Error()
					} else {
						res["OK"] = true
						res["SnapshotID"] = snapshotID
					}
				} else {
					res["OK"] = false
					res["Error"] = "restoreSnapshot unavailable"
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
			if env != nil && env.emitEvent != nil {
				for _, ent := range entities {
					_ = env.emitEvent(types.InboundEvent{
						DeviceID: ent.DeviceID, EntityID: ent.EntityID,
						CorrelationID: correlationID, Payload: rawPayload,
					})
				}
			}
			return 0
		},
		"Count": func(L *lua.LState) int {
			L.Push(lua.LNumber(len(entities)))
			return 1
		},
		"FindEntities": func(L *lua.LState) int {
			query := queryFromLuaArg(L, 2)
			filtered := applyFoundEntityLimit(filterFoundEntities(entities, query), query.Limit)
			L.Push(newEntityCollection(L, rt, filtered, env))
			return 1
		},
	})
	return t
}

// newEntityHandle creates a Lua table representing a single entity with methods.
func newEntityHandle(L *lua.LState, rt *scriptRuntime, ent foundEntity, env *luaEnv) *lua.LTable {
	t := mapToLTable(L, map[string]any{
		"PluginID": ent.PluginID, "DeviceID": ent.DeviceID, "EntityID": ent.EntityID,
		"Domain": ent.Domain, "SourceID": ent.SourceID, "SourceName": ent.SourceName,
		"LocalName": ent.LocalName, "Labels": ent.Labels,
	})
	L.SetFuncs(t, map[string]lua.LGFunction{
		"OnEvent": func(L *lua.LState) int {
			eventType := strings.TrimSpace(L.CheckString(2))
			handler := strings.TrimSpace(L.CheckString(3))
			if eventType != "" && handler != "" {
				rt.eventSubs[entityScopedSelector(ent.PluginID, ent.DeviceID, ent.EntityID, eventType)] = handler
			}
			return 0
		},
		"OnCommand": func(L *lua.LState) int {
			action := strings.TrimSpace(L.CheckString(2))
			handler := strings.TrimSpace(L.CheckString(3))
			if action != "" && handler != "" {
				rt.commandSubs[entityScopedSelector(ent.PluginID, ent.DeviceID, ent.EntityID, action)] = handler
			}
			return 0
		},
		"SendCommand": func(L *lua.LState) int {
			action := strings.TrimSpace(L.CheckString(2))
			if action == "" {
				L.Push(lua.LNil)
				L.Push(lua.LString("action is required"))
				return 2
			}
			if env == nil || env.sendCommand == nil {
				L.Push(lua.LNil)
				L.Push(lua.LString("sendCommand unavailable"))
				return 2
			}
			payload := lValueToAny(L.Get(3))
			body, _ := json.Marshal(mergeActionPayload(action, payload))
			status, err := env.sendCommand(ent.PluginID, ent.DeviceID, ent.EntityID, body)
			if err != nil {
				L.Push(lua.LNil)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(mapToLTable(L, map[string]any{
				"PluginID": ent.PluginID, "DeviceID": ent.DeviceID, "EntityID": ent.EntityID,
				"CommandID": status.CommandID, "State": string(status.State), "Error": status.Error,
			}))
			L.Push(lua.LNil)
			return 2
		},
		"RestoreSnapshot": func(L *lua.LState) int {
			nameOrID := strings.TrimSpace(L.CheckString(2))
			if nameOrID == "" {
				L.Push(lua.LFalse)
				L.Push(lua.LString("snapshot name/id is required"))
				return 2
			}
			if env == nil || env.restoreSnapshot == nil {
				L.Push(lua.LFalse)
				L.Push(lua.LString("restoreSnapshot unavailable"))
				return 2
			}
			snapshotID, err := env.restoreSnapshot(ent, nameOrID)
			if err != nil {
				L.Push(lua.LFalse)
				L.Push(lua.LString(err.Error()))
				return 2
			}
			L.Push(lua.LTrue)
			L.Push(lua.LString(snapshotID))
			return 2
		},
		"GetMembers": func(L *lua.LState) int {
			q, ok := defaultMembersQuery(ent)
			if L.GetTop() >= 2 && L.Get(2) != lua.LNil {
				q = queryFromLuaArg(L, 2)
				ok = !isEmptyScriptQuery(q)
			}
			dq := &dynamicQuery{
				id:        nextID("dynq"),
				query:     q,
				eventSubs: map[string]string{},
				members:   map[string]foundEntity{},
			}
			rt.dynamicSubs[dq.id] = dq
			if ok && env != nil && env.findEntities != nil {
				noop := func(*scriptRuntime, string, map[string]any) {}
				dq.refresh(env.findEntities, rt, noop)
			}
			L.Push(newDynamicQueryCollection(L, rt, dq, env))
			return 1
		},
	})
	return t
}

// newDynamicQueryCollection creates a Lua table for a live dynamic query.
func newDynamicQueryCollection(L *lua.LState, rt *scriptRuntime, dq *dynamicQuery, env *luaEnv) *lua.LTable {
	t := L.NewTable()
	t.RawSetString("ID", lua.LString(dq.id))
	L.SetFuncs(t, map[string]lua.LGFunction{
		"ID": func(L *lua.LState) int {
			L.Push(lua.LString(dq.id))
			return 1
		},
		"OnEvent": func(L *lua.LState) int {
			selector := strings.TrimSpace(L.CheckString(2))
			handler := strings.TrimSpace(L.CheckString(3))
			if selector != "" && handler != "" {
				dq.eventSubs[selector] = handler
			}
			return 0
		},
		"OnChange": func(L *lua.LState) int {
			handler := strings.TrimSpace(L.CheckString(2))
			dq.changeHandler = handler
			return 0
		},
		"Snapshot": func(L *lua.LState) int {
			if env != nil && env.findEntities != nil {
				noop := func(*scriptRuntime, string, map[string]any) {}
				dq.refresh(env.findEntities, rt, noop)
			}
			out := L.NewTable()
			for _, ent := range dq.members {
				out.Append(mapToLTable(L, map[string]any{
					"PluginID": ent.PluginID, "DeviceID": ent.DeviceID, "EntityID": ent.EntityID,
					"Domain": ent.Domain, "SourceID": ent.SourceID, "SourceName": ent.SourceName,
					"LocalName": ent.LocalName, "Labels": ent.Labels,
				}))
			}
			L.Push(out)
			return 1
		},
		"Count": func(L *lua.LState) int {
			if env != nil && env.findEntities != nil {
				noop := func(*scriptRuntime, string, map[string]any) {}
				dq.refresh(env.findEntities, rt, noop)
			}
			L.Push(lua.LNumber(len(dq.members)))
			return 1
		},
		"FindEntities": func(L *lua.LState) int {
			if env != nil && env.findEntities != nil {
				noop := func(*scriptRuntime, string, map[string]any) {}
				dq.refresh(env.findEntities, rt, noop)
			}
			current := make([]foundEntity, 0, len(dq.members))
			for _, ent := range dq.members {
				current = append(current, ent)
			}
			query := queryFromLuaArg(L, 2)
			filtered := applyFoundEntityLimit(filterFoundEntities(current, query), query.Limit)
			L.Push(newEntityCollection(L, rt, filtered, env))
			return 1
		},
		"Close": func(L *lua.LState) int {
			delete(rt.dynamicSubs, dq.id)
			return 0
		},
	})
	return t
}

// --- resolveSnapshotID ---

func (r *Runner) resolveSnapshotID(deviceID, entityID, nameOrID string) (string, error) {
	nameOrID = strings.TrimSpace(nameOrID)
	if nameOrID == "" {
		return "", fmt.Errorf("snapshot name/id is required")
	}
	ent := r.loadEntity(deviceID, entityID)
	if ent.ID == "" {
		return "", fmt.Errorf("entity not found: %s/%s", deviceID, entityID)
	}
	if _, ok := ent.Snapshots[nameOrID]; ok {
		return nameOrID, nil
	}
	for id, snap := range ent.Snapshots {
		if strings.TrimSpace(snap.Name) == nameOrID {
			return id, nil
		}
	}
	return "", fmt.Errorf("snapshot not found by name/id: %s", nameOrID)
}

// --- expectedSearchPlugins ---

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

// --- Type bridge ---

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

// --- Formatters ---

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

func eventSelectors(env types.EntityEventEnvelope, systemAction, payloadAction string) []string {
	p, d, e := env.PluginID, env.DeviceID, env.EntityID
	out := []string{
		fmt.Sprintf("%s.%s.%s", p, d, e),
		fmt.Sprintf("%s.%s", p, e),
		p,
	}
	add := func(base, action string) {
		if action != "" {
			out = append(out, base+"."+action)
		}
	}
	add(fmt.Sprintf("%s.%s.%s", p, d, e), systemAction)
	add(fmt.Sprintf("%s.%s", p, e), systemAction)
	add(p, systemAction)
	if payloadAction != "" && payloadAction != systemAction {
		add(fmt.Sprintf("%s.%s.%s", p, d, e), payloadAction)
		add(fmt.Sprintf("%s.%s", p, e), payloadAction)
		add(p, payloadAction)
	}
	if systemAction != "" && payloadAction != "" && systemAction != payloadAction {
		hier := systemAction + "." + payloadAction
		add(fmt.Sprintf("%s.%s.%s", p, d, e), hier)
		add(fmt.Sprintf("%s.%s", p, e), hier)
		add(p, hier)
	}
	seen := make(map[string]bool)
	unique := make([]string, 0, len(out))
	for _, s := range out {
		if !seen[s] {
			seen[s] = true
			unique = append(unique, s)
		}
	}
	return unique
}

func luaCommandFromTypes(cmd types.Command, action string) map[string]any {
	return map[string]any{
		"ID": cmd.ID, "PluginID": cmd.PluginID, "DeviceID": cmd.DeviceID,
		"EntityID": cmd.EntityID, "EntityType": cmd.EntityType,
		"Action": action, "Payload": jsonRawToAny(cmd.Payload),
	}
}

func luaEventFromEnvelope(env types.EntityEventEnvelope, eventType string) map[string]any {
	return map[string]any{
		"EventID": env.EventID, "PluginID": env.PluginID, "DeviceID": env.DeviceID,
		"EntityID": env.EntityID, "EntityType": env.EntityType,
		"CorrelationID": env.CorrelationID, "Type": eventType,
		"Payload": jsonRawToAny(env.Payload), "CreatedAt": env.CreatedAt.UnixMilli(),
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
		"desired": jsonRawToAny(data.Desired), "reported": jsonRawToAny(data.Reported),
		"effective": jsonRawToAny(data.Effective), "sync_status": string(data.SyncStatus),
		"last_command_id": data.LastCommandID, "last_event_id": data.LastEventID,
	}
	if !data.UpdatedAt.IsZero() {
		out["updated_at"] = data.UpdatedAt.UnixMilli()
	}
	return out
}

// --- Argument parsers ---

func queryFromLuaArg(L *lua.LState, idx int) scriptQuery {
	v := L.Get(idx)
	switch x := v.(type) {
	case lua.LString:
		s := strings.TrimSpace(string(x))
		if q, ok := parseScriptQueryString(s); ok {
			return q
		}
		return scriptQuery{Text: s}
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

func parseScriptQueryString(raw string) (scriptQuery, bool) {
	s := strings.TrimSpace(raw)
	if s == "" || !strings.HasPrefix(s, "?") {
		return scriptQuery{}, false
	}
	values, err := url.ParseQuery(strings.TrimPrefix(s, "?"))
	if err != nil {
		return scriptQuery{}, false
	}
	out := scriptQuery{
		Text:     firstQueryValue(values, "q", "text"),
		Pattern:  firstQueryValue(values, "pattern"),
		PluginID: firstQueryValue(values, "plugin_id"),
		DeviceID: firstQueryValue(values, "device_id"),
		EntityID: firstQueryValue(values, "entity_id"),
		Domain:   firstQueryValue(values, "domain"),
	}
	if lim := strings.TrimSpace(firstQueryValue(values, "limit")); lim != "" {
		if n, err := strconv.Atoi(lim); err == nil {
			out.Limit = n
		}
	}
	labels := map[string][]string{}
	for _, rawLabel := range values["label"] {
		parts := strings.SplitN(strings.TrimSpace(rawLabel), ":", 2)
		if len(parts) != 2 {
			continue
		}
		k, v := strings.TrimSpace(parts[0]), strings.TrimSpace(parts[1])
		if k != "" && v != "" {
			labels[k] = append(labels[k], v)
		}
	}
	if len(labels) > 0 {
		out.Labels = labels
	}
	return out, true
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
	return strings.TrimSpace(L.CheckString(2)), strings.TrimSpace(L.CheckString(3)),
		strings.TrimSpace(L.CheckString(4)), lValueToAny(L.CheckAny(5))
}

func entityRefFromLuaArg(L *lua.LState, idx int) (pluginID, deviceID, entityID string) {
	tbl, ok := L.Get(idx).(*lua.LTable)
	if !ok {
		return "", "", ""
	}
	obj, _ := lValueToAny(tbl).(map[string]any)
	if obj == nil {
		return "", "", ""
	}
	return getString(obj, "PluginID"), getString(obj, "DeviceID"), getString(obj, "EntityID")
}

func emitEventArgs(L *lua.LState) (deviceID, entityID string, payload any, correlationID string) {
	payload = map[string]any{}
	if tbl, ok := L.Get(2).(*lua.LTable); ok {
		obj, _ := lValueToAny(tbl).(map[string]any)
		if obj == nil {
			return "", "", payload, ""
		}
		return getString(obj, "DeviceID"), getString(obj, "EntityID"), getMapAny(obj, "Payload"), getString(obj, "CorrelationID")
	}
	return strings.TrimSpace(L.CheckString(2)), strings.TrimSpace(L.CheckString(3)),
		lValueToAny(L.CheckAny(4)), strings.TrimSpace(L.OptString(5, ""))
}

type luaBatchCommand struct {
	PluginID string
	DeviceID string
	EntityID string
	Payload  any
}

func sendBatchCommandArgs(L *lua.LState, idx int) ([]luaBatchCommand, string) {
	tbl, ok := L.Get(idx).(*lua.LTable)
	if !ok {
		return nil, "batch commands must be a table"
	}
	items, ok := lValueToAny(tbl).([]any)
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
		pl := obj["Payload"]
		if pl == nil {
			pl = obj["payload"]
		}
		if pl == nil {
			pl = map[string]any{}
		}
		out = append(out, luaBatchCommand{PluginID: pluginID, DeviceID: deviceID, EntityID: entityID, Payload: pl})
	}
	return out, ""
}

// --- Package-level matchSelector (kept for test compatibility) ---

// matchSelector matches a wildcard pattern against an actual selector string.
// Tests call this directly.
func matchSelector(pattern, actual string) bool {
	// Use a package-level router instance for backward-compat with tests.
	return globalRouter.matchPattern(pattern, actual)
}

// globalRouter is a package-level subscriptionRouter used by matchSelector.
var globalRouter = newSubscriptionRouter()

// --- Helpers ---

func entityScopedSelector(pluginID, deviceID, entityID, suffix string) string {
	return fmt.Sprintf("%s.%s.%s.%s",
		strings.TrimSpace(pluginID), strings.TrimSpace(deviceID),
		strings.TrimSpace(entityID), strings.TrimSpace(suffix))
}

func mergeActionPayload(action string, payload any) map[string]any {
	out := map[string]any{"type": strings.TrimSpace(strings.ToLower(action))}
	switch v := payload.(type) {
	case nil:
		return out
	case map[string]any:
		for k, val := range v {
			out[k] = val
		}
	case []any:
		if out["type"] == "set_rgb" {
			out["rgb"] = v
		} else if out["type"] == "set_xy" {
			out["xy"] = v
		} else {
			out["value"] = v
		}
	default:
		out["value"] = v
	}
	return out
}

func isEmptyScriptQuery(q scriptQuery) bool {
	return q.Text == "" && q.Pattern == "" && q.PluginID == "" &&
		q.DeviceID == "" && q.EntityID == "" && q.Domain == "" && len(q.Labels) == 0
}

func foundEntityEqual(a, b foundEntity) bool {
	if a.PluginID != b.PluginID || a.DeviceID != b.DeviceID || a.EntityID != b.EntityID ||
		a.Domain != b.Domain || a.SourceID != b.SourceID || a.SourceName != b.SourceName || a.LocalName != b.LocalName {
		return false
	}
	if len(a.Labels) != len(b.Labels) {
		return false
	}
	for k, av := range a.Labels {
		bv, ok := b.Labels[k]
		if !ok || len(av) != len(bv) {
			return false
		}
		for i, s := range av {
			if s != bv[i] {
				return false
			}
		}
	}
	return true
}

func entityMemberKey(pluginID, deviceID, entityID string) string {
	return pluginID + "|" + deviceID + "|" + entityID
}

func dynamicQueryChangePayload(dq *dynamicQuery, changeType string, ent foundEntity, memberCount int) map[string]any {
	return map[string]any{
		"QueryID": dq.id, "Type": changeType, "Revision": dq.revision, "Count": memberCount,
		"Entity": map[string]any{
			"PluginID": ent.PluginID, "DeviceID": ent.DeviceID, "EntityID": ent.EntityID,
			"Domain": ent.Domain, "SourceID": ent.SourceID, "SourceName": ent.SourceName,
			"LocalName": ent.LocalName, "Labels": ent.Labels,
		},
	}
}

func applyFoundEntityLimit(items []foundEntity, limit int) []foundEntity {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
}

func defaultMembersQuery(ent foundEntity) (scriptQuery, bool) {
	if ent.Labels == nil {
		return scriptQuery{}, false
	}
	keys := []string{"virtual_source_query", "source_query", "SourceQuery", "members_query", "MembersQuery"}
	for _, key := range keys {
		values := ent.Labels[key]
		if len(values) == 0 {
			continue
		}
		raw := strings.TrimSpace(values[0])
		if raw == "" {
			continue
		}
		if q, ok := parseScriptQueryString(raw); ok {
			return q, true
		}
		return scriptQuery{Text: raw, Pattern: raw}, true
	}
	return scriptQuery{}, false
}

func filterFoundEntities(items []foundEntity, q scriptQuery) []foundEntity {
	if isEmptyScriptQuery(q) {
		return append([]foundEntity(nil), items...)
	}
	pattern := strings.TrimSpace(q.Pattern)
	if pattern == "" {
		pattern = strings.TrimSpace(q.Text)
	}
	if pattern == "" {
		pattern = "*"
	}
	textLower := strings.ToLower(pattern)
	isWildcard := strings.ContainsAny(pattern, "*?")

	out := make([]foundEntity, 0, len(items))
	for _, ent := range items {
		if q.PluginID != "" && ent.PluginID != q.PluginID {
			continue
		}
		if q.DeviceID != "" && ent.DeviceID != q.DeviceID {
			continue
		}
		if q.EntityID != "" && ent.EntityID != q.EntityID {
			continue
		}
		if q.Domain != "" && ent.Domain != q.Domain {
			continue
		}
		if !matchesStringSliceLabels(ent.Labels, q.Labels) {
			continue
		}
		if pattern == "*" {
			out = append(out, ent)
			continue
		}
		if isWildcard {
			ok, _ := filepath.Match(pattern, ent.EntityID)
			if ok {
				out = append(out, ent)
			}
			continue
		}
		if containsFoundEntityText(ent, textLower) {
			out = append(out, ent)
		}
	}
	return out
}

func containsFoundEntityText(ent foundEntity, textLower string) bool {
	if strings.Contains(strings.ToLower(ent.EntityID), textLower) ||
		strings.Contains(strings.ToLower(ent.SourceID), textLower) ||
		strings.Contains(strings.ToLower(ent.SourceName), textLower) ||
		strings.Contains(strings.ToLower(ent.LocalName), textLower) {
		return true
	}
	for _, vals := range ent.Labels {
		for _, v := range vals {
			if strings.Contains(strings.ToLower(v), textLower) {
				return true
			}
		}
	}
	return false
}

func matchesStringSliceLabels(have, want map[string][]string) bool {
	if len(want) == 0 {
		return true
	}
	if have == nil {
		return false
	}
	for k, wantVals := range want {
		haveVals := have[k]
		for _, wv := range wantVals {
			found := false
			for _, hv := range haveVals {
				if hv == wv {
					found = true
					break
				}
			}
			if !found {
				return false
			}
		}
	}
	return true
}

func applyFoundDeviceLimit(items []foundDevice, limit int) []foundDevice {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
}

func toAnySlice(items []string) []any {
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

func firstQueryValue(values url.Values, keys ...string) string {
	for _, key := range keys {
		if v := strings.TrimSpace(values.Get(key)); v != "" {
			return v
		}
	}
	return ""
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
	if x, ok := v.(map[string]any); ok {
		return x
	}
	b, _ := json.Marshal(v)
	var out map[string]any
	if err := json.Unmarshal(b, &out); err == nil && out != nil {
		return out
	}
	return map[string]any{}
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
				vals := make([]string, 0, len(vv))
				for _, item := range vv {
					if s, ok := item.(string); ok && strings.TrimSpace(s) != "" {
						vals = append(vals, strings.TrimSpace(s))
					}
				}
				if len(vals) > 0 {
					out[k] = vals
				}
			}
		}
		return out
	default:
		return nil
	}
}
