package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/slidebolt/sdk-types"
)

// Runner wires a Plugin implementation to the NATS bus and handles the full
// RPC lifecycle: registration, health, devices, entities, commands, and events.
type Runner struct {
	nc         *nats.Conn
	plugin     Plugin
	rpcSubject string
	dataDir    string
	manifest   types.Manifest

	statusMu sync.RWMutex
	statuses map[string]types.CommandStatus

	registryMu sync.RWMutex
	registry   map[string]types.Registration

	scriptsMu sync.Mutex
	scripts   map[scriptKey]*scriptRuntime
}

var idSeq uint64

func nextID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), atomic.AddUint64(&idSeq, 1))
}

func NewRunner(p Plugin) *Runner {
	return &Runner{
		plugin:   p,
		dataDir:  MustGetEnv(EnvPluginData),
		statuses: make(map[string]types.CommandStatus),
		registry: make(map[string]types.Registration),
		scripts:  make(map[scriptKey]*scriptRuntime),
	}
}

func (r *Runner) Run() error {
	url := MustGetEnv(EnvNATSURL)
	var err error
	for i := 0; i < 5; i++ {
		r.nc, err = nats.Connect(url)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return err
	}
	defer r.nc.Close()

	initialState := r.loadState("default")
	var updatedState types.Storage
	r.manifest, updatedState = r.plugin.OnInitialize(Config{DataDir: r.dataDir, EventSink: r}, initialState)
	r.rpcSubject = SubjectRPCPrefix + r.manifest.ID
	r.saveStateSynced("default", updatedState)
	r.plugin.OnReady()

	reg := types.Registration{Manifest: r.manifest, RPCSubject: r.rpcSubject}
	regData, _ := json.Marshal(reg)
	r.registryMu.Lock()
	r.registry[r.manifest.ID] = reg
	r.registryMu.Unlock()
	r.nc.Publish(SubjectRegistration, regData)

	r.nc.Subscribe(r.rpcSubject, r.handleRPC)
	r.nc.Subscribe(SubjectSearchPlugins, r.handlePluginSearch)
	r.nc.Subscribe(SubjectSearchDevices, r.handleDeviceSearch)
	r.nc.Subscribe(SubjectRegistration, r.handleRegistration)
	r.nc.Subscribe(SubjectEntityEvents, r.handleEntityEvent)
	r.nc.Subscribe(SubjectDiscoveryProbe, func(m *nats.Msg) {
		r.nc.Publish(SubjectRegistration, regData)
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	_ = r.nc.Drain()
	return nil
}

func (r *Runner) handleRPC(m *nats.Msg) {
	var req types.Request
	json.Unmarshal(m.Data, &req)
	var result any
	var rpcErr *types.RPCError

	switch req.Method {
	case HealthEndpoint:
		status, err := r.plugin.OnHealthCheck()
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = map[string]string{"status": status, "service": "plugin"}
		}

	case "initialize":
		result = r.manifest

	case "devices/create":
		var dev types.Device
		json.Unmarshal(req.Params, &dev)
		created, err := r.plugin.OnDeviceCreate(dev)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.saveDevice(created)
			result = created
		}

	case "devices/update":
		var dev types.Device
		json.Unmarshal(req.Params, &dev)
		updated, err := r.plugin.OnDeviceUpdate(dev)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.saveDevice(updated)
			result = updated
		}

	case "devices/delete":
		var id string
		json.Unmarshal(req.Params, &id)
		if err := r.plugin.OnDeviceDelete(id); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.deleteDevice(id)
			result = true
		}

	case "devices/list":
		current := r.loadDevices()
		list, err := r.plugin.OnDevicesList(current)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = list
		}

	case "entities/create":
		var ent types.Entity
		json.Unmarshal(req.Params, &ent)
		ent.Data.SyncStatus = "in_sync"
		ent.Data.UpdatedAt = time.Now().UTC()
		created, err := r.plugin.OnEntityCreate(ent)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.saveEntity(created)
			result = created
		}

	case "entities/list":
		var params struct {
			DeviceID string `json:"device_id"`
		}
		json.Unmarshal(req.Params, &params)
		current := r.loadEntities(params.DeviceID)
		list, err := r.plugin.OnEntitiesList(params.DeviceID, current)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = list
		}

	case "entities/commands/create":
		var params struct {
			DeviceID string          `json:"device_id"`
			EntityID string          `json:"entity_id"`
			Payload  json.RawMessage `json:"payload"`
		}
		json.Unmarshal(req.Params, &params)
		status, err := r.createCommand(params.DeviceID, params.EntityID, params.Payload)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = status
		}

	case "commands/status/get":
		var params struct {
			CommandID string `json:"command_id"`
		}
		json.Unmarshal(req.Params, &params)
		status, ok := r.getCommandStatus(params.CommandID)
		if !ok {
			rpcErr = &types.RPCError{Code: -32002, Message: "command not found"}
		} else {
			result = status
		}

	case "entities/events/ingest":
		var evt types.InboundEvent
		json.Unmarshal(req.Params, &evt)
		updated, err := r.processInboundEvent(evt)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = updated
		}

	case "scripts/get":
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
		}
		json.Unmarshal(req.Params, &params)
		source, path, err := r.getScriptSource(params.DeviceID, params.EntityID)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32004, Message: err.Error()}
		} else {
			result = map[string]any{
				"plugin_id": r.manifest.ID,
				"device_id": params.DeviceID,
				"entity_id": params.EntityID,
				"path":      path,
				"source":    source,
			}
		}

	case "scripts/put":
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
			Source   string `json:"source"`
		}
		json.Unmarshal(req.Params, &params)
		path, err := r.putScriptSource(params.DeviceID, params.EntityID, params.Source)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = map[string]any{
				"plugin_id": r.manifest.ID,
				"device_id": params.DeviceID,
				"entity_id": params.EntityID,
				"path":      path,
				"source":    params.Source,
			}
		}

	case "scripts/delete":
		var params struct {
			DeviceID   string `json:"device_id"`
			EntityID   string `json:"entity_id"`
			PurgeState bool   `json:"purge_state"`
		}
		json.Unmarshal(req.Params, &params)
		err := r.deleteScriptSource(params.DeviceID, params.EntityID, params.PurgeState)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = map[string]any{
				"plugin_id":   r.manifest.ID,
				"device_id":   params.DeviceID,
				"entity_id":   params.EntityID,
				"deleted":     true,
				"state_purge": params.PurgeState,
			}
		}

	case "scripts/state/get":
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
		}
		json.Unmarshal(req.Params, &params)
		state, path := r.getScriptState(params.DeviceID, params.EntityID)
		result = map[string]any{
			"plugin_id": r.manifest.ID,
			"device_id": params.DeviceID,
			"entity_id": params.EntityID,
			"path":      path,
			"state":     state,
		}

	case "scripts/state/put":
		var params struct {
			DeviceID string         `json:"device_id"`
			EntityID string         `json:"entity_id"`
			State    map[string]any `json:"state"`
		}
		json.Unmarshal(req.Params, &params)
		path, err := r.putScriptState(params.DeviceID, params.EntityID, params.State)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = map[string]any{
				"plugin_id": r.manifest.ID,
				"device_id": params.DeviceID,
				"entity_id": params.EntityID,
				"path":      path,
				"state":     params.State,
			}
		}

	case "scripts/state/delete":
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
		}
		json.Unmarshal(req.Params, &params)
		path, err := r.deleteScriptState(params.DeviceID, params.EntityID)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = map[string]any{
				"plugin_id": r.manifest.ID,
				"device_id": params.DeviceID,
				"entity_id": params.EntityID,
				"path":      path,
				"state":     map[string]any{},
			}
		}

	default:
		rpcErr = &types.RPCError{Code: -32601, Message: "method not found"}
	}

	r.sendResponse(m, req.ID, result, rpcErr)
}

func (r *Runner) createCommand(deviceID, entityID string, payload json.RawMessage) (types.CommandStatus, error) {
	ent := r.resolveEntity(deviceID, entityID)
	if ent.ID == "" {
		return types.CommandStatus{}, fmt.Errorf("entity not found")
	}

	now := time.Now().UTC()
	cmd := types.Command{
		ID:         nextID("cmd"),
		PluginID:   r.manifest.ID,
		DeviceID:   deviceID,
		EntityID:   entityID,
		EntityType: ent.Domain,
		Payload:    payload,
		CreatedAt:  now,
	}
	status := types.CommandStatus{
		CommandID:     cmd.ID,
		PluginID:      cmd.PluginID,
		DeviceID:      cmd.DeviceID,
		EntityID:      cmd.EntityID,
		EntityType:    cmd.EntityType,
		State:         types.CommandPending,
		CreatedAt:     now,
		LastUpdatedAt: now,
	}
	r.setCommandStatus(status)

	ent.Data.LastCommandID = cmd.ID
	ent.Data.SyncStatus = "pending"
	ent.Data.UpdatedAt = now
	updated, err := r.plugin.OnCommand(cmd, ent)
	if err != nil {
		status.State = types.CommandFailed
		status.Error = err.Error()
		status.LastUpdatedAt = time.Now().UTC()
		r.setCommandStatus(status)
		ent.Data.SyncStatus = "failed"
		ent.Data.UpdatedAt = status.LastUpdatedAt
		r.saveEntity(ent)
		return types.CommandStatus{}, err
	}

	updated.Data.LastCommandID = cmd.ID
	if updated.Data.SyncStatus == "" {
		updated.Data.SyncStatus = "pending"
	}
	updated.Data.UpdatedAt = time.Now().UTC()
	r.saveEntity(updated)
	go r.dispatchLuaCommand(cmd)
	return status, nil
}

// EmitEvent satisfies EventSink, letting plugin code publish provider-originated
// events back into the system after completing last-mile work.
func (r *Runner) EmitEvent(evt types.InboundEvent) error {
	_, err := r.processInboundEvent(evt)
	return err
}

func (r *Runner) processInboundEvent(evt types.InboundEvent) (types.Entity, error) {
	ent := r.resolveEntity(evt.DeviceID, evt.EntityID)
	if ent.ID == "" {
		return types.Entity{}, fmt.Errorf("entity not found")
	}

	event := types.Event{
		ID:            nextID("evt"),
		PluginID:      r.manifest.ID,
		DeviceID:      evt.DeviceID,
		EntityID:      evt.EntityID,
		EntityType:    ent.Domain,
		CorrelationID: evt.CorrelationID,
		Payload:       evt.Payload,
		CreatedAt:     time.Now().UTC(),
	}

	updated, err := r.plugin.OnEvent(event, ent)
	if err != nil {
		return types.Entity{}, err
	}
	updated.Data.LastEventID = event.ID
	if event.CorrelationID != "" {
		if status, ok := r.getCommandStatus(event.CorrelationID); ok {
			status.State = types.CommandSucceeded
			status.LastUpdatedAt = time.Now().UTC()
			r.setCommandStatus(status)
			updated.Data.LastCommandID = event.CorrelationID
		}
	}
	if updated.Data.SyncStatus == "" || updated.Data.SyncStatus == "pending" {
		updated.Data.SyncStatus = "in_sync"
	}
	updated.Data.UpdatedAt = time.Now().UTC()
	r.saveEntity(updated)

	envelope := types.EntityEventEnvelope{
		EventID:       event.ID,
		PluginID:      event.PluginID,
		DeviceID:      event.DeviceID,
		EntityID:      event.EntityID,
		EntityType:    event.EntityType,
		CorrelationID: event.CorrelationID,
		Payload:       event.Payload,
		CreatedAt:     event.CreatedAt,
	}
	if data, err := json.Marshal(envelope); err == nil {
		r.nc.Publish(SubjectEntityEvents, data)
	}
	go r.dispatchLuaEvent(envelope)

	return updated, nil
}

func (r *Runner) handleRegistration(m *nats.Msg) {
	var reg types.Registration
	if err := json.Unmarshal(m.Data, &reg); err != nil {
		return
	}
	if reg.Manifest.ID == "" || reg.RPCSubject == "" {
		return
	}
	r.registryMu.Lock()
	r.registry[reg.Manifest.ID] = reg
	r.registryMu.Unlock()
}

func (r *Runner) handleEntityEvent(m *nats.Msg) {
	var env types.EntityEventEnvelope
	if err := json.Unmarshal(m.Data, &env); err != nil {
		return
	}
	if env.PluginID == r.manifest.ID {
		return
	}
	r.dispatchLuaEvent(env)
}

func (r *Runner) getCommandStatus(id string) (types.CommandStatus, bool) {
	r.statusMu.RLock()
	defer r.statusMu.RUnlock()
	st, ok := r.statuses[id]
	return st, ok
}

func (r *Runner) setCommandStatus(status types.CommandStatus) {
	r.statusMu.Lock()
	defer r.statusMu.Unlock()
	r.statuses[status.CommandID] = status
}

func (r *Runner) handlePluginSearch(m *nats.Msg) {
	var q types.SearchQuery
	json.Unmarshal(m.Data, &q)
	if match, _ := filepath.Match(q.Pattern, r.manifest.Name); match {
		data, _ := json.Marshal(r.manifest)
		m.Respond(data)
	}
}

func (r *Runner) handleDeviceSearch(m *nats.Msg) {
	var q types.SearchQuery
	json.Unmarshal(m.Data, &q)
	var matches []types.Device
	for _, d := range r.loadDevices() {
		if ok, _ := filepath.Match(q.Pattern, d.ID); ok {
			matches = append(matches, d)
		}
	}
	if len(matches) > 0 {
		data, _ := json.Marshal(matches)
		m.Respond(data)
	}
}

func (r *Runner) snapshotRegistry() map[string]types.Registration {
	r.registryMu.RLock()
	defer r.registryMu.RUnlock()
	out := make(map[string]types.Registration, len(r.registry))
	for k, v := range r.registry {
		out[k] = v
	}
	return out
}

func (r *Runner) callRPC(pluginID, method string, params any, timeout time.Duration) (types.Response, error) {
	r.registryMu.RLock()
	reg, ok := r.registry[pluginID]
	r.registryMu.RUnlock()
	if !ok {
		return types.Response{}, fmt.Errorf("plugin not registered: %s", pluginID)
	}

	paramsBytes, _ := json.Marshal(params)
	id := json.RawMessage(`1`)
	req := types.Request{JSONRPC: types.JSONRPCVersion, ID: &id, Method: method, Params: paramsBytes}
	data, _ := json.Marshal(req)
	msg, err := r.nc.Request(reg.RPCSubject, data, timeout)
	if err != nil {
		return types.Response{}, err
	}
	var resp types.Response
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		return types.Response{}, err
	}
	if resp.Error != nil {
		return resp, fmt.Errorf(resp.Error.Message)
	}
	return resp, nil
}

func (r *Runner) callListDevices(pluginID string) ([]types.Device, error) {
	resp, err := r.callRPC(pluginID, "devices/list", nil, 1200*time.Millisecond)
	if err != nil {
		return nil, err
	}
	var out []types.Device
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Runner) callListEntities(pluginID, deviceID string) ([]types.Entity, error) {
	resp, err := r.callRPC(pluginID, "entities/list", map[string]any{"device_id": deviceID}, 1200*time.Millisecond)
	if err != nil {
		return nil, err
	}
	var out []types.Entity
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return nil, err
	}
	return out, nil
}

func (r *Runner) callCreateCommand(pluginID, deviceID, entityID string, payload json.RawMessage) (types.CommandStatus, error) {
	resp, err := r.callRPC(pluginID, "entities/commands/create", map[string]any{
		"device_id": deviceID,
		"entity_id": entityID,
		"payload":   payload,
	}, 1500*time.Millisecond)
	if err != nil {
		return types.CommandStatus{}, err
	}
	var out types.CommandStatus
	if err := json.Unmarshal(resp.Result, &out); err != nil {
		return types.CommandStatus{}, err
	}
	return out, nil
}

func (r *Runner) sendResponse(m *nats.Msg, id *json.RawMessage, result any, rpcErr *types.RPCError) {
	var resBytes json.RawMessage
	if result != nil {
		resBytes, _ = json.Marshal(result)
	}
	resp := types.Response{JSONRPC: types.JSONRPCVersion, Result: resBytes, Error: rpcErr}
	if id != nil {
		resp.ID = *id
	}
	data, _ := json.Marshal(resp)
	m.Respond(data)
}

// --- File persistence ---

func (r *Runner) saveDevice(dev types.Device) {
	dir := filepath.Join(r.dataDir, "devices")
	os.MkdirAll(dir, 0o755)
	type DiskDevice types.Device
	data, _ := json.MarshalIndent(DiskDevice(dev), "", "  ")
	os.WriteFile(filepath.Join(dir, dev.ID+".json"), data, 0o644)
}

func (r *Runner) deleteDevice(id string) {
	os.Remove(filepath.Join(r.dataDir, "devices", id+".json"))
}

func (r *Runner) loadDevices() []types.Device {
	files, _ := filepath.Glob(filepath.Join(r.dataDir, "devices", "*.json"))
	items := make([]types.Device, 0, len(files))
	for _, f := range files {
		data, _ := os.ReadFile(f)
		var dev types.Device
		json.Unmarshal(data, &dev)
		items = append(items, dev)
	}
	return items
}

func (r *Runner) saveEntity(ent types.Entity) {
	dir := filepath.Join(r.dataDir, "devices", ent.DeviceID, "entities")
	os.MkdirAll(dir, 0o755)
	data, _ := json.MarshalIndent(ent, "", "  ")
	os.WriteFile(filepath.Join(dir, ent.ID+".json"), data, 0o644)
}

func (r *Runner) loadEntity(deviceID, entityID string) types.Entity {
	data, _ := os.ReadFile(filepath.Join(r.dataDir, "devices", deviceID, "entities", entityID+".json"))
	var ent types.Entity
	json.Unmarshal(data, &ent)
	return ent
}

func (r *Runner) resolveEntity(deviceID, entityID string) types.Entity {
	if ent := r.loadEntity(deviceID, entityID); ent.ID != "" {
		return ent
	}
	list, err := r.plugin.OnEntitiesList(deviceID, r.loadEntities(deviceID))
	if err != nil {
		return types.Entity{}
	}
	for _, e := range list {
		if e.ID == entityID {
			if e.DeviceID == "" {
				e.DeviceID = deviceID
			}
			r.saveEntity(e)
			return e
		}
	}
	return types.Entity{}
}

func (r *Runner) loadEntities(deviceID string) []types.Entity {
	files, _ := filepath.Glob(filepath.Join(r.dataDir, "devices", deviceID, "entities", "*.json"))
	items := make([]types.Entity, 0, len(files))
	for _, f := range files {
		data, _ := os.ReadFile(f)
		var ent types.Entity
		json.Unmarshal(data, &ent)
		items = append(items, ent)
	}
	return items
}

func (r *Runner) loadState(id string) types.Storage {
	data, _ := os.ReadFile(filepath.Join(r.dataDir, id+".json"))
	var store types.Storage
	json.Unmarshal(data, &store)
	return store
}

func (r *Runner) saveStateSynced(id string, store types.Storage) {
	data, _ := json.MarshalIndent(store, "", "  ")
	f, _ := os.Create(filepath.Join(r.dataDir, id+".json"))
	defer f.Close()
	f.Write(data)
	f.Sync()
}
