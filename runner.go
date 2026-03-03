package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
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

	lockMu      sync.Mutex
	deviceLocks map[string]*sync.Mutex

	hashMu   sync.RWMutex
	fileHash map[string]string
}

var idSeq uint64

func nextID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), atomic.AddUint64(&idSeq, 1))
}

func NewRunner(p Plugin) (*Runner, error) {
	dataDir, err := RequireEnv(EnvPluginData)
	if err != nil {
		return nil, err
	}
	return &Runner{
		plugin:      p,
		dataDir:     dataDir,
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		scripts:     make(map[scriptKey]*scriptRuntime),
		deviceLocks: make(map[string]*sync.Mutex),
		fileHash:    make(map[string]string),
	}, nil
}

func (r *Runner) Run() error {
	discover := flag.Bool("discover", false, "Run discovery cycle and exit")
	flag.Parse()

	if *discover {
		return r.runDiscoveryMode()
	}

	url, err := RequireEnv(EnvNATSURL)
	if err != nil {
		return err
	}
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
	r.manifest, updatedState = r.plugin.OnInitialize(Config{DataDir: r.dataDir, EventSink: r, RawStore: r}, initialState)
	r.rpcSubject = SubjectRPCPrefix + r.manifest.ID
	r.saveStateSynced("default", updatedState)
	r.plugin.OnReady()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := r.plugin.WaitReady(waitCtx); err != nil {
		log.Printf("plugin-runner: WaitReady failed: %v; proceeding with registration anyway", err)
	}
	waitCancel()

	// Proactively initialize devices before accepting RPC traffic so that
	// adapters and in-memory state are ready before the first command arrives.
	if list, err := r.plugin.OnDevicesList(r.loadDevices()); err != nil {
		log.Printf("plugin-runner: proactive OnDevicesList failed: %v", err)
	} else {
		for _, dev := range list {
			if strings.TrimSpace(dev.ID) != "" {
				r.saveDevice(dev)
			}
		}
	}

	reg := types.Registration{Manifest: r.manifest, RPCSubject: r.rpcSubject}
	regData, err := json.Marshal(reg)
	if err != nil {
		return fmt.Errorf("failed to marshal registration: %w", err)
	}
	r.registryMu.Lock()
	r.registry[r.manifest.ID] = reg
	r.registryMu.Unlock()
	if err := r.nc.Publish(SubjectRegistration, regData); err != nil {
		log.Printf("plugin-runner: failed to publish registration: %v", err)
	}

	r.nc.Subscribe(r.rpcSubject, r.handleRPC)
	r.nc.Subscribe(SubjectSearchPlugins, r.handlePluginSearch)
	r.nc.Subscribe(SubjectSearchDevices, r.handleDeviceSearch)
	r.nc.Subscribe(SubjectSearchEntities, r.handleEntitySearch)
	r.nc.Subscribe(SubjectRegistration, r.handleRegistration)
	r.nc.Subscribe(SubjectEntityEvents, r.handleEntityEvent)
	r.nc.Subscribe(SubjectDiscoveryProbe, func(m *nats.Msg) {
		r.nc.Publish(SubjectRegistration, regData)
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
	r.plugin.OnShutdown()
	_ = r.nc.Drain()
	return nil
}

func (r *Runner) runDiscoveryMode() error {
	_ = os.MkdirAll(r.dataDir, 0o755)
	initialState := r.loadState("default")
	r.manifest, _ = r.plugin.OnInitialize(Config{DataDir: r.dataDir, EventSink: r, RawStore: r}, initialState)
	r.plugin.OnReady()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := r.plugin.WaitReady(waitCtx); err != nil {
		log.Printf("plugin-runner: WaitReady failed in discovery mode: %v; proceeding anyway", err)
	}
	waitCancel()

	devices, err := r.plugin.OnDevicesList(nil)
	if err != nil {
		return fmt.Errorf("discovery failed: %w", err)
	}

	output, err := json.MarshalIndent(devices, "", "  ")
	if err != nil {
		return fmt.Errorf("failed to marshal discovery results: %w", err)
	}

	fmt.Println(string(output))
	return nil
}

func (r *Runner) handleRPC(m *nats.Msg) {
	var req types.Request
	if err := json.Unmarshal(m.Data, &req); err != nil {
		r.sendResponse(m, nil, nil, &types.RPCError{Code: -32700, Message: "parse error: " + err.Error()})
		return
	}
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

	case "storage/update":
		current := r.loadState("default")
		updated, err := r.plugin.OnStorageUpdate(current)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.saveStateSynced("default", updated)
			result = updated
		}

	case "devices/create":
		var dev types.Device
		if err := json.Unmarshal(req.Params, &dev); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		created, err := r.plugin.OnDeviceCreate(dev)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.saveDevice(created)
			result = created
		}

	case "devices/update":
		var incoming types.Device
		if err := json.Unmarshal(req.Params, &incoming); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		// Merge with persisted data so source fields (source_id, source_name)
		// and the user field (local_name) never overwrite each other.
		dev := r.loadDeviceByID(incoming.ID)
		if dev.ID == "" {
			dev = incoming
		} else {
			if incoming.SourceID != "" {
				dev.SourceID = incoming.SourceID
			}
			if incoming.SourceName != "" {
				dev.SourceName = incoming.SourceName
			}
			if incoming.LocalName != "" {
				dev.LocalName = incoming.LocalName
			}
			if len(incoming.Labels) > 0 {
				dev.Labels = incoming.Labels
			}
		}
		updated, err := r.plugin.OnDeviceUpdate(dev)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.saveDevice(updated)
			result = updated
		}

	case "devices/delete":
		var id string
		if err := json.Unmarshal(req.Params, &id); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
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
			// Persist discovered devices so first-time discovery is durable.
			for _, dev := range list {
				if strings.TrimSpace(dev.ID) == "" {
					continue
				}
				r.saveDevice(dev)
			}
			result = list
		}

	case "entities/create":
		var ent types.Entity
		if err := json.Unmarshal(req.Params, &ent); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		ent.Data.SyncStatus = "in_sync"
		ent.Data.UpdatedAt = time.Now().UTC()
		created, err := r.plugin.OnEntityCreate(ent)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.saveEntity(created)
			result = created
		}

	case "entities/update":
		var incoming types.Entity
		if err := json.Unmarshal(req.Params, &incoming); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		// Merge with persisted data so source fields (domain, actions) and the
		// user field (local_name) never overwrite each other.
		ent := r.loadEntity(incoming.DeviceID, incoming.ID)
		if ent.ID == "" {
			ent = incoming
		} else {
			if incoming.LocalName != "" {
				ent.LocalName = incoming.LocalName
			}
			if incoming.Domain != "" {
				ent.Domain = incoming.Domain
			}
			if len(incoming.Actions) > 0 {
				ent.Actions = incoming.Actions
			}
			if len(incoming.Labels) > 0 {
				ent.Labels = incoming.Labels
			}
		}
		updated, err := r.plugin.OnEntityUpdate(ent)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.saveEntity(updated)
			result = updated
		}

	case "entities/delete":
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		if err := r.plugin.OnEntityDelete(params.DeviceID, params.EntityID); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			r.deleteEntity(params.DeviceID, params.EntityID)
			result = true
		}

	case "entities/list":
		var params struct {
			DeviceID string `json:"device_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		current := r.loadEntities(params.DeviceID)
		list, err := r.plugin.OnEntitiesList(params.DeviceID, current)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			// Persist discovered entities so entity state survives restarts.
			for _, ent := range list {
				if strings.TrimSpace(ent.ID) == "" {
					continue
				}
				if strings.TrimSpace(ent.DeviceID) == "" {
					ent.DeviceID = params.DeviceID
				}
				if strings.TrimSpace(ent.DeviceID) == "" {
					continue
				}
				r.saveEntity(ent)
			}
			result = list
		}

	case "entities/commands/create":
		var params struct {
			DeviceID string          `json:"device_id"`
			EntityID string          `json:"entity_id"`
			Payload  json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
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
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		status, ok := r.getCommandStatus(params.CommandID)
		if !ok {
			rpcErr = &types.RPCError{Code: -32002, Message: "command not found"}
		} else {
			result = status
		}

	case "entities/events/ingest":
		var evt types.InboundEventTyped[types.GenericPayload]
		if err := json.Unmarshal(req.Params, &evt); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
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
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
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
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
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
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
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
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
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
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
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
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
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
	typedPayload := types.GenericPayload{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &typedPayload); err != nil {
			status.State = types.CommandFailed
			status.Error = fmt.Sprintf("invalid command payload: %v", err)
			status.LastUpdatedAt = time.Now().UTC()
			r.setCommandStatus(status)
			return types.CommandStatus{}, err
		}
	}
	updated, err := r.plugin.OnCommandTyped(types.CommandRequest[types.GenericPayload]{
		CommandID: cmd.ID,
		PluginID:  cmd.PluginID,
		Device:    r.loadDeviceByID(deviceID),
		Entity:    ent,
		Payload:   typedPayload,
	}, ent)
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

// EmitTypedEvent satisfies EventSink, letting plugin code publish provider-originated
// events back into the system after completing last-mile work.
func (r *Runner) EmitTypedEvent(evt types.InboundEventTyped[types.GenericPayload]) error {
	if r.nc == nil {
		return nil // No-op in discovery mode
	}
	_, err := r.processInboundEvent(evt)
	return err
}

func (r *Runner) processInboundEvent(evt types.InboundEventTyped[types.GenericPayload]) (types.Entity, error) {
	ent := r.resolveEntity(evt.DeviceID, evt.EntityID)
	if ent.ID == "" {
		return types.Entity{}, fmt.Errorf("entity not found")
	}
	event := types.EventTyped[types.GenericPayload]{
		ID:            nextID("evt"),
		PluginID:      r.manifest.ID,
		DeviceID:      evt.DeviceID,
		EntityID:      evt.EntityID,
		EntityType:    ent.Domain,
		CorrelationID: evt.CorrelationID,
		Payload:       evt.Payload,
		CreatedAt:     time.Now().UTC(),
	}
	updated, err := r.plugin.OnEventTyped(event, ent)
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

	envelopePayload, err := json.Marshal(event.Payload)
	if err != nil {
		return types.Entity{}, fmt.Errorf("failed to marshal event payload: %w", err)
	}
	envelope := types.EntityEventEnvelope{
		EventID:       event.ID,
		PluginID:      event.PluginID,
		DeviceID:      event.DeviceID,
		EntityID:      event.EntityID,
		EntityType:    event.EntityType,
		CorrelationID: event.CorrelationID,
		Payload:       envelopePayload,
		CreatedAt:     event.CreatedAt,
	}
	if data, err := json.Marshal(envelope); err != nil {
		log.Printf("plugin-runner: failed to marshal event envelope: %v", err)
	} else if err := r.nc.Publish(SubjectEntityEvents, data); err != nil {
		log.Printf("plugin-runner: failed to publish entity event: %v", err)
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
	if err := json.Unmarshal(m.Data, &q); err != nil {
		return
	}
	if match, _ := filepath.Match(q.Pattern, r.manifest.Name); match {
		data, err := json.Marshal(r.manifest)
		if err != nil {
			log.Printf("plugin-runner: failed to marshal plugin search result: %v", err)
			return
		}
		if err := m.Respond(data); err != nil {
			log.Printf("plugin-runner: failed to respond to plugin search: %v", err)
		}
	}
}

func (r *Runner) handleDeviceSearch(m *nats.Msg) {
	var q types.SearchQuery
	if err := json.Unmarshal(m.Data, &q); err != nil {
		return
	}
	var matches []types.Device
	for _, d := range r.loadDevices() {
		if ok, _ := filepath.Match(q.Pattern, d.ID); !ok {
			continue
		}
		if !matchesLabels(d.Labels, q.Labels) {
			continue
		}
		matches = append(matches, d)
	}
	if len(matches) > 0 {
		data, err := json.Marshal(matches)
		if err != nil {
			log.Printf("plugin-runner: failed to marshal device search results: %v", err)
			return
		}
		if err := m.Respond(data); err != nil {
			log.Printf("plugin-runner: failed to respond to device search: %v", err)
		}
	}
}

func (r *Runner) handleEntitySearch(m *nats.Msg) {
	var q types.SearchQuery
	if err := json.Unmarshal(m.Data, &q); err != nil {
		return
	}
	var matches []types.Entity
	for _, d := range r.loadDevices() {
		for _, e := range r.loadEntities(d.ID) {
			if q.Pattern != "" {
				if ok, _ := filepath.Match(q.Pattern, e.ID); !ok {
					continue
				}
			}
			if !matchesLabels(e.Labels, q.Labels) {
				continue
			}
			matches = append(matches, e)
		}
	}
	if len(matches) > 0 {
		data, err := json.Marshal(matches)
		if err != nil {
			log.Printf("plugin-runner: failed to marshal entity search results: %v", err)
			return
		}
		if err := m.Respond(data); err != nil {
			log.Printf("plugin-runner: failed to respond to entity search: %v", err)
		}
	}
}

func matchesLabels(have, want map[string][]string) bool {
	if len(want) == 0 {
		return true
	}
	if have == nil {
		return false
	}
	for k, wantVals := range want {
		haveVals := have[k]
		matched := false
		for _, w := range wantVals {
			for _, h := range haveVals {
				if h == w {
					matched = true
					break
				}
			}
			if matched {
				break
			}
		}
		if !matched {
			return false
		}
	}
	return true
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
		return resp, fmt.Errorf("%s", resp.Error.Message)
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
		var err error
		resBytes, err = json.Marshal(result)
		if err != nil {
			log.Printf("plugin-runner: failed to marshal RPC result: %v", err)
			rpcErr = &types.RPCError{Code: -32603, Message: "internal error: failed to marshal result"}
			resBytes = nil
		}
	}
	resp := types.Response{JSONRPC: types.JSONRPCVersion, Result: resBytes, Error: rpcErr}
	if id != nil {
		resp.ID = *id
	}
	data, err := json.Marshal(resp)
	if err != nil {
		log.Printf("plugin-runner: failed to marshal RPC response: %v", err)
		return
	}
	if err := m.Respond(data); err != nil {
		log.Printf("plugin-runner: failed to send RPC response: %v", err)
	}
}

// --- File persistence ---

func (r *Runner) saveDevice(dev types.Device) {
	dir := filepath.Join(r.dataDir, "devices")
	os.MkdirAll(dir, 0o755)
	type DiskDevice types.Device
	data, _ := json.MarshalIndent(DiskDevice(dev), "", "  ")
	_ = r.writeIfChanged(filepath.Join(dir, dev.ID+".json"), data)
}

func (r *Runner) deleteDevice(id string) {
	path := filepath.Join(r.dataDir, "devices", id+".json")
	_ = os.Remove(path)
	r.clearHash(path)
	// Remove the entity directory for this device so no orphan files remain.
	_ = os.RemoveAll(filepath.Join(r.dataDir, "devices", id))
}

func (r *Runner) deleteEntity(deviceID, entityID string) {
	r.withDeviceLock(deviceID, func() {
		path := filepath.Join(r.dataDir, "devices", deviceID, "entities", entityID+".json")
		_ = os.Remove(path)
		r.clearHash(path)
	})
}

func (r *Runner) loadDevices() []types.Device {
	files, _ := filepath.Glob(filepath.Join(r.dataDir, "devices", "*.json"))
	items := make([]types.Device, 0, len(files))
	for _, f := range files {
		data, _ := os.ReadFile(f)
		r.seedHash(f, data)
		var dev types.Device
		json.Unmarshal(data, &dev)
		items = append(items, dev)
	}
	return items
}

func (r *Runner) loadDeviceByID(id string) types.Device {
	path := filepath.Join(r.dataDir, "devices", id+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return types.Device{}
	}
	r.seedHash(path, data)
	var dev types.Device
	json.Unmarshal(data, &dev)
	return dev
}

func (r *Runner) saveEntity(ent types.Entity) {
	// Ensure a device file exists before writing the entity so that
	// loadDevices() can always discover the owning device.
	deviceFile := filepath.Join(r.dataDir, "devices", ent.DeviceID+".json")
	if _, err := os.Stat(deviceFile); os.IsNotExist(err) {
		r.saveDevice(types.Device{ID: ent.DeviceID})
	}
	r.withDeviceLock(ent.DeviceID, func() {
		dir := filepath.Join(r.dataDir, "devices", ent.DeviceID, "entities")
		_ = os.MkdirAll(dir, 0o755)
		data, _ := json.MarshalIndent(ent, "", "  ")
		_ = r.writeIfChanged(filepath.Join(dir, ent.ID+".json"), data)
	})
}

func (r *Runner) loadEntity(deviceID, entityID string) types.Entity {
	var data []byte
	r.withDeviceLock(deviceID, func() {
		data, _ = os.ReadFile(filepath.Join(r.dataDir, "devices", deviceID, "entities", entityID+".json"))
	})
	r.seedHash(filepath.Join(r.dataDir, "devices", deviceID, "entities", entityID+".json"), data)
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
	items := make([]types.Entity, 0)
	r.withDeviceLock(deviceID, func() {
		files, _ := filepath.Glob(filepath.Join(r.dataDir, "devices", deviceID, "entities", "*.json"))
		items = make([]types.Entity, 0, len(files))
		for _, f := range files {
			data, _ := os.ReadFile(f)
			r.seedHash(f, data)
			var ent types.Entity
			json.Unmarshal(data, &ent)
			items = append(items, ent)
		}
	})
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
	_ = r.writeIfChanged(filepath.Join(r.dataDir, id+".json"), data)
}

func (r *Runner) deviceLock(deviceID string) *sync.Mutex {
	key := strings.TrimSpace(deviceID)
	if key == "" {
		key = "_global"
	}
	r.lockMu.Lock()
	defer r.lockMu.Unlock()
	if l, ok := r.deviceLocks[key]; ok {
		return l
	}
	l := &sync.Mutex{}
	r.deviceLocks[key] = l
	return l
}

func (r *Runner) withDeviceLock(deviceID string, fn func()) {
	l := r.deviceLock(deviceID)
	l.Lock()
	defer l.Unlock()
	fn()
}

func hashBytes(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func (r *Runner) getHash(path string) (string, bool) {
	r.hashMu.RLock()
	defer r.hashMu.RUnlock()
	h, ok := r.fileHash[path]
	return h, ok
}

func (r *Runner) setHash(path, hash string) {
	r.hashMu.Lock()
	r.fileHash[path] = hash
	r.hashMu.Unlock()
}

func (r *Runner) clearHash(path string) {
	r.hashMu.Lock()
	delete(r.fileHash, path)
	r.hashMu.Unlock()
}

func (r *Runner) seedHash(path string, data []byte) {
	if len(data) == 0 {
		return
	}
	if _, ok := r.getHash(path); ok {
		return
	}
	r.setHash(path, hashBytes(data))
}

func (r *Runner) writeIfChanged(path string, data []byte) error {
	newHash := hashBytes(data)
	if oldHash, ok := r.getHash(path); ok && oldHash == newHash {
		return nil
	}

	if oldHash, ok := r.getHash(path); !ok || oldHash != newHash {
		if existing, err := os.ReadFile(path); err == nil {
			existingHash := hashBytes(existing)
			r.setHash(path, existingHash)
			if existingHash == newHash {
				return nil
			}
		}
	}

	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}

	tmp, err := os.CreateTemp(dir, ".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		_ = os.Remove(tmpName)
		return err
	}
	r.setHash(path, newHash)
	return nil
}

// --- RawStore implementation ---

// ReadRawDevice reads protocol-specific raw data for a device.
func (r *Runner) ReadRawDevice(deviceID string) (json.RawMessage, error) {
	path := filepath.Join(r.dataDir, "raw", "devices", deviceID+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// WriteRawDevice writes protocol-specific raw data for a device.
func (r *Runner) WriteRawDevice(deviceID string, data json.RawMessage) error {
	path := filepath.Join(r.dataDir, "raw", "devices", deviceID+".json")
	return r.writeIfChanged(path, data)
}

// ReadRawEntity reads protocol-specific raw data for an entity.
func (r *Runner) ReadRawEntity(deviceID, entityID string) (json.RawMessage, error) {
	path := filepath.Join(r.dataDir, "raw", "devices", deviceID, "entities", entityID+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

// WriteRawEntity writes protocol-specific raw data for an entity.
func (r *Runner) WriteRawEntity(deviceID, entityID string, data json.RawMessage) error {
	path := filepath.Join(r.dataDir, "raw", "devices", deviceID, "entities", entityID+".json")
	return r.writeIfChanged(path, data)
}
