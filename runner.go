package runner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
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

	logger   *slog.Logger
	logLevel *slog.LevelVar

	stateStore       stateStore
	snapshotStore    stateStore
	snapshotInterval time.Duration
	snapshotStop     chan struct{}
	snapshotDone     chan struct{}
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
	logger, levelVar := newLogger(defaultLogServiceName())
	r := &Runner{
		plugin:      p,
		dataDir:     dataDir,
		statuses:    make(map[string]types.CommandStatus),
		registry:    make(map[string]types.Registration),
		scripts:     make(map[scriptKey]*scriptRuntime),
		deviceLocks: make(map[string]*sync.Mutex),
		fileHash:    make(map[string]string),
		logger:      logger,
		logLevel:    levelVar,
	}
	r.stateStore = newMemoryStateStore()
	r.snapshotStore = newStateStore(r, "file")
	r.snapshotInterval = loadSnapshotInterval()
	r.hydrateCanonicalFromSnapshot()
	return r, nil
}

func (r *Runner) GetLogLevel() string {
	if r == nil || r.logLevel == nil {
		return "info"
	}
	return formatLogLevel(r.logLevel.Level())
}

func (r *Runner) SetLogLevel(level string) (string, error) {
	if r == nil {
		return "", fmt.Errorf("runner is nil")
	}
	parsed, err := validateLogLevel(level)
	if err != nil {
		return "", err
	}
	if r.logLevel == nil {
		r.logLevel = &slog.LevelVar{}
	}
	r.logLevel.Set(parsed)
	if r.logger != nil {
		r.logger.Info("log level updated", "level", formatLogLevel(parsed))
	}
	return formatLogLevel(parsed), nil
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
	r.manifest, updatedState = r.plugin.OnInitialize(Config{DataDir: r.dataDir, EventSink: r, RawStore: r, Logger: r.logger}, initialState)
	r.rpcSubject = SubjectRPCPrefix + r.manifest.ID
	r.saveStateSynced("default", updatedState)
	r.plugin.OnReady()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := r.plugin.WaitReady(waitCtx); err != nil {
		log.Printf("plugin-runner: WaitReady failed: %v; proceeding with registration anyway", err)
	}
	waitCancel()

	// Enforce deterministic topology hydration before accepting RPC traffic:
	// devices first, then entities for every known device.
	if err := r.bootstrapStartupTopology(); err != nil {
		return fmt.Errorf("startup topology hydration failed: %w", err)
	}
	r.startSnapshotLoop()
	defer func() {
		r.stopSnapshotLoop()
		r.flushSnapshot()
	}()

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
	r.manifest, _ = r.plugin.OnInitialize(Config{DataDir: r.dataDir, EventSink: r, RawStore: r, Logger: r.logger}, initialState)
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

	case "logging/get_level":
		result = map[string]any{
			"level": r.GetLogLevel(),
		}

	case "logging/set_level":
		var payload struct {
			Level string `json:"level"`
		}
		if len(req.Params) > 0 && string(req.Params) != "null" {
			if err := json.Unmarshal(req.Params, &payload); err != nil || strings.TrimSpace(payload.Level) == "" {
				var levelText string
				if err2 := json.Unmarshal(req.Params, &levelText); err2 == nil {
					payload.Level = levelText
				} else if err != nil {
					rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
					break
				}
			}
		}
		level, err := r.SetLogLevel(payload.Level)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: err.Error()}
			break
		}
		result = map[string]any{
			"level": level,
		}

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
		created := r.createDeviceWithFallback(dev)
		r.saveDevice(created)
		result = created

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
		discovered, err := r.plugin.OnDevicesList(current)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			merged := ReconcileDevicesAdditive(current, discovered)
			for _, dev := range merged {
				if strings.TrimSpace(dev.ID) == "" {
					continue
				}
				r.saveDevice(dev)
			}
			result = merged
		}

	case "devices/refresh":
		refreshed, err := r.refreshDevices()
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = refreshed
		}

	case "entities/create":
		var ent types.Entity
		if err := json.Unmarshal(req.Params, &ent); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		created := r.createEntityWithFallback(ent)
		r.saveEntity(created)
		result = created

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
		discovered, err := r.plugin.OnEntitiesList(params.DeviceID, current)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			merged := ReconcileEntitiesAdditive(current, discovered, params.DeviceID)
			for _, ent := range merged {
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
			result = merged
		}

	case "entities/refresh":
		var params struct {
			DeviceID string `json:"device_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		refreshed, err := r.refreshEntities(params.DeviceID)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = refreshed
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
		var evt types.InboundEvent
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
	cmdStart := time.Now()
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "create_command start",
			"device_id", deviceID,
			"entity_id", entityID,
			"start_unix_ms", cmdStart.UnixMilli())
	}
	resolveStart := time.Now()
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "create_command resolve_entity start",
			"device_id", deviceID,
			"entity_id", entityID,
			"start_unix_ms", resolveStart.UnixMilli())
	}
	ent := r.resolveEntity(deviceID, entityID)
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "create_command resolve_entity done",
			"device_id", deviceID,
			"entity_id", entityID,
			"elapsed_ms", time.Since(resolveStart).Milliseconds())
	}
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
	ent.Data.SyncStatus = types.SyncStatusPending
	ent.Data.UpdatedAt = now

	onCommandStart := time.Now()
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "create_command on_command start",
			"command_id", cmd.ID,
			"device_id", deviceID,
			"entity_id", entityID,
			"start_unix_ms", onCommandStart.UnixMilli())
	}
	updated, err := r.plugin.OnCommand(cmd, ent)
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "create_command on_command done",
			"command_id", cmd.ID,
			"device_id", deviceID,
			"entity_id", entityID,
			"elapsed_ms", time.Since(onCommandStart).Milliseconds(),
			"ok", err == nil)
	}
	if err != nil {
		status.State = types.CommandFailed
		status.Error = err.Error()
		status.LastUpdatedAt = time.Now().UTC()
		r.setCommandStatus(status)
		ent.Data.SyncStatus = types.SyncStatusFailed
		ent.Data.UpdatedAt = status.LastUpdatedAt
		entCopy := ent
		go func() {
			persistStart := time.Now()
			r.saveEntity(entCopy)
			if r.logger != nil {
				r.logger.Log(context.Background(), LevelTrace, "create_command persist failed_entity",
					"command_id", cmd.ID,
					"device_id", deviceID,
					"entity_id", entityID,
					"persist_elapsed_ms", time.Since(persistStart).Milliseconds(),
					"total_elapsed_ms", time.Since(cmdStart).Milliseconds())
			}
		}()
		return types.CommandStatus{}, err
	}

	updated.Data.LastCommandID = cmd.ID
	if updated.Data.SyncStatus == types.SyncStatusEmpty {
		updated.Data.SyncStatus = types.SyncStatusPending
	}
	updated.Data.UpdatedAt = time.Now().UTC()
	updatedCopy := updated
	go func() {
		persistStart := time.Now()
		r.saveEntity(updatedCopy)
		if r.logger != nil {
			r.logger.Log(context.Background(), LevelTrace, "create_command persist ok",
				"command_id", cmd.ID,
				"device_id", deviceID,
				"entity_id", entityID,
				"persist_elapsed_ms", time.Since(persistStart).Milliseconds(),
				"total_elapsed_ms", time.Since(cmdStart).Milliseconds())
		}
	}()
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
	if _, err := requiredEventType(evt.Payload); err != nil {
		return types.Entity{}, err
	}

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
	if updated.Data.SyncStatus == types.SyncStatusEmpty || updated.Data.SyncStatus == types.SyncStatusPending {
		updated.Data.SyncStatus = types.SyncStatusSynced
	}
	updated.Data.SyncStatus = types.NormalizeSyncStatus(updated.Data.SyncStatus)
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
	} else if r.nc != nil {
		if err := r.nc.Publish(SubjectEntityEvents, data); err != nil {
			log.Printf("plugin-runner: failed to publish entity event: %v", err)
		}
	}
	go r.dispatchLuaEvent(envelope)

	return updated, nil
}

func requiredEventType(payload json.RawMessage) (string, error) {
	var probe struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return "", fmt.Errorf("event payload must be valid JSON object with required field \"type\": %w", err)
	}
	t := strings.TrimSpace(probe.Type)
	if t == "" {
		return "", fmt.Errorf("event payload missing required field \"type\"")
	}
	return t, nil
}

func (r *Runner) handleRegistration(m *nats.Msg) {
	var reg types.Registration
	if err := json.Unmarshal(m.Data, &reg); err != nil {
		return
	}
	if reg.Manifest.ID == "" || reg.RPCSubject == "" {
		return
	}

	for _, schema := range reg.Manifest.Schemas {
		types.RegisterDomain(schema)
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
	r.statuses[status.CommandID] = status
	r.statusMu.Unlock()
	if r.nc != nil {
		if data, err := json.Marshal(status); err == nil {
			r.nc.Publish(SubjectCommandStatus, data)
		}
	}
}

func (r *Runner) handlePluginSearch(m *nats.Msg) {
	var q types.SearchQuery
	if err := json.Unmarshal(m.Data, &q); err != nil {
		return
	}
	var matches []types.Manifest
	qLower := strings.ToLower(q.Pattern)

	if qLower != "" && qLower != "*" {
		matched := false
		if strings.Contains(strings.ToLower(r.manifest.ID), qLower) ||
			strings.Contains(strings.ToLower(r.manifest.Name), qLower) {
			matched = true
		}
		if matched {
			matches = append(matches, r.manifest)
		}
	} else {
		if match, _ := filepath.Match(q.Pattern, r.manifest.Name); match {
			matches = append(matches, r.manifest)
		}
	}

	if matches == nil {
		matches = []types.Manifest{}
	}
	res := types.SearchPluginsResponse{
		PluginID: r.manifest.ID,
		Matches:  matches,
	}
	data, err := json.Marshal(res)
	if err != nil {
		log.Printf("plugin-runner: failed to marshal plugin search result: %v", err)
		return
	}
	if err := m.Respond(data); err != nil {
		log.Printf("plugin-runner: failed to respond to plugin search: %v", err)
	}
}

func (r *Runner) handleDeviceSearch(m *nats.Msg) {
	var q types.SearchQuery
	if err := json.Unmarshal(m.Data, &q); err != nil {
		return
	}
	matches := r.searchDevicesLocal(q)
	if matches == nil {
		matches = []types.Device{}
	}
	res := types.SearchDevicesResponse{
		PluginID: r.manifest.ID,
		Matches:  matches,
	}
	data, err := json.Marshal(res)
	if err != nil {
		log.Printf("plugin-runner: failed to marshal device search results: %v", err)
		return
	}
	if err := m.Respond(data); err != nil {
		log.Printf("plugin-runner: failed to respond to device search: %v", err)
	}
}

func (r *Runner) handleEntitySearch(m *nats.Msg) {
	var q types.SearchQuery
	if err := json.Unmarshal(m.Data, &q); err != nil {
		return
	}
	matches := r.searchEntitiesLocal(q)
	if matches == nil {
		matches = []types.Entity{}
	}
	res := types.SearchEntitiesResponse{
		PluginID: r.manifest.ID,
		Matches:  matches,
	}
	data, err := json.Marshal(res)
	if err != nil {
		log.Printf("plugin-runner: failed to marshal entity search results: %v", err)
		return
	}
	if err := m.Respond(data); err != nil {
		log.Printf("plugin-runner: failed to respond to entity search: %v", err)
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

func (r *Runner) searchDevicesLocal(q types.SearchQuery) []types.Device {
	var matches []types.Device
	qLower := strings.ToLower(q.Pattern)
	for _, d := range r.loadDevices() {
		if q.PluginID != "" && q.PluginID != r.manifest.ID {
			break
		}
		if q.DeviceID != "" && q.DeviceID != d.ID {
			continue
		}
		if !matchesLabels(d.Labels, q.Labels) {
			continue
		}

		if qLower != "" && qLower != "*" {
			matched := false
			if strings.Contains(strings.ToLower(d.ID), qLower) ||
				strings.Contains(strings.ToLower(d.SourceID), qLower) ||
				strings.Contains(strings.ToLower(d.SourceName), qLower) ||
				strings.Contains(strings.ToLower(d.LocalName), qLower) {
				matched = true
			}
			if !matched {
				for _, vals := range d.Labels {
					for _, v := range vals {
						if strings.Contains(strings.ToLower(v), qLower) {
							matched = true
							break
						}
					}
					if matched {
						break
					}
				}
			}
			if !matched {
				continue
			}
		} else if q.Pattern != "" {
			if ok, _ := filepath.Match(q.Pattern, d.ID); !ok {
				continue
			}
		}

		matches = append(matches, d)
	}
	return applyDeviceLimit(matches, q.Limit)
}

func (r *Runner) searchEntitiesLocal(q types.SearchQuery) []types.Entity {
	var matches []types.Entity
	qLower := strings.ToLower(q.Pattern)
	for _, d := range r.loadDevices() {
		if q.PluginID != "" && q.PluginID != r.manifest.ID {
			break
		}
		if q.DeviceID != "" && q.DeviceID != d.ID {
			continue
		}
		for _, e := range r.loadEntities(d.ID) {
			if q.EntityID != "" && q.EntityID != e.ID {
				continue
			}
			if q.Domain != "" && q.Domain != e.Domain {
				continue
			}
			if !matchesLabels(e.Labels, q.Labels) {
				continue
			}

			if qLower != "" && qLower != "*" {
				matched := false
				if strings.Contains(strings.ToLower(e.ID), qLower) ||
					strings.Contains(strings.ToLower(e.SourceID), qLower) ||
					strings.Contains(strings.ToLower(e.SourceName), qLower) ||
					strings.Contains(strings.ToLower(e.LocalName), qLower) {
					matched = true
				}
				if !matched {
					for _, vals := range e.Labels {
						for _, v := range vals {
							if strings.Contains(strings.ToLower(v), qLower) {
								matched = true
								break
							}
						}
						if matched {
							break
						}
					}
				}
				if !matched {
					continue
				}
			} else if q.Pattern != "" {
				if ok, _ := filepath.Match(q.Pattern, e.ID); !ok {
					continue
				}
			}

			matches = append(matches, e)
		}
	}
	return applyEntityLimit(matches, q.Limit)
}

func applyDeviceLimit(items []types.Device, limit int) []types.Device {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
}

func applyEntityLimit(items []types.Entity, limit int) []types.Entity {
	if limit <= 0 || len(items) <= limit {
		return items
	}
	return items[:limit]
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

	start := time.Now()
	paramsBytes, _ := json.Marshal(params)
	id := json.RawMessage(`1`)
	req := types.Request{JSONRPC: types.JSONRPCVersion, ID: &id, Method: method, Params: paramsBytes}
	data, _ := json.Marshal(req)
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "rpc request",
			"plugin_id", pluginID,
			"method", method,
			"timeout_ms", timeout.Milliseconds(),
			"start_unix_ms", start.UnixMilli())
	}
	msg, err := r.nc.Request(reg.RPCSubject, data, timeout)
	if err != nil {
		if r.logger != nil {
			r.logger.Log(context.Background(), LevelTrace, "rpc request failed",
				"plugin_id", pluginID,
				"method", method,
				"error", err.Error(),
				"elapsed_ms", time.Since(start).Milliseconds())
		}
		return types.Response{}, err
	}
	var resp types.Response
	if err := json.Unmarshal(msg.Data, &resp); err != nil {
		if r.logger != nil {
			r.logger.Log(context.Background(), LevelTrace, "rpc response unmarshal failed",
				"plugin_id", pluginID,
				"method", method,
				"error", err.Error(),
				"elapsed_ms", time.Since(start).Milliseconds())
		}
		return types.Response{}, err
	}
	if resp.Error != nil {
		if r.logger != nil {
			r.logger.Log(context.Background(), LevelTrace, "rpc response error",
				"plugin_id", pluginID,
				"method", method,
				"error", resp.Error.Message,
				"elapsed_ms", time.Since(start).Milliseconds())
		}
		return resp, fmt.Errorf("%s", resp.Error.Message)
	}
	if r.logger != nil {
		fields := []any{
			"plugin_id", pluginID,
			"method", method,
			"elapsed_ms", time.Since(start).Milliseconds(),
		}
		if method == "entities/commands/create" {
			var status types.CommandStatus
			if err := json.Unmarshal(resp.Result, &status); err == nil && strings.TrimSpace(status.CommandID) != "" {
				fields = append(fields,
					"command_id", status.CommandID,
					"command_state", string(status.State),
					"target_device_id", status.DeviceID,
					"target_entity_id", status.EntityID)
			}
		}
		r.logger.Log(context.Background(), LevelTrace, "rpc response ok", fields...)
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

func (r *Runner) refreshDevices() ([]types.Device, error) {
	current := r.loadDevices()
	discovered, err := r.plugin.OnDevicesList(current)
	if err != nil {
		return nil, err
	}
	merged := ReconcileDevicesAdditive(current, discovered)
	for _, dev := range merged {
		if strings.TrimSpace(dev.ID) == "" {
			continue
		}
		r.saveDevice(dev)
		if _, err := r.refreshEntities(dev.ID); err != nil {
			return nil, err
		}
	}
	return merged, nil
}

func (r *Runner) bootstrapStartupTopology() error {
	current := r.loadDevices()
	discovered, err := r.plugin.OnDevicesList(current)
	if err != nil {
		return err
	}
	merged := ReconcileDevicesAdditive(current, discovered)
	for _, dev := range merged {
		if strings.TrimSpace(dev.ID) == "" {
			continue
		}
		r.saveDevice(dev)
	}
	for _, dev := range merged {
		if strings.TrimSpace(dev.ID) == "" {
			continue
		}
		if _, err := r.refreshEntities(dev.ID); err != nil {
			return fmt.Errorf("entities hydrate failed for device %s: %w", dev.ID, err)
		}
	}
	return nil
}

func (r *Runner) refreshEntities(deviceID string) ([]types.Entity, error) {
	current := r.loadEntities(deviceID)
	discovered, err := r.plugin.OnEntitiesList(deviceID, current)
	if err != nil {
		return nil, err
	}
	merged := ReconcileEntitiesAdditive(current, discovered, deviceID)
	for _, ent := range merged {
		if strings.TrimSpace(ent.ID) == "" {
			continue
		}
		if strings.TrimSpace(ent.DeviceID) == "" {
			ent.DeviceID = deviceID
		}
		if strings.TrimSpace(ent.DeviceID) == "" {
			continue
		}
		r.saveEntity(ent)
	}
	return merged, nil
}

func (r *Runner) createDeviceWithFallback(dev types.Device) types.Device {
	input := dev
	if strings.TrimSpace(input.ID) == "" {
		input.ID = strings.TrimSpace(input.SourceID)
	}
	if strings.TrimSpace(input.SourceID) == "" {
		input.SourceID = input.ID
	}
	if input.LocalName == "" {
		input.LocalName = input.Name()
	}

	created, err := r.plugin.OnDeviceCreate(input)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("devices/create plugin rejected; using synthetic fallback", "plugin", r.manifest.ID, "device_id", input.ID, "error", err.Error())
		}
		return input
	}

	if strings.TrimSpace(created.ID) == "" {
		created.ID = input.ID
	}
	if strings.TrimSpace(created.SourceID) == "" {
		created.SourceID = input.SourceID
	}
	if created.LocalName == "" {
		created.LocalName = created.Name()
	}
	return created
}

func (r *Runner) createEntityWithFallback(ent types.Entity) types.Entity {
	input := ent
	if strings.TrimSpace(input.ID) == "" {
		input.ID = strings.TrimSpace(input.SourceID)
	}
	if strings.TrimSpace(input.SourceID) == "" {
		input.SourceID = input.ID
	}
	if input.LocalName == "" {
		if input.SourceName != "" {
			input.LocalName = input.SourceName
		} else {
			input.LocalName = input.ID
		}
	}
	if input.Data.SyncStatus == types.SyncStatusEmpty {
		input.Data.SyncStatus = types.SyncStatusSynced
	}
	if input.Data.UpdatedAt.IsZero() {
		input.Data.UpdatedAt = time.Now().UTC()
	}

	created, err := r.plugin.OnEntityCreate(input)
	if err != nil {
		if r.logger != nil {
			r.logger.Warn("entities/create plugin rejected; using synthetic fallback", "plugin", r.manifest.ID, "device_id", input.DeviceID, "entity_id", input.ID, "error", err.Error())
		}
		return input
	}

	if strings.TrimSpace(created.ID) == "" {
		created.ID = input.ID
	}
	if strings.TrimSpace(created.SourceID) == "" {
		created.SourceID = input.SourceID
	}
	if strings.TrimSpace(created.DeviceID) == "" {
		created.DeviceID = input.DeviceID
	}
	if created.LocalName == "" {
		created.LocalName = input.LocalName
	}
	if created.Data.SyncStatus == types.SyncStatusEmpty {
		created.Data.SyncStatus = input.Data.SyncStatus
	}
	if created.Data.UpdatedAt.IsZero() {
		created.Data.UpdatedAt = input.Data.UpdatedAt
	}
	return created
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
	_ = r.ensureStateStore().SaveDevice(dev)
}

func (r *Runner) deleteDevice(id string) {
	_ = r.ensureStateStore().DeleteDevice(id)
}

func (r *Runner) deleteEntity(deviceID, entityID string) {
	_ = r.ensureStateStore().DeleteEntity(deviceID, entityID)
}

func (r *Runner) loadDevices() []types.Device {
	items, err := r.ensureStateStore().LoadDevices()
	if err != nil {
		return nil
	}
	return items
}

func (r *Runner) loadDeviceByID(id string) types.Device {
	dev, err := r.ensureStateStore().LoadDeviceByID(id)
	if err != nil {
		return types.Device{}
	}
	return dev
}

func (r *Runner) saveEntity(ent types.Entity) {
	_ = r.ensureStateStore().SaveEntity(ent)
}

func (r *Runner) loadEntity(deviceID, entityID string) types.Entity {
	ent, err := r.ensureStateStore().LoadEntity(deviceID, entityID)
	if err != nil {
		return types.Entity{}
	}
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
	items, err := r.ensureStateStore().LoadEntities(deviceID)
	if err != nil {
		return nil
	}
	return items
}

func (r *Runner) loadState(id string) types.Storage {
	store, err := r.ensureStateStore().LoadState(id)
	if err != nil {
		return types.Storage{}
	}
	return store
}

func (r *Runner) saveStateSynced(id string, store types.Storage) {
	_ = r.ensureStateStore().SaveState(id, store)
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
	start := time.Now()
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
	syncStart := time.Now()
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		return err
	}
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "write_if_changed fsync",
			"path", path,
			"sync_elapsed_ms", time.Since(syncStart).Milliseconds())
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
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "write_if_changed done",
			"path", path,
			"total_elapsed_ms", time.Since(start).Milliseconds())
	}
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
