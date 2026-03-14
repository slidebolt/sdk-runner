package runner

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
	regsvc "github.com/slidebolt/registry"
	lightentity "github.com/slidebolt/sdk-entities/light"
	switchentity "github.com/slidebolt/sdk-entities/switch"
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

	reg       *regsvc.Registry
	scheduler *scheduler

	logger   *slog.Logger
	logLevel *slog.LevelVar
}

var idSeq uint64

func nextID(prefix string) string {
	return fmt.Sprintf("%s-%d-%d", prefix, time.Now().UnixNano(), atomic.AddUint64(&idSeq, 1))
}

func NewRunner(p Plugin) (*Runner, error) {
	dataDir, err := RequireEnv(types.EnvPluginDataDir)
	if err != nil {
		return nil, err
	}
	namespace := "runner-local"
	if rpcSubject := strings.TrimSpace(os.Getenv(types.EnvPluginRPCSbj)); rpcSubject != "" {
		namespace = strings.TrimPrefix(rpcSubject, types.SubjectRPCPrefix)
		if strings.TrimSpace(namespace) == "" {
			namespace = "runner-local"
		}
	}
	dataDirRoot := dataDir
	if filepath.Base(dataDir) == namespace {
		dataDirRoot = filepath.Dir(dataDir)
	}
	logger, levelVar := newLogger(defaultLogServiceName())
	r := &Runner{
		plugin:    p,
		dataDir:   dataDir,
		statuses:  make(map[string]types.CommandStatus),
		scheduler: newScheduler(),
		logger:    logger,
		logLevel:  levelVar,
	}
	r.reg = regsvc.RegistryService(namespace, regsvc.WithDataDir(dataDirRoot), regsvc.WithPersist(regsvc.PersistOnChange))
	return r, nil
}

func (r *Runner) LogLevel() string {
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

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return r.RunContext(ctx)
}

// RunContext starts the plugin runner and serves RPC requests until ctx is done.
func (r *Runner) RunContext(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	url, err := RequireEnv(types.EnvNATSURL)
	if err != nil {
		return err
	}
	var regData []byte
	for i := 0; i < 5; i++ {
		r.nc, err = nats.Connect(url,
			nats.RetryOnFailedConnect(true),
			nats.MaxReconnects(-1),
			nats.ReconnectWait(200*time.Millisecond),
			nats.DisconnectErrHandler(func(_ *nats.Conn, derr error) {
				if r.logger != nil {
					r.logger.Warn("nats disconnected", "error", derr)
				}
			}),
			nats.ReconnectHandler(func(c *nats.Conn) {
				if r.logger != nil {
					r.logger.Info("nats reconnected", "url", c.ConnectedUrl())
				}
				// Push a fresh registration immediately after reconnect so the
				// gateway can restore registry/search routing quickly.
				if len(regData) > 0 {
					_ = c.Publish(types.SubjectRegistration, regData)
				}
			}),
		)
		if err == nil {
			break
		}
		time.Sleep(time.Second)
	}
	if err != nil {
		return err
	}
	defer r.nc.Close()

	// Attach NATS to the registry now that we have a live connection.
	// This wires up lifecycle-event publishing and search-subject subscriptions.
	if err := r.reg.AttachNATS(r.nc); err != nil {
		return fmt.Errorf("registry NATS attach failed: %w", err)
	}

	// Load persisted state from disk into the registry's in-memory cache before
	// handing control to the plugin. Without this, LoadState() always returns
	// empty on first call because the cache was never populated.
	if err := r.reg.LoadAll(); err != nil {
		return fmt.Errorf("registry load failed: %w", err)
	}

	pluginCtx := PluginContext{
		Registry:  r.reg,
		Logger:    r.logger,
		Events:    r,
		Commands:  r,
		Scheduler: r.scheduler,
	}
	var errInit error
	r.manifest, errInit = r.plugin.Initialize(pluginCtx)
	if errInit != nil {
		return fmt.Errorf("plugin initialization failed: %w", errInit)
	}
	r.rpcSubject = types.SubjectRPCPrefix + r.manifest.ID

	r.scheduler.start()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := r.plugin.Start(waitCtx); err != nil {
		log.Printf("plugin-runner: Start failed: %v; proceeding with registration anyway", err)
	}
	waitCancel()

	reg := types.Registration{Manifest: r.manifest, RPCSubject: r.rpcSubject}
	regData, err = json.Marshal(reg)
	if err != nil {
		return fmt.Errorf("failed to marshal registration: %w", err)
	}
	if err := r.nc.Publish(types.SubjectRegistration, regData); err != nil {
		log.Printf("plugin-runner: failed to publish registration: %v", err)
	}

	r.nc.Subscribe(r.rpcSubject, r.handleRPC)
	r.nc.Subscribe(r.rpcSubject+".command", r.handleDirectCommand)
	r.nc.Subscribe(types.SubjectSearchPlugins, r.handlePluginSearch)
	r.nc.Subscribe(types.SubjectRegistration, r.handleRegistration)
	r.nc.Subscribe(types.SubjectDiscoveryProbe, func(m *nats.Msg) {
		r.nc.Publish(types.SubjectRegistration, regData)
	})
	// Registration heartbeat: keeps gateway registry converged even if
	// reconnect/probe messages are missed during turbulence.
	regStop := make(chan struct{})
	go func() {
		t := time.NewTicker(1 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-regStop:
				return
			case <-t.C:
				_ = r.nc.Publish(types.SubjectRegistration, regData)
			}
		}
	}()

	<-ctx.Done()
	close(regStop)
	r.scheduler.stop()
	_ = r.plugin.Stop()
	_ = r.nc.Drain()
	return nil
}

func (r *Runner) runDiscoveryMode() error {
	_ = os.MkdirAll(r.dataDir, 0o755)
	ctx := PluginContext{
		Registry:  r.reg,
		Logger:    r.logger,
		Events:    r,
		Commands:  r,
		Scheduler: r.scheduler,
	}
	var err error
	r.manifest, err = r.plugin.Initialize(ctx)
	if err != nil {
		return fmt.Errorf("plugin initialization failed: %w", err)
	}

	r.scheduler.start()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := r.plugin.Start(waitCtx); err != nil {
		log.Printf("plugin-runner: Start failed in discovery mode: %v; proceeding anyway", err)
	}
	waitCancel()

	time.Sleep(2 * time.Second) // wait for lazy discovery routines
	r.scheduler.stop()
	devices := r.reg.LoadDevices()

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
	case types.RPCMethodHealthCheck:
		result = map[string]string{"status": "perfect", "service": "plugin"}

	case types.RPCMethodInitialize:
		result = r.manifest

	case types.RPCMethodLoggingGetLevel:
		result = map[string]any{
			"level": r.LogLevel(),
		}

	case types.RPCMethodLoggingSetLevel:
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

	case types.RPCMethodStorageUpdate:
		var newConfig types.Storage
		if err := json.Unmarshal(req.Params, &newConfig); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		if err := r.reg.SaveState(newConfig); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		result = newConfig

	case types.RPCMethodStorageFlush:
		if err := r.reg.Flush(); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		result = map[string]any{"ok": true}

	case types.RPCMethodPluginReset:
		if err := r.plugin.OnReset(); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		result = map[string]any{
			"ok": true,
		}

	case types.RPCMethodDevicesCreate:
		var dev types.Device
		if err := strictDecode(req.Params, &dev); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		if strings.TrimSpace(dev.ID) == "" {
			dev.ID = strings.TrimSpace(dev.SourceID)
		}
		if strings.TrimSpace(dev.SourceID) == "" {
			dev.SourceID = dev.ID
		}
		if strings.TrimSpace(dev.LocalName) == "" {
			dev.LocalName = dev.Name()
		}
		_, existed := r.reg.LoadDevice(dev.ID)
		if err := r.reg.SaveDevice(dev); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		if existed {
			r.publishLifecycle(types.SubjectDeviceUpdated, types.BatchDeviceItem{PluginID: r.manifest.ID, Device: dev})
		} else {
			r.publishLifecycle(types.SubjectDeviceCreated, types.BatchDeviceItem{PluginID: r.manifest.ID, Device: dev})
		}
		result = dev

	case types.RPCMethodDevicesUpdate:
		var incoming types.Device
		if err := strictDecode(req.Params, &incoming); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		// Merge with persisted data so source fields (source_id, source_name)
		// and the user field (local_name) never overwrite each other.
		dev, ok := r.reg.LoadDevice(incoming.ID)
		if !ok {
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
		if err := r.reg.SaveDevice(dev); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		if ok {
			r.publishLifecycle(types.SubjectDeviceUpdated, types.BatchDeviceItem{PluginID: r.manifest.ID, Device: dev})
		} else {
			r.publishLifecycle(types.SubjectDeviceCreated, types.BatchDeviceItem{PluginID: r.manifest.ID, Device: dev})
		}
		result = dev

	case types.RPCMethodDevicesDelete:
		var id string
		if err := json.Unmarshal(req.Params, &id); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		if err := r.reg.DeleteDevice(id); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		r.publishLifecycle(types.SubjectDeviceDeleted, types.BatchDeviceRef{PluginID: r.manifest.ID, DeviceID: id})
		result = true

	case types.RPCMethodDevicesList:
		devices := r.reg.LoadDevices()
		r.publishLifecycle(types.SubjectDeviceRead, types.BatchDeviceRead{PluginID: r.manifest.ID, Count: len(devices)})
		result = devices

	case types.RPCMethodDevicesRefresh:
		devices := r.reg.LoadDevices()
		for _, dev := range devices {
			r.publishLifecycle(types.SubjectDeviceUpdated, types.BatchDeviceItem{PluginID: r.manifest.ID, Device: dev})
		}
		result = devices

	case types.RPCMethodEntitiesCreate:
		var ent types.Entity
		if err := strictDecode(req.Params, &ent); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		_, existed := r.reg.GetEntity(r.reg.Namespace(), ent.DeviceID, ent.ID)
		if err := r.reg.SaveEntity(ent); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		if existed {
			r.publishLifecycle(types.SubjectEntityUpdated, types.BatchEntityItem{PluginID: r.manifest.ID, DeviceID: ent.DeviceID, Entity: ent})
		} else {
			r.publishLifecycle(types.SubjectEntityCreated, types.BatchEntityItem{PluginID: r.manifest.ID, DeviceID: ent.DeviceID, Entity: ent})
		}
		if dev, ok := r.reg.LoadDevice(ent.DeviceID); ok {
			_ = r.reg.SaveDevice(dev)
			r.publishLifecycle(types.SubjectDeviceUpdated, types.BatchDeviceItem{PluginID: r.manifest.ID, Device: dev})
		} else if strings.TrimSpace(ent.DeviceID) != "" {
			dev := types.Device{ID: ent.DeviceID, SourceID: ent.DeviceID, LocalName: ent.DeviceID}
			_ = r.reg.SaveDevice(dev)
			r.publishLifecycle(types.SubjectDeviceCreated, types.BatchDeviceItem{PluginID: r.manifest.ID, Device: dev})
		}
		result = ent

	case types.RPCMethodEntitiesUpdate:
		var incoming types.Entity
		if err := strictDecode(req.Params, &incoming); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		// Merge with persisted data so source fields (domain, actions) and the
		// user field (local_name) never overwrite each other.
		ent, ok := r.reg.GetEntity(r.reg.Namespace(), incoming.DeviceID, incoming.ID)
		if !ok {
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
		if err := r.reg.SaveEntity(ent); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		if ok {
			r.publishLifecycle(types.SubjectEntityUpdated, types.BatchEntityItem{PluginID: r.manifest.ID, DeviceID: ent.DeviceID, Entity: ent})
		} else {
			r.publishLifecycle(types.SubjectEntityCreated, types.BatchEntityItem{PluginID: r.manifest.ID, DeviceID: ent.DeviceID, Entity: ent})
		}
		result = ent

	case types.RPCMethodEntitiesDelete:
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		if err := r.reg.DeleteEntity(r.reg.Namespace(), params.DeviceID, params.EntityID); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		r.publishLifecycle(types.SubjectEntityDeleted, types.BatchEntityRef{PluginID: r.manifest.ID, DeviceID: params.DeviceID, EntityID: params.EntityID})
		result = true

	case types.RPCMethodEntitiesList:
		var params struct {
			DeviceID string `json:"device_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		entities := r.reg.GetEntities(r.reg.Namespace(), params.DeviceID)
		r.publishLifecycle(types.SubjectEntityRead, types.BatchEntityRead{
			PluginID: r.manifest.ID,
			DeviceID: params.DeviceID,
			Count:    len(entities),
		})
		result = entities

	case types.RPCMethodEntitiesRefresh:
		var params struct {
			DeviceID string `json:"device_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		entities := r.reg.GetEntities(r.reg.Namespace(), params.DeviceID)
		for _, ent := range entities {
			r.publishLifecycle(types.SubjectEntityUpdated, types.BatchEntityItem{
				PluginID: r.manifest.ID,
				DeviceID: params.DeviceID,
				Entity:   ent,
			})
		}
		result = entities

	case types.RPCMethodSnapshotsSave:
		var params struct {
			DeviceID string              `json:"device_id"`
			EntityID string              `json:"entity_id"`
			Name     string              `json:"name"`
			Labels   map[string][]string `json:"labels,omitempty"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		snap, err := r.saveSnapshot(params.DeviceID, params.EntityID, params.Name, params.Labels)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = snap
		}

	case types.RPCMethodSnapshotsList:
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		snaps, err := r.listSnapshots(params.DeviceID, params.EntityID)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = snaps
		}

	case types.RPCMethodSnapshotsDelete:
		var params struct {
			DeviceID   string `json:"device_id"`
			EntityID   string `json:"entity_id"`
			SnapshotID string `json:"snapshot_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		snap, err := r.deleteSnapshot(params.DeviceID, params.EntityID, params.SnapshotID)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = snap
		}

	case types.RPCMethodSnapshotsRestore:
		var params struct {
			DeviceID   string `json:"device_id"`
			EntityID   string `json:"entity_id"`
			SnapshotID string `json:"snapshot_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		snap, err := r.restoreSnapshot(params.DeviceID, params.EntityID, params.SnapshotID)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = snap
		}

	case types.RPCMethodCommandsCreate:
		var params struct {
			CommandID string          `json:"command_id"`
			DeviceID  string          `json:"device_id"`
			EntityID  string          `json:"entity_id"`
			Payload   json.RawMessage `json:"payload"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		if strings.TrimSpace(params.CommandID) == "" {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: command_id is required"}
			break
		}
		status, err := r.createCommand(params.CommandID, params.DeviceID, params.EntityID, params.Payload)
		if err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
		} else {
			result = status
		}

	case types.RPCMethodCommandsStatusGet:
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

	case types.RPCMethodEventsIngest:
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

	case types.RPCMethodScriptsPut:
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
			Source   string `json:"source"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		script := types.Script{
			DeviceID: params.DeviceID,
			EntityID: params.EntityID,
			Source:   params.Source,
		}
		if err := r.reg.SaveScript(script); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		result = map[string]string{"entity_id": params.EntityID}

	case types.RPCMethodScriptsGet:
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		script, ok := r.reg.LoadScript(params.DeviceID, params.EntityID)
		if !ok {
			rpcErr = &types.RPCError{Code: -32004, Message: "script not found"}
			break
		}
		result = map[string]string{"entity_id": params.EntityID, "source": script.Source}

	case types.RPCMethodScriptsDelete:
		var params struct {
			DeviceID   string `json:"device_id"`
			EntityID   string `json:"entity_id"`
			PurgeState bool   `json:"purge_state"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		if err := r.reg.DeleteScript(params.DeviceID, params.EntityID); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		if params.PurgeState {
			if err := r.reg.DeleteLocalScript(params.DeviceID, params.EntityID); err != nil {
				rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
				break
			}
		}
		result = map[string]bool{"ok": true}

	case types.RPCMethodScriptStateGet:
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		state, ok := r.reg.LoadLocalScript(params.DeviceID, params.EntityID)
		if !ok || state == nil {
			result = map[string]any{}
			break
		}
		result = state

	case types.RPCMethodScriptStatePut:
		var params struct {
			DeviceID string         `json:"device_id"`
			EntityID string         `json:"entity_id"`
			State    map[string]any `json:"state"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		if err := r.reg.SaveLocalScript(params.DeviceID, params.EntityID, params.State); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		if params.State == nil {
			result = map[string]any{}
		} else {
			result = params.State
		}

	case types.RPCMethodScriptStateDelete:
		var params struct {
			DeviceID string `json:"device_id"`
			EntityID string `json:"entity_id"`
		}
		if err := json.Unmarshal(req.Params, &params); err != nil {
			rpcErr = &types.RPCError{Code: -32700, Message: "invalid params: " + err.Error()}
			break
		}
		if err := r.reg.DeleteLocalScript(params.DeviceID, params.EntityID); err != nil {
			rpcErr = &types.RPCError{Code: -32001, Message: err.Error()}
			break
		}
		result = map[string]bool{"ok": true}

	default:
		rpcErr = &types.RPCError{Code: -32601, Message: "method not found"}
	}

	r.sendResponse(m, req.ID, result, rpcErr)
}

func (r *Runner) createCommand(commandID, deviceID, entityID string, payload json.RawMessage) (types.CommandStatus, error) {
	cmdStart := time.Now()
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "create_command start",
			"device_id", deviceID,
			"entity_id", entityID,
			"payload", string(payload),
			"start_unix_ms", cmdStart.UnixMilli())
	}
	resolveStart := time.Now()
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "create_command resolve_entity start",
			"device_id", deviceID,
			"entity_id", entityID,
			"start_unix_ms", resolveStart.UnixMilli())
	}
	ent, ok := r.reg.GetEntity(r.reg.Namespace(), deviceID, entityID)
	if !ok {
		ent = types.Entity{}
	}
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
		ID:         commandID,
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
	if err := r.reg.SaveEntity(ent); err == nil {
		r.publishLifecycle(types.SubjectEntityUpdated, types.BatchEntityItem{
			PluginID: r.manifest.ID,
			DeviceID: ent.DeviceID,
			Entity:   ent,
		})
	}

	onCommandStart := time.Now()
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "create_command on_command start",
			"command_id", cmd.ID,
			"device_id", deviceID,
			"entity_id", entityID,
			"start_unix_ms", onCommandStart.UnixMilli())
	}
	err := r.plugin.OnCommand(cmd, ent)
	updated := ent
	if current, ok := r.reg.GetEntity(r.reg.Namespace(), deviceID, entityID); ok {
		updated = current
	}
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
			_ = r.reg.SaveEntity(entCopy)
			r.publishLifecycle(types.SubjectEntityUpdated, types.BatchEntityItem{
				PluginID: r.manifest.ID,
				DeviceID: entCopy.DeviceID,
				Entity:   entCopy,
			})
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
	if r.logger != nil {
		r.logger.Log(context.Background(), LevelTrace, "create_command persist ok",
			"command_id", cmd.ID,
			"device_id", deviceID,
			"entity_id", entityID,
			"persist_elapsed_ms", int64(0),
			"total_elapsed_ms", time.Since(cmdStart).Milliseconds())
	}
	return status, nil
}

func (r *Runner) PublishEvent(evt types.InboundEvent) error {
	_, err := r.processInboundEvent(evt)
	return err
}

func (r *Runner) SendCommand(req types.Command) error {
	if r.nc == nil {
		return fmt.Errorf("nats not connected")
	}
	data, err := json.Marshal(req)
	if err != nil {
		return err
	}
	subj := types.SubjectRPCPrefix + req.PluginID + ".command"
	return r.nc.Publish(subj, data)
}

// handleDirectCommand processes fire-and-forget command messages published
// directly to this plugin's "<rpcSubject>.command" NATS subject. This enables
// plugin-to-plugin command routing without going through the gateway HTTP layer.
func (r *Runner) handleDirectCommand(m *nats.Msg) {
	var cmd types.Command
	if err := json.Unmarshal(m.Data, &cmd); err != nil {
		log.Printf("plugin-runner: handleDirectCommand: unmarshal: %v", err)
		return
	}
	if _, err := r.createCommand(cmd.ID, cmd.DeviceID, cmd.EntityID, cmd.Payload); err != nil {
		log.Printf("plugin-runner: handleDirectCommand: createCommand entity=%s: %v", cmd.EntityID, err)
	}
}

func (r *Runner) processInboundEvent(evt types.InboundEvent) (types.Entity, error) {
	ent, ok := r.reg.GetEntity(r.reg.Namespace(), evt.DeviceID, evt.EntityID)
	if !ok {
		ent = types.Entity{}
	}
	if ent.ID == "" {
		return types.Entity{}, fmt.Errorf("entity not found")
	}
	if err := validateInboundPayload(ent.Domain, evt.Payload); err != nil {
		return types.Entity{}, err
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

	// Plugins no longer handle events via OnEvent, the runner directly updates the canonical state.
	updated := ent
	updated.Data.Reported = evt.Payload
	updated.Data.Effective = evt.Payload
	updated.Data.SyncStatus = types.SyncStatusSynced
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
	_ = r.reg.SaveEntity(updated)
	r.publishLifecycle(types.SubjectEntityUpdated, types.BatchEntityItem{
		PluginID: r.manifest.ID,
		DeviceID: updated.DeviceID,
		Entity:   updated,
	})

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
		if err := r.nc.Publish(types.SubjectEntityEvents, data); err != nil {
			log.Printf("plugin-runner: failed to publish entity event: %v", err)
		}
	}
	return updated, nil
}

func validateInboundPayload(domain string, payload json.RawMessage) error {
	switch domain {
	case lightentity.Type:
		return validateCanonicalLightStatePayload(payload)
	case switchentity.Type:
		return validateCanonicalSwitchStatePayload(payload)
	default:
		return nil
	}
}

func validateCanonicalLightStatePayload(payload json.RawMessage) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return fmt.Errorf("light payload must be valid canonical state JSON: %w", err)
	}
	if _, ok := raw["type"]; ok {
		return fmt.Errorf("light payload must be canonical state, not legacy event payload")
	}
	var probe struct {
		Power *bool `json:"power"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return fmt.Errorf("light payload must decode as canonical state: %w", err)
	}
	if probe.Power == nil {
		return fmt.Errorf("light payload missing required field \"power\"")
	}
	cmds, err := types.StateToCommands(lightentity.Type, payload)
	if err != nil {
		return fmt.Errorf("light payload is not valid canonical state: %w", err)
	}
	for _, cmdPayload := range cmds {
		cmd, err := lightentity.ParseCommand(types.Command{Payload: cmdPayload})
		if err != nil {
			return fmt.Errorf("light payload is not valid canonical state: %w", err)
		}
		if err := lightentity.ValidateCommand(cmd); err != nil {
			return fmt.Errorf("light payload is not valid canonical state: %w", err)
		}
	}
	return nil
}

func validateCanonicalSwitchStatePayload(payload json.RawMessage) error {
	var raw map[string]json.RawMessage
	if err := json.Unmarshal(payload, &raw); err != nil {
		return fmt.Errorf("switch payload must be valid canonical state JSON: %w", err)
	}
	if _, ok := raw["type"]; ok {
		return fmt.Errorf("switch payload must be canonical state, not legacy event payload")
	}
	var probe struct {
		Power *bool `json:"power"`
	}
	if err := json.Unmarshal(payload, &probe); err != nil {
		return fmt.Errorf("switch payload must decode as canonical state: %w", err)
	}
	if probe.Power == nil {
		return fmt.Errorf("switch payload missing required field \"power\"")
	}
	cmds, err := types.StateToCommands(switchentity.Type, payload)
	if err != nil {
		return fmt.Errorf("switch payload is not valid canonical state: %w", err)
	}
	for _, cmdPayload := range cmds {
		cmd, err := switchentity.ParseCommand(types.Command{Payload: cmdPayload})
		if err != nil {
			return fmt.Errorf("switch payload is not valid canonical state: %w", err)
		}
		if err := switchentity.ValidateCommand(cmd); err != nil {
			return fmt.Errorf("switch payload is not valid canonical state: %w", err)
		}
	}
	return nil
}

func (r *Runner) saveSnapshot(deviceID, entityID, name string, labels map[string][]string) (types.EntitySnapshot, error) {
	ent, ok := r.reg.GetEntity(r.reg.Namespace(), deviceID, entityID)
	if !ok || ent.ID == "" {
		return types.EntitySnapshot{}, fmt.Errorf("entity not found")
	}
	state := ent.Data.Effective
	if len(state) == 0 {
		state = json.RawMessage(`{}`)
	}
	snap := types.EntitySnapshot{
		ID:        nextID("snap"),
		Name:      name,
		State:     append(json.RawMessage(nil), state...),
		Labels:    labels,
		CreatedAt: time.Now().UTC(),
	}
	if ent.Snapshots == nil {
		ent.Snapshots = make(map[string]types.EntitySnapshot)
	}
	ent.Snapshots[snap.ID] = snap
	if err := r.reg.SaveEntity(ent); err != nil {
		return types.EntitySnapshot{}, err
	}
	r.publishLifecycle(types.SubjectEntityUpdated, types.BatchEntityItem{
		PluginID: r.manifest.ID,
		DeviceID: ent.DeviceID,
		Entity:   ent,
	})
	return snap, nil
}

func (r *Runner) listSnapshots(deviceID, entityID string) ([]types.EntitySnapshot, error) {
	ent, ok := r.reg.GetEntity(r.reg.Namespace(), deviceID, entityID)
	if !ok || ent.ID == "" {
		return nil, fmt.Errorf("entity not found")
	}
	out := make([]types.EntitySnapshot, 0, len(ent.Snapshots))
	for _, snap := range ent.Snapshots {
		out = append(out, snap)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].CreatedAt.Before(out[j].CreatedAt) })
	return out, nil
}

func (r *Runner) deleteSnapshot(deviceID, entityID, snapshotID string) (types.EntitySnapshot, error) {
	ent, ok := r.reg.GetEntity(r.reg.Namespace(), deviceID, entityID)
	if !ok || ent.ID == "" {
		return types.EntitySnapshot{}, fmt.Errorf("entity not found")
	}
	snap, ok := ent.Snapshots[snapshotID]
	if !ok {
		return types.EntitySnapshot{}, fmt.Errorf("snapshot not found")
	}
	delete(ent.Snapshots, snapshotID)
	if len(ent.Snapshots) == 0 {
		ent.Snapshots = nil
	}
	if err := r.reg.SaveEntity(ent); err != nil {
		return types.EntitySnapshot{}, err
	}
	r.publishLifecycle(types.SubjectEntityUpdated, types.BatchEntityItem{
		PluginID: r.manifest.ID,
		DeviceID: ent.DeviceID,
		Entity:   ent,
	})
	return snap, nil
}

func (r *Runner) restoreSnapshot(deviceID, entityID, snapshotID string) (types.EntitySnapshot, error) {
	ent, ok := r.reg.GetEntity(r.reg.Namespace(), deviceID, entityID)
	if !ok || ent.ID == "" {
		return types.EntitySnapshot{}, fmt.Errorf("entity not found")
	}
	snap, ok := ent.Snapshots[snapshotID]
	if !ok {
		return types.EntitySnapshot{}, fmt.Errorf("snapshot not found")
	}
	if _, err := r.createCommand(nextID("gcmd"), deviceID, entityID, append(json.RawMessage(nil), snap.State...)); err != nil {
		return types.EntitySnapshot{}, err
	}
	return snap, nil
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
			r.nc.Publish(types.SubjectCommandStatus, data)
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

func strictDecode(data []byte, out any) error {
	dec := json.NewDecoder(bytes.NewReader(data))
	dec.DisallowUnknownFields()
	return dec.Decode(out)
}

// --- Lifecycle ---

func (r *Runner) publishLifecycle(subject string, payload any) {
	if r.nc == nil {
		return
	}
	data, err := json.Marshal(payload)
	if err != nil {
		log.Printf("plugin-runner: failed to marshal lifecycle event %s: %v", subject, err)
		return
	}
	if err := r.nc.Publish(subject, data); err != nil {
		log.Printf("plugin-runner: failed to publish lifecycle event %s: %v", subject, err)
	}
}
