package runner

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	types "github.com/slidebolt/sdk-types"
)

type stateStore interface {
	SaveDevice(dev types.Device) error
	DeleteDevice(id string) error
	LoadDevices() ([]types.Device, error)
	LoadDeviceByID(id string) (types.Device, error)

	SaveEntity(ent types.Entity) error
	DeleteEntity(deviceID, entityID string) error
	LoadEntity(deviceID, entityID string) (types.Entity, error)
	LoadEntities(deviceID string) ([]types.Entity, error)

	LoadState(id string) (types.Storage, error)
	SaveState(id string, store types.Storage) error
}

func newStateStore(r *Runner, backend string) stateStore {
	switch strings.ToLower(strings.TrimSpace(backend)) {
	case "memory":
		return newMemoryStateStore()
	default:
		return &fileStateStore{r: r}
	}
}

func (r *Runner) ensureStateStore() stateStore {
	r.lockMu.Lock()
	defer r.lockMu.Unlock()
	if r.stateStore == nil {
		r.stateStore = newStateStore(r, "file")
	}
	return r.stateStore
}
type fileStateStore struct {
	r *Runner
}

func (s *fileStateStore) SaveDevice(dev types.Device) error {
	if strings.TrimSpace(dev.SourceID) == "" {
		dev.SourceID = dev.ID
	}
	dir := filepath.Join(s.r.dataDir, "devices")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	type diskDevice types.Device
	data, err := json.MarshalIndent(diskDevice(dev), "", "  ")
	if err != nil {
		return err
	}
	return s.r.writeIfChanged(filepath.Join(dir, dev.ID+".json"), data)
}

func (s *fileStateStore) DeleteDevice(id string) error {
	path := filepath.Join(s.r.dataDir, "devices", id+".json")
	_ = os.Remove(path)
	s.r.clearHash(path)
	_ = os.RemoveAll(filepath.Join(s.r.dataDir, "devices", id))
	return nil
}

func (s *fileStateStore) LoadDevices() ([]types.Device, error) {
	files, err := filepath.Glob(filepath.Join(s.r.dataDir, "devices", "*.json"))
	if err != nil {
		return nil, err
	}
	items := make([]types.Device, 0, len(files))
	for _, f := range files {
		data, err := os.ReadFile(f)
		if err != nil {
			continue
		}
		s.r.seedHash(f, data)
		var dev types.Device
		if err := json.Unmarshal(data, &dev); err != nil {
			continue
		}
		if strings.TrimSpace(dev.SourceID) == "" {
			dev.SourceID = dev.ID
		}
		items = append(items, dev)
	}
	return items, nil
}

func (s *fileStateStore) LoadDeviceByID(id string) (types.Device, error) {
	path := filepath.Join(s.r.dataDir, "devices", id+".json")
	data, err := os.ReadFile(path)
	if err != nil {
		return types.Device{}, err
	}
	s.r.seedHash(path, data)
	var dev types.Device
	if err := json.Unmarshal(data, &dev); err != nil {
		return types.Device{}, err
	}
	if strings.TrimSpace(dev.SourceID) == "" {
		dev.SourceID = dev.ID
	}
	return dev, nil
}

func (s *fileStateStore) SaveEntity(ent types.Entity) error {
	if strings.TrimSpace(ent.SourceID) == "" {
		ent.SourceID = ent.ID
	}
	deviceFile := filepath.Join(s.r.dataDir, "devices", ent.DeviceID+".json")
	if _, err := os.Stat(deviceFile); os.IsNotExist(err) {
		if err := s.SaveDevice(types.Device{ID: ent.DeviceID}); err != nil {
			return err
		}
	}
	var writeErr error
	s.r.withDeviceLock(ent.DeviceID, func() {
		dir := filepath.Join(s.r.dataDir, "devices", ent.DeviceID, "entities")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			writeErr = err
			return
		}
		data, err := json.MarshalIndent(ent, "", "  ")
		if err != nil {
			writeErr = err
			return
		}
		writeErr = s.r.writeIfChanged(filepath.Join(dir, ent.ID+".json"), data)
	})
	return writeErr
}

func (s *fileStateStore) DeleteEntity(deviceID, entityID string) error {
	s.r.withDeviceLock(deviceID, func() {
		path := filepath.Join(s.r.dataDir, "devices", deviceID, "entities", entityID+".json")
		_ = os.Remove(path)
		s.r.clearHash(path)
	})
	return nil
}

func (s *fileStateStore) LoadEntity(deviceID, entityID string) (types.Entity, error) {
	var (
		data    []byte
		readErr error
	)
	path := filepath.Join(s.r.dataDir, "devices", deviceID, "entities", entityID+".json")
	s.r.withDeviceLock(deviceID, func() {
		data, readErr = os.ReadFile(path)
	})
	if readErr != nil {
		return types.Entity{}, readErr
	}
	s.r.seedHash(path, data)
	var ent types.Entity
	if err := json.Unmarshal(data, &ent); err != nil {
		return types.Entity{}, err
	}
	if strings.TrimSpace(ent.SourceID) == "" {
		ent.SourceID = ent.ID
	}
	return ent, nil
}

func (s *fileStateStore) LoadEntities(deviceID string) ([]types.Entity, error) {
	items := make([]types.Entity, 0)
	var listErr error
	s.r.withDeviceLock(deviceID, func() {
		files, err := filepath.Glob(filepath.Join(s.r.dataDir, "devices", deviceID, "entities", "*.json"))
		if err != nil {
			listErr = err
			return
		}
		items = make([]types.Entity, 0, len(files))
		for _, f := range files {
			data, err := os.ReadFile(f)
			if err != nil {
				continue
			}
			s.r.seedHash(f, data)
			var ent types.Entity
			if err := json.Unmarshal(data, &ent); err != nil {
				continue
			}
			if strings.TrimSpace(ent.SourceID) == "" {
				ent.SourceID = ent.ID
			}
			items = append(items, ent)
		}
	})
	return items, listErr
}

func (s *fileStateStore) LoadState(id string) (types.Storage, error) {
	data, err := os.ReadFile(filepath.Join(s.r.dataDir, id+".json"))
	if err != nil {
		return types.Storage{}, err
	}
	var store types.Storage
	if err := json.Unmarshal(data, &store); err != nil {
		return types.Storage{}, err
	}
	return store, nil
}

func (s *fileStateStore) SaveState(id string, store types.Storage) error {
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	return s.r.writeIfChanged(filepath.Join(s.r.dataDir, id+".json"), data)
}

type memoryStateStore struct {
	mu       sync.Mutex
	devices  map[string]types.Device
	entities map[string]map[string]types.Entity
	states   map[string]types.Storage
}

func newMemoryStateStore() *memoryStateStore {
	return &memoryStateStore{
		devices:  make(map[string]types.Device),
		entities: make(map[string]map[string]types.Entity),
		states:   make(map[string]types.Storage),
	}
}

func (s *memoryStateStore) SaveDevice(dev types.Device) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(dev.SourceID) == "" {
		dev.SourceID = dev.ID
	}
	s.devices[dev.ID] = cloneDevice(dev)
	return nil
}

func (s *memoryStateStore) DeleteDevice(id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.devices, id)
	delete(s.entities, id)
	return nil
}

func (s *memoryStateStore) LoadDevices() ([]types.Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.devices))
	for k := range s.devices {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]types.Device, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneDevice(s.devices[k]))
	}
	return out, nil
}

func (s *memoryStateStore) LoadDeviceByID(id string) (types.Device, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	dev, ok := s.devices[id]
	if !ok {
		return types.Device{}, os.ErrNotExist
	}
	return cloneDevice(dev), nil
}

func (s *memoryStateStore) SaveEntity(ent types.Entity) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if strings.TrimSpace(ent.SourceID) == "" {
		ent.SourceID = ent.ID
	}
	if _, ok := s.devices[ent.DeviceID]; !ok {
		s.devices[ent.DeviceID] = types.Device{ID: ent.DeviceID, SourceID: ent.DeviceID}
	}
	if s.entities[ent.DeviceID] == nil {
		s.entities[ent.DeviceID] = make(map[string]types.Entity)
	}
	s.entities[ent.DeviceID][ent.ID] = cloneEntity(ent)
	return nil
}

func (s *memoryStateStore) DeleteEntity(deviceID, entityID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entities[deviceID] != nil {
		delete(s.entities[deviceID], entityID)
	}
	return nil
}

func (s *memoryStateStore) LoadEntity(deviceID, entityID string) (types.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entities[deviceID] == nil {
		return types.Entity{}, os.ErrNotExist
	}
	ent, ok := s.entities[deviceID][entityID]
	if !ok {
		return types.Entity{}, os.ErrNotExist
	}
	return cloneEntity(ent), nil
}

func (s *memoryStateStore) LoadEntities(deviceID string) ([]types.Entity, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.entities[deviceID] == nil {
		return nil, nil
	}
	keys := make([]string, 0, len(s.entities[deviceID]))
	for k := range s.entities[deviceID] {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]types.Entity, 0, len(keys))
	for _, k := range keys {
		out = append(out, cloneEntity(s.entities[deviceID][k]))
	}
	return out, nil
}

func (s *memoryStateStore) LoadState(id string) (types.Storage, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[id]
	if !ok {
		return types.Storage{}, os.ErrNotExist
	}
	return cloneStorage(st), nil
}

func (s *memoryStateStore) SaveState(id string, store types.Storage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.states[id] = cloneStorage(store)
	return nil
}

func cloneDevice(d types.Device) types.Device {
	out := d
	if len(d.Labels) > 0 {
		out.Labels = make(map[string][]string, len(d.Labels))
		for k, v := range d.Labels {
			out.Labels[k] = append([]string(nil), v...)
		}
	}
	return out
}

func cloneEntity(e types.Entity) types.Entity {
	out := e
	if len(e.Actions) > 0 {
		out.Actions = append([]string(nil), e.Actions...)
	}
	if len(e.Labels) > 0 {
		out.Labels = make(map[string][]string, len(e.Labels))
		for k, v := range e.Labels {
			out.Labels[k] = append([]string(nil), v...)
		}
	}
	out.Data.Desired = append([]byte(nil), e.Data.Desired...)
	out.Data.Reported = append([]byte(nil), e.Data.Reported...)
	out.Data.Effective = append([]byte(nil), e.Data.Effective...)
	if len(e.Snapshots) > 0 {
		out.Snapshots = make(map[string]types.EntitySnapshot, len(e.Snapshots))
		for id, s := range e.Snapshots {
			snap := s
			snap.State = append([]byte(nil), s.State...)
			if len(s.Labels) > 0 {
				snap.Labels = make(map[string][]string, len(s.Labels))
				for k, v := range s.Labels {
					snap.Labels[k] = append([]string(nil), v...)
				}
			}
			out.Snapshots[id] = snap
		}
	}
	return out
}

func cloneStorage(s types.Storage) types.Storage {
	return types.Storage{
		Meta: s.Meta,
		Data: append([]byte(nil), s.Data...),
	}
}
