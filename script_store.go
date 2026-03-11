package runner

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
)

// scriptStore owns all filesystem I/O for Lua scripts and their persisted state.
// It has no knowledge of Lua, NATS, or subscriptions.
type scriptStore struct {
	dataDir string
}

func newScriptStore(dataDir string) *scriptStore {
	return &scriptStore{dataDir: dataDir}
}

func (s *scriptStore) sourcePath(deviceID, entityID string) string {
	return filepath.Join(s.dataDir, "devices", deviceID, "entities", entityID+".lua")
}

func (s *scriptStore) statePath(deviceID, entityID string) string {
	return filepath.Join(s.dataDir, "devices", deviceID, "entities", entityID+".state.lua.json")
}

func (s *scriptStore) readSource(deviceID, entityID string) (string, string, error) {
	path := s.sourcePath(deviceID, entityID)
	data, err := os.ReadFile(path)
	if err != nil {
		return "", path, fmt.Errorf("script not found")
	}
	return string(data), path, nil
}

func (s *scriptStore) writeSource(deviceID, entityID, source string) (string, error) {
	path := s.sourcePath(deviceID, entityID)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return path, err
	}
	return path, os.WriteFile(path, []byte(source), 0o644)
}

func (s *scriptStore) deleteSource(deviceID, entityID string) {
	_ = os.Remove(s.sourcePath(deviceID, entityID))
}

func (s *scriptStore) deleteState(deviceID, entityID string) {
	_ = os.Remove(s.statePath(deviceID, entityID))
}

func (s *scriptStore) loadState(deviceID, entityID string) map[string]any {
	data, err := os.ReadFile(s.statePath(deviceID, entityID))
	if err != nil || len(data) == 0 {
		return map[string]any{}
	}
	var out map[string]any
	if err := json.Unmarshal(data, &out); err != nil || out == nil {
		return map[string]any{}
	}
	return out
}

// saveState persists state to disk. writeIfChanged avoids redundant writes.
func (s *scriptStore) saveState(deviceID, entityID string, state map[string]any, writeIfChanged func(string, []byte) error) {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		log.Printf("plugin-runner: failed to marshal script state: %v", err)
		return
	}
	if err := writeIfChanged(s.statePath(deviceID, entityID), data); err != nil {
		log.Printf("plugin-runner: failed to write script state: %v", err)
	}
}

// statMtime returns path and modification time of the script file.
// Returns an error if the file does not exist.
func (s *scriptStore) statMtime(deviceID, entityID string) (string, int64, error) {
	path := s.sourcePath(deviceID, entityID)
	info, err := os.Stat(path)
	if err != nil {
		return path, 0, err
	}
	return path, info.ModTime().UnixNano(), nil
}

// discoverKeys scans the data directory for all .lua script files.
func (s *scriptStore) discoverKeys() []scriptKey {
	paths, _ := filepath.Glob(filepath.Join(s.dataDir, "devices", "*", "entities", "*.lua"))
	keys := make([]scriptKey, 0, len(paths))
	for _, path := range paths {
		deviceID := filepath.Base(filepath.Dir(filepath.Dir(path)))
		entityID := strings.TrimSuffix(filepath.Base(path), ".lua")
		if deviceID != "" && entityID != "" {
			keys = append(keys, scriptKey{DeviceID: deviceID, EntityID: entityID})
		}
	}
	return keys
}
