package runner

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/slidebolt/sdk-types"
)

// Store is the plugin-scoped persistence layer.
// It manages translation between Go objects and local JSON files,
// sandboxed to the plugin's own data directory.
type Store struct {
	dataDir string
}

func NewStore(dataDir string) *Store {
	return &Store{dataDir: dataDir}
}

func (s *Store) Get(id string) (types.Storage, error) {
	data, err := os.ReadFile(s.resolvePath(id))
	if err != nil {
		return types.Storage{}, fmt.Errorf("storage not found: %w", err)
	}
	var store types.Storage
	if err := json.Unmarshal(data, &store); err != nil {
		return types.Storage{}, fmt.Errorf("failed to decode storage: %w", err)
	}
	return store, nil
}

func (s *Store) Put(id string, store types.Storage) error {
	if err := os.MkdirAll(s.dataDir, 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(store, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(s.resolvePath(id), data, 0o644)
}

func (s *Store) Delete(id string) error {
	return os.Remove(s.resolvePath(id))
}

// resolvePath prevents path traversal by stripping directory components from id.
func (s *Store) resolvePath(id string) string {
	return filepath.Join(s.dataDir, filepath.Base(id)+".json")
}
