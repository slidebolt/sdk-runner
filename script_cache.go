package runner

import "sync"

// scriptRuntimeCache manages the lifecycle of Lua VM instances.
// It handles mtime-based hot-reload detection, lazy loading, and stale-entry cleanup.
type scriptRuntimeCache struct {
	mu      sync.Mutex
	entries map[scriptKey]*scriptRuntime
	store   *scriptStore
	factory func(key scriptKey, path string, mtime int64) (*scriptRuntime, error)
}

func newScriptRuntimeCache(store *scriptStore, factory func(key scriptKey, path string, mtime int64) (*scriptRuntime, error)) *scriptRuntimeCache {
	return &scriptRuntimeCache{
		entries: make(map[scriptKey]*scriptRuntime),
		store:   store,
		factory: factory,
	}
}

// ensure returns the runtime for the given entity, loading or hot-reloading as needed.
// Returns (nil, nil) if no script file exists for the entity.
func (c *scriptRuntimeCache) ensure(deviceID, entityID string) (*scriptRuntime, error) {
	key := scriptKey{DeviceID: deviceID, EntityID: entityID}

	c.mu.Lock()
	defer c.mu.Unlock()

	path, mtime, err := c.store.statMtime(deviceID, entityID)
	if err != nil {
		// File gone — close and evict any existing runtime.
		if rt, ok := c.entries[key]; ok {
			rt.mu.Lock()
			rt.L.Close()
			rt.closed = true
			rt.mu.Unlock()
			delete(c.entries, key)
		}
		return nil, nil
	}

	if rt, ok := c.entries[key]; ok && rt.mtimeUnix == mtime {
		return rt, nil
	}

	// Close stale entry before loading fresh.
	if rt, ok := c.entries[key]; ok {
		rt.mu.Lock()
		rt.L.Close()
		rt.closed = true
		rt.mu.Unlock()
		delete(c.entries, key)
	}

	rt, err := c.factory(key, path, mtime)
	if err != nil {
		return nil, err
	}
	c.entries[key] = rt
	return rt, nil
}

// invalidate closes and removes the runtime for the given entity.
func (c *scriptRuntimeCache) invalidate(deviceID, entityID string) {
	key := scriptKey{DeviceID: deviceID, EntityID: entityID}
	c.mu.Lock()
	defer c.mu.Unlock()
	if rt, ok := c.entries[key]; ok {
		rt.mu.Lock()
		rt.L.Close()
		rt.closed = true
		rt.mu.Unlock()
		delete(c.entries, key)
	}
}

// sync scans disk for .lua files, ensures runtimes for all found scripts,
// and removes runtimes whose files have been deleted.
func (c *scriptRuntimeCache) sync() {
	keys := c.store.discoverKeys()
	seen := make(map[scriptKey]struct{}, len(keys))
	for _, key := range keys {
		seen[key] = struct{}{}
		_, _ = c.ensure(key.DeviceID, key.EntityID)
	}
	// Collect stale keys without holding the lock during invalidation.
	c.mu.Lock()
	stale := make([]scriptKey, 0)
	for key := range c.entries {
		if _, ok := seen[key]; !ok {
			stale = append(stale, key)
		}
	}
	c.mu.Unlock()
	for _, key := range stale {
		c.invalidate(key.DeviceID, key.EntityID)
	}
}

// snapshot returns a point-in-time slice of all active runtimes.
func (c *scriptRuntimeCache) snapshot() []*scriptRuntime {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]*scriptRuntime, 0, len(c.entries))
	for _, rt := range c.entries {
		out = append(out, rt)
	}
	return out
}
