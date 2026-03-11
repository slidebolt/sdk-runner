package runner

import (
	"encoding/json"
	"fmt"
	"time"

	registry "github.com/slidebolt/registry"
	"github.com/slidebolt/sdk-types"
)

// FindEntities queries the gateway/registry cache using the universal
// registry query format.
func (r *Runner) FindEntities(q types.SearchQuery) ([]RegistryEntity, error) {
	if r == nil || r.nc == nil {
		return nil, fmt.Errorf("nats not connected")
	}
	req := registry.Query{
		Pattern:  q.Pattern,
		Labels:   q.Labels,
		PluginID: q.PluginID,
		DeviceID: q.DeviceID,
		EntityID: q.EntityID,
		Domain:   q.Domain,
		Limit:    q.Limit,
	}
	data, err := json.Marshal(req)
	if err != nil {
		return nil, err
	}
	msg, err := r.nc.Request(SubjectRegistryQueryEntities, data, 2*time.Second)
	if err != nil {
		return nil, err
	}
	var regOut []registry.EntityWithPlugin
	if err := json.Unmarshal(msg.Data, &regOut); err != nil {
		return nil, err
	}
	out := make([]RegistryEntity, 0, len(regOut))
	for _, item := range regOut {
		out = append(out, RegistryEntity{Entity: item.Entity, PluginID: item.PluginID})
	}
	return out, nil
}
