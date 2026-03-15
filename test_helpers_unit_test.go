package runner

import (
	"testing"

	regsvc "github.com/slidebolt/registry"
	types "github.com/slidebolt/sdk-types"
)

func newTestRunner(t *testing.T) *Runner {
	t.Helper()
	return &Runner{
		dataDir:  t.TempDir(),
		statuses: make(map[string]types.CommandStatus),
		reg:      regsvc.RegistryService("runner-test", regsvc.WithPersist(regsvc.PersistNever)),
	}
}
