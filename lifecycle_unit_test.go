package runner

import (
	"context"
	"testing"

	"github.com/slidebolt/sdk-types"
)

type mockPlugin struct {
	stopCalled bool
}

func (m *mockPlugin) Initialize(ctx PluginContext) (types.Manifest, error) {
	return types.Manifest{ID: "test-plugin"}, nil
}
func (m *mockPlugin) Start(ctx context.Context) error { return nil }
func (m *mockPlugin) Stop() error {
	m.stopCalled = true
	return nil
}
func (m *mockPlugin) OnReset() error                                  { return nil }
func (m *mockPlugin) OnCommand(cmd types.Command, ent types.Entity) error { return nil }

func TestRunner_MockSetup(t *testing.T) {
	// Setup environment for NewRunner
	t.Setenv(types.EnvPluginDataDir, t.TempDir())
	t.Setenv(types.EnvNATSURL, "nats://127.0.0.1:4222")

	p := &mockPlugin{}
	_, err := NewRunner(p)
	if err != nil {
		t.Fatalf("failed to create runner: %v", err)
	}
}
