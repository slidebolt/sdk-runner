package runner

import (
	"context"
	"log/slog"

	regsvc "github.com/slidebolt/registry"
	"github.com/slidebolt/sdk-types"
)

// PluginContext is the dependency-injection container for a plugin.
type PluginContext struct {
	Logger *slog.Logger

	// Active Services provided by the Runner
	Registry  *regsvc.Registry  // Direct registry access: state/devices/entities/scripts/query
	Events    EventService      // Publishes state changes
	Commands  CommandService    // Sends actions to other devices
	Scheduler SchedulerService  // Periodic job scheduling
}

// EventService emits device state changes asynchronously.
type EventService interface {
	PublishEvent(evt types.InboundEvent) error
}

// CommandService allows plugins to control other plugins.
type CommandService interface {
	SendCommand(req types.Command) error
}

// Plugin is the simplified, reaction-only interface.
type Plugin interface {
	// 1. Lifecycle
	// Initialize defines the schema and initial setup.
	Initialize(ctx PluginContext) (types.Manifest, error)

	// Start begins background workers, polling, or network connections.
	Start(ctx context.Context) error

	// Stop gracefully tears down all background activity.
	Stop() error

	// 2. Reactions
	// OnReset is triggered when the user requests a hard wipe of plugin state.
	OnReset() error

	// OnCommand is triggered when the system requests an action on a device.
	OnCommand(req types.Command, entity types.Entity) error
}
