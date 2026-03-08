package runner

// Environment variable keys read by the runner.
const (
	EnvNATSURL    = "NATS_URL"
	EnvPluginData = "PLUGIN_DATA_DIR"
	// EnvStateSnapshotIntervalSec controls background snapshot cadence from in-memory canonical state to file snapshots.
	// Default is 30 seconds.
	EnvStateSnapshotIntervalSec = "STATE_SNAPSHOT_INTERVAL_SEC"
)

// Environment variable keys set by the launcher for the gateway.
// Not consumed by the runner itself.
const (
	EnvAPIPort      = "API_PORT"
	EnvAPIHost      = "API_HOST"
	EnvPluginRPCSbj = "PLUGIN_RPC_SUBJECT"
	EnvRuntimeFile  = "RUNTIME_FILE"
)

// Internal gateway API paths.
const (
	HealthEndpoint = "/_internal/health"
)

// NATS subjects.
const (
	SubjectRPCPrefix      = "slidebolt.rpc."
	SubjectRegistration   = "slidebolt.registration"
	SubjectDiscoveryProbe = "slidebolt.discovery.probe"
	SubjectSearchDevices  = "slidebolt.search.devices"
	SubjectSearchEntities = "slidebolt.search.entities"
	SubjectSearchPlugins  = "slidebolt.search.plugins"
	SubjectEntityEvents   = "slidebolt.entity.events"
	SubjectCommandStatus  = "slidebolt.command.status"
	SubjectGatewayDiscovery = "slidebolt.gateway.discovery"
)
