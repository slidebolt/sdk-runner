package runner

// Environment variable keys passed to every plugin process by the launcher.
const (
	EnvAPIPort      = "API_PORT"
	EnvAPIHost      = "API_HOST"
	EnvNATSURL      = "NATS_URL"
	EnvPluginRPCSbj = "PLUGIN_RPC_SUBJECT"
	EnvPluginData   = "PLUGIN_DATA_DIR"
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
)
