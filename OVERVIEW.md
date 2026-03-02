### `sdk-runner` repository

#### Project Overview

This repository is the heart of the Slidebolt plugin SDK. It provides the `Runner`, a component that plugin developers use to bootstrap their custom plugins and connect them to the wider Slidebolt ecosystem. The runner abstracts away the complexities of messaging, persistence, and lifecycle management.

#### Architecture

The `sdk-runner` is a Go package that provides a framework for running Slidebolt plugins. A plugin developer creates a struct that implements the `runner.Plugin` interface and then passes it to the `Runner` to handle the rest.

The key responsibilities of the runner are:

-   **Plugin Lifecycle**: It manages the lifecycle of a plugin by calling its `OnInitialize`, `OnReady`, `OnHealthCheck`, and `OnShutdown` methods at the appropriate times.

-   **NATS Messaging**: It handles all communication over the NATS message bus. It automatically subscribes to the correct RPC subjects for the plugin and exposes a simple `EventSink` interface for the plugin to emit events.

-   **Persistence**: It provides a simple, file-based persistence layer for plugins. The runner automatically saves and loads device and entity definitions to and from the plugin's dedicated data directory. It also offers a `RawStore` for plugins to save protocol-specific data.

-   **State Reconciliation**: It includes smart reconciliation logic (`ReconcileDevice`) to merge newly discovered device information with existing data, ensuring that user-customized settings (like device names) are preserved.

-   **Lua Scripting Engine**: A powerful feature of the runner is its built-in Lua scripting engine. Users can attach `.lua` scripts to entities to create "virtual devices" or complex automations. These scripts can react to events from other devices, maintain their own state, and send commands, effectively acting as plugins themselves.

#### Key Files

| File | Description |
| :--- | :--- |
| `runner.go` | The main implementation of the plugin runner, which handles the NATS connection, RPC dispatching, and persistence. |
| `interface.go` | Defines the crucial `Plugin` interface that every plugin must implement. |
| `script_engine.go` | Implements the embedded Lua scripting engine, allowing for user-created virtual devices and automations. |
| `store.go` | Provides a simple file-based persistence layer for plugin data. |
| `reconcile.go` | Contains the logic for intelligently merging discovered device state with existing, user-configured state. |
| `constants.go`| Defines the shared NATS subjects and environment variables that form the basis of the plugin communication protocol. |

#### Usage

This is a library package and is not intended to be run directly. Plugin developers import it and use the following pattern in their `main.go` to start their plugin:

```go
func main() {
    // MyPlugin implements the runner.Plugin interface
    if err := runner.NewRunner(&MyPlugin{}).Run(); err != nil {
        log.Fatal(err)
    }
}
```
