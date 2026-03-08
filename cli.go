package runner

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/slidebolt/sdk-types"
)

// RunCLI provides a shared plugin entrypoint with launcher-like commands:
// serve (default), discover, debug-ui, and help.
func RunCLI(newPlugin func() Plugin) error {
	if newPlugin == nil {
		return fmt.Errorf("plugin factory is required")
	}
	command, args := parseCLICommand(os.Args[1:])
	switch command {
	case "help":
		printCLIUsage()
		return nil
	case "serve":
		return runServe(newPlugin, args)
	case "discover":
		if len(args) > 0 {
			return fmt.Errorf("discover does not accept extra arguments")
		}
		r, err := newRunnerForCLI(newPlugin)
		if err != nil {
			return err
		}
		return r.RunDiscovery()
	case "debug-ui":
		if len(args) > 0 {
			return fmt.Errorf("debug-ui does not accept extra arguments")
		}
		r, err := newRunnerForCLI(newPlugin)
		if err != nil {
			return err
		}
		return r.RunDebugUI()
	default:
		printCLIUsage()
		return fmt.Errorf("unknown command: %s", command)
	}
}

func runServe(newPlugin func() Plugin, args []string) error {
	r, err := newRunnerForCLI(newPlugin)
	if err != nil {
		return err
	}
	originalArgs := os.Args
	os.Args = append([]string{originalArgs[0]}, args...)
	defer func() { os.Args = originalArgs }()
	return r.Run()
}

func parseCLICommand(args []string) (string, []string) {
	if len(args) == 0 {
		return "serve", nil
	}
	first := strings.ToLower(strings.TrimSpace(args[0]))
	switch first {
	case "serve", "up":
		return "serve", args[1:]
	case "discover", "--discover":
		return "discover", args[1:]
	case "debug-ui", "debug", "ui":
		return "debug-ui", args[1:]
	case "help", "-h", "--help":
		return "help", args[1:]
	default:
		return first, args[1:]
	}
}

func printCLIUsage() {
	ui := newCLITheme()
	fmt.Println(ui.Title("Plugin Runner"))
	fmt.Println("Usage: plugin [serve|discover|debug-ui|help]")
	fmt.Printf("  %-8s %s\n", ui.Key("serve"), "Run plugin with NATS RPC lifecycle (default).")
	fmt.Printf("  %-8s %s\n", ui.Key("discover"), "Run one device discovery cycle and exit.")
	fmt.Printf("  %-8s %s\n", ui.Key("debug-ui"), "Run plugin standalone interactive debug shell (no NATS required).")
}

func newRunnerForCLI(newPlugin func() Plugin) (*Runner, error) {
	ensureDefaultDataDir()
	return NewRunner(newPlugin())
}

func ensureDefaultDataDir() {
	if strings.TrimSpace(os.Getenv(EnvPluginData)) != "" {
		return
	}
	exe, err := os.Executable()
	name := "plugin"
	if err == nil {
		base := filepath.Base(exe)
		if base != "" {
			name = sanitizeName(base)
		}
	}
	_ = os.Setenv(EnvPluginData, filepath.Join(".build", "data", name))
}

func sanitizeName(s string) string {
	var b strings.Builder
	for _, r := range s {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || r == '-' || r == '_' {
			b.WriteRune(r)
		}
	}
	out := b.String()
	if out == "" {
		return "plugin"
	}
	return out
}

// RunDiscovery runs a single discovery cycle and exits.
func (r *Runner) RunDiscovery() error {
	return r.runDiscoveryMode()
}

// RunDebugUI boots the plugin and exposes a local interactive shell that does
// not require gateway/NATS connectivity.
func (r *Runner) RunDebugUI() error {
	ui := newCLITheme()
	_ = os.MkdirAll(r.dataDir, 0o755)
	initialState := r.loadState("default")
	var updatedState types.Storage
	fmt.Println(ui.Step(1, "Plugin", "Initializing..."))
	r.manifest, updatedState = r.plugin.OnInitialize(Config{DataDir: r.dataDir, EventSink: r, RawStore: r, Logger: r.logger}, initialState)
	r.saveStateSynced("default", updatedState)
	r.plugin.OnReady()
	defer r.plugin.OnShutdown()

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 30*time.Second)
	if err := r.plugin.WaitReady(waitCtx); err != nil {
		log.Printf("plugin-runner: WaitReady failed in debug mode: %v; proceeding anyway", err)
	}
	waitCancel()

	fmt.Println(ui.Step(2, "Discovery", "Refreshing devices/entities..."))
	if _, err := r.refreshDevices(); err != nil {
		log.Printf("%s initial refresh failed in debug mode: %v", ui.Warn("plugin-runner:"), err)
	}

	fmt.Println(ui.Step(3, "Debug UI", "Ready"))
	fmt.Printf("%s %s (%s)\n", ui.Info("plugin"), ui.Value(r.manifest.ID), ui.Value(r.manifest.Name))
	fmt.Printf("%s %s\n", ui.Info("data"), ui.Value(r.dataDir))
	fmt.Println(ui.Muted("commands: help, status, devices, entities <device_id>, refresh [device_id], command <device_id> <entity_id> <json>, event <device_id> <entity_id> <json> [correlation_id], quit"))

	sc := bufio.NewScanner(os.Stdin)
	for {
		fmt.Print(ui.Prompt("> "))
		if !sc.Scan() {
			return sc.Err()
		}
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		if err := r.handleDebugCommand(line); err != nil {
			if err == errDebugExit {
				return nil
			}
			fmt.Printf("%s %v\n", ui.Error("error:"), err)
		}
	}
}

var errDebugExit = fmt.Errorf("debug-exit")

func (r *Runner) handleDebugCommand(line string) error {
	ui := newCLITheme()
	cmd, rest := splitHead(line)
	switch cmd {
	case "help":
		fmt.Println(ui.Title("Commands"))
		fmt.Println("help")
		fmt.Println("status")
		fmt.Println("devices")
		fmt.Println("entities <device_id>")
		fmt.Println("refresh [device_id]")
		fmt.Println("command <device_id> <entity_id> <json>")
		fmt.Println("event <device_id> <entity_id> <json> [correlation_id]")
		fmt.Println("quit")
		return nil
	case "status":
		health, err := r.plugin.OnHealthCheck()
		if err != nil {
			return err
		}
		fmt.Printf("%s %s\n", ui.Info("health:"), ui.Value(health))
		return nil
	case "devices":
		data, _ := json.MarshalIndent(r.loadDevices(), "", "  ")
		fmt.Println(string(data))
		return nil
	case "entities":
		deviceID := strings.TrimSpace(rest)
		if deviceID == "" {
			return fmt.Errorf("usage: entities <device_id>")
		}
		data, _ := json.MarshalIndent(r.loadEntities(deviceID), "", "  ")
		fmt.Println(string(data))
		return nil
	case "refresh":
		deviceID := strings.TrimSpace(rest)
		if deviceID == "" {
			devices, err := r.refreshDevices()
			if err != nil {
				return err
			}
			fmt.Printf("%s %d\n", ui.Info("refreshed devices:"), len(devices))
			return nil
		}
		entities, err := r.refreshEntities(deviceID)
		if err != nil {
			return err
		}
		fmt.Printf("%s %d\n", ui.Info("refreshed entities:"), len(entities))
		return nil
	case "command":
		deviceID, tail, ok := cutWord(rest)
		if !ok {
			return fmt.Errorf("usage: command <device_id> <entity_id> <json>")
		}
		entityID, payloadText, ok := cutWord(tail)
		if !ok {
			return fmt.Errorf("usage: command <device_id> <entity_id> <json>")
		}
		payload := json.RawMessage(strings.TrimSpace(payloadText))
		if !json.Valid(payload) {
			return fmt.Errorf("command payload must be valid JSON")
		}
		status, err := r.createCommand(deviceID, entityID, payload)
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(status, "", "  ")
		fmt.Println(string(data))
		return nil
	case "event":
		deviceID, tail, ok := cutWord(rest)
		if !ok {
			return fmt.Errorf("usage: event <device_id> <entity_id> <json> [correlation_id]")
		}
		entityID, payloadAndTail, ok := cutWord(tail)
		if !ok {
			return fmt.Errorf("usage: event <device_id> <entity_id> <json> [correlation_id]")
		}
		payloadText, correlationID := splitJSONAndOptionalTail(payloadAndTail)
		payload := json.RawMessage(strings.TrimSpace(payloadText))
		if !json.Valid(payload) {
			return fmt.Errorf("event payload must be valid JSON")
		}
		updated, err := r.processInboundEvent(types.InboundEvent{
			DeviceID:      deviceID,
			EntityID:      entityID,
			CorrelationID: strings.TrimSpace(correlationID),
			Payload:       payload,
		})
		if err != nil {
			return err
		}
		data, _ := json.MarshalIndent(updated, "", "  ")
		fmt.Println(string(data))
		return nil
	case "quit", "exit":
		return errDebugExit
	default:
		return fmt.Errorf("unknown command `%s` (run `help`)", cmd)
	}
}

func splitHead(s string) (string, string) {
	s = strings.TrimSpace(s)
	if s == "" {
		return "", ""
	}
	i := strings.IndexAny(s, " \t")
	if i == -1 {
		return s, ""
	}
	return strings.TrimSpace(s[:i]), strings.TrimSpace(s[i+1:])
}

func cutWord(s string) (string, string, bool) {
	head, rest := splitHead(s)
	if head == "" {
		return "", "", false
	}
	return head, rest, true
}

func splitJSONAndOptionalTail(s string) (string, string) {
	text := strings.TrimSpace(s)
	if text == "" {
		return "", ""
	}
	if text[0] != '{' && text[0] != '[' {
		return text, ""
	}
	var open, close byte
	if text[0] == '{' {
		open, close = '{', '}'
	} else {
		open, close = '[', ']'
	}
	depth := 0
	inString := false
	escaped := false
	for i := 0; i < len(text); i++ {
		ch := text[i]
		if inString {
			if escaped {
				escaped = false
				continue
			}
			if ch == '\\' {
				escaped = true
				continue
			}
			if ch == '"' {
				inString = false
			}
			continue
		}
		if ch == '"' {
			inString = true
			continue
		}
		if ch == open {
			depth++
		} else if ch == close {
			depth--
			if depth == 0 {
				return strings.TrimSpace(text[:i+1]), strings.TrimSpace(text[i+1:])
			}
		}
	}
	return text, ""
}

func newCLITheme() Theme { return NewTheme() }
