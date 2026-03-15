package runner

import (
	"bytes"
	"context"
	"log/slog"
	"os"
	"strings"
	"testing"
)

func TestParseLogLevelTrace(t *testing.T) {
	if got := parseLogLevel("trace"); got != LevelTrace {
		t.Fatalf("parseLogLevel(trace)=%v want=%v", got, LevelTrace)
	}
	if got := formatLogLevel(LevelTrace); got != "trace" {
		t.Fatalf("formatLogLevel(LevelTrace)=%q want=trace", got)
	}
}

func TestResolveLogLevelPrecedence(t *testing.T) {
	t.Setenv("PLUGIN_PLUGIN_ESPHOME_LOG_LEVEL", "debug")
	t.Setenv(EnvPluginLogLevel, "error")
	t.Setenv(EnvLogLevel, "warn")
	if got := resolveLogLevel("plugin-esphome"); got != slog.LevelDebug {
		t.Fatalf("resolveLogLevel precedence got=%v want=%v", got, slog.LevelDebug)
	}
}

func TestLoggerFiltersByLevelAndSupportsRuntimeUpdate(t *testing.T) {
	var buf bytes.Buffer
	logger, levelVar := newLoggerWithWriter("plugin-esphome", &buf)
	levelVar.Set(slog.LevelInfo)

	logger.Debug("debug-before")
	logger.Info("info-before")
	logger.Log(context.Background(), LevelTrace, "trace-before")
	out := buf.String()
	if strings.Contains(out, "debug-before") || strings.Contains(out, "trace-before") {
		t.Fatalf("unexpected low-level entries at info level: %q", out)
	}
	if !strings.Contains(out, "info-before") {
		t.Fatalf("missing info entry: %q", out)
	}

	levelVar.Set(LevelTrace)
	logger.Debug("debug-after")
	logger.Log(context.Background(), LevelTrace, "trace-after")
	out = buf.String()
	if !strings.Contains(out, "debug-after") || !strings.Contains(out, "trace-after") {
		t.Fatalf("expected debug+trace after runtime update, output=%q", out)
	}
}

func TestValidateLogLevelRejectsInvalid(t *testing.T) {
	if _, err := validateLogLevel("nope"); err == nil {
		t.Fatal("expected error for invalid level")
	}
}

func TestDefaultLogServiceName(t *testing.T) {
	name := defaultLogServiceName()
	if strings.TrimSpace(name) == "" {
		t.Fatal("default log service name should not be empty")
	}
	if strings.Contains(name, string(os.PathSeparator)) {
		t.Fatalf("service name should be basename only: %q", name)
	}
}
