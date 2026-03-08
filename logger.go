package runner

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"unicode"
)

const (
	EnvPluginLogLevel = "PLUGIN_LOG_LEVEL"
	EnvLogLevel       = "LOG_LEVEL"
	LevelTrace        = slog.LevelDebug - 4
)

func newLogger(service string) (*slog.Logger, *slog.LevelVar) {
	return newLoggerWithWriter(service, os.Stdout)
}

func newLoggerWithWriter(service string, w io.Writer) (*slog.Logger, *slog.LevelVar) {
	level := resolveLogLevel(service)
	levelVar := &slog.LevelVar{}
	levelVar.Set(level)
	handler := slog.NewTextHandler(w, &slog.HandlerOptions{
		Level: levelVar,
	})
	return slog.New(handler).With("service", service), levelVar
}

func resolveLogLevel(service string) slog.Level {
	keys := logLevelKeys(service)
	for _, key := range keys {
		if val := strings.TrimSpace(os.Getenv(key)); val != "" {
			return parseLogLevel(val)
		}
	}
	return slog.LevelInfo
}

func logLevelKeys(service string) []string {
	keys := make([]string, 0, 4)
	if normalized := normalizeLogKey(service); normalized != "" {
		keys = append(keys, "PLUGIN_"+normalized+"_LOG_LEVEL")
	}
	keys = append(keys, EnvPluginLogLevel, EnvLogLevel)
	return keys
}

func parseLogLevel(value string) slog.Level {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "trace":
		return LevelTrace
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	case "info", "":
		return slog.LevelInfo
	default:
		return slog.LevelInfo
	}
}

func formatLogLevel(level slog.Level) string {
	switch {
	case level <= LevelTrace:
		return "trace"
	case level <= slog.LevelDebug:
		return "debug"
	case level <= slog.LevelInfo:
		return "info"
	case level <= slog.LevelWarn:
		return "warn"
	default:
		return "error"
	}
}

func validateLogLevel(value string) (slog.Level, error) {
	level := strings.ToLower(strings.TrimSpace(value))
	switch level {
	case "trace", "debug", "info", "warn", "warning", "error":
		return parseLogLevel(level), nil
	default:
		return slog.LevelInfo, fmt.Errorf("invalid log level %q (expected trace|debug|info|warn|error)", value)
	}
}

func defaultLogServiceName() string {
	exe, err := os.Executable()
	if err != nil {
		return "plugin"
	}
	base := filepath.Base(exe)
	base = strings.TrimSpace(base)
	if base == "" {
		return "plugin"
	}
	return strings.ToLower(base)
}

func normalizeLogKey(service string) string {
	s := strings.TrimSpace(strings.ToUpper(service))
	var b strings.Builder
	lastUnderscore := false
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) {
			b.WriteRune(r)
			lastUnderscore = false
			continue
		}
		if !lastUnderscore {
			b.WriteByte('_')
			lastUnderscore = true
		}
	}
	return strings.Trim(b.String(), "_")
}
