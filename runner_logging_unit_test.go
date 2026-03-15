package runner

import (
	"bytes"
	"strings"
	"testing"
)

func TestRunnerSetAndGetLogLevel(t *testing.T) {
	var buf bytes.Buffer
	logger, levelVar := newLoggerWithWriter("plugin-esphome", &buf)

	r := &Runner{
		logger:   logger,
		logLevel: levelVar,
	}

	if got := r.LogLevel(); got != "info" {
		t.Fatalf("default level got=%q want=info", got)
	}

	lvl, err := r.SetLogLevel("trace")
	if err != nil {
		t.Fatalf("SetLogLevel(trace) err=%v", err)
	}
	if lvl != "trace" {
		t.Fatalf("SetLogLevel returned %q want trace", lvl)
	}
	if got := r.LogLevel(); got != "trace" {
		t.Fatalf("LogLevel got=%q want=trace", got)
	}

	logger.Debug("debug-visible")
	if !strings.Contains(buf.String(), "debug-visible") {
		t.Fatalf("expected debug log after trace level update, output=%q", buf.String())
	}
}

func TestRunnerSetLogLevelRejectsInvalid(t *testing.T) {
	r := &Runner{}
	if _, err := r.SetLogLevel("chatty"); err == nil {
		t.Fatal("expected invalid log level error")
	}
}

func TestRunnerLogLevelIsolationPerInstance(t *testing.T) {
	r1 := &Runner{}
	r2 := &Runner{}
	if _, err := r1.SetLogLevel("trace"); err != nil {
		t.Fatalf("r1 SetLogLevel err=%v", err)
	}
	if got := r1.LogLevel(); got != "trace" {
		t.Fatalf("r1 level=%q want=trace", got)
	}
	if got := r2.LogLevel(); got != "info" {
		t.Fatalf("r2 level=%q want=info", got)
	}
}
