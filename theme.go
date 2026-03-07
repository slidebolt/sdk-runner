package runner

import (
	"fmt"
	"os"
	"strings"
)

// Theme provides shared CLI color and formatting helpers.
type Theme struct {
	enabled bool
}

// NewTheme builds a terminal-aware theme.
func NewTheme() Theme {
	term := strings.TrimSpace(os.Getenv("TERM"))
	noColor := strings.TrimSpace(os.Getenv("NO_COLOR")) != ""
	return Theme{
		enabled: !noColor && term != "" && term != "dumb",
	}
}

func (t Theme) color(code, s string) string {
	if !t.enabled {
		return s
	}
	return "\x1b[" + code + "m" + s + "\x1b[0m"
}

func (t Theme) Title(s string) string  { return t.color("1;36", s) }
func (t Theme) Key(s string) string    { return t.color("1;34", s) }
func (t Theme) Info(s string) string   { return t.color("36", s) }
func (t Theme) Value(s string) string  { return t.color("1;37", s) }
func (t Theme) Warn(s string) string   { return t.color("33", s) }
func (t Theme) Error(s string) string  { return t.color("31", s) }
func (t Theme) Muted(s string) string  { return t.color("2;37", s) }
func (t Theme) Prompt(s string) string { return t.color("1;32", s) }

func (t Theme) Step(n int, area, msg string) string {
	return fmt.Sprintf("%s %s: %s", t.Key(fmt.Sprintf("%d.", n)), t.Title(area), msg)
}
