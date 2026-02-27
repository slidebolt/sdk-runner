package runner

import (
	"fmt"
	"os"
)

// MustGetEnv returns the value of an environment variable or panics.
// Used during plugin startup where a missing variable is a fatal misconfiguration.
func MustGetEnv(key string) string {
	v := os.Getenv(key)
	if v == "" {
		panic(fmt.Sprintf("FATAL: required environment variable %s is not set", key))
	}
	return v
}
