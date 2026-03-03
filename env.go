package runner

import (
	"fmt"
	"os"
)

// RequireEnv returns the value of an environment variable or an error if unset.
func RequireEnv(key string) (string, error) {
	v := os.Getenv(key)
	if v == "" {
		return "", fmt.Errorf("required environment variable %s is not set", key)
	}
	return v, nil
}
