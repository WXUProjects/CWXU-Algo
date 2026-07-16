package _const

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

var (
	jwtSecretMu sync.RWMutex
	jwtSecret   string
)

// ConfigureJWTSecret sets the config.yaml value. A non-empty environment
// variable takes precedence so orchestrators can still inject secrets safely.
func ConfigureJWTSecret(value string) error {
	if env := strings.TrimSpace(os.Getenv("CWXU_JWT_SECRET")); env != "" {
		value = env
	}
	value = strings.TrimSpace(value)
	if len(value) < 32 {
		return fmt.Errorf("server.jwt_secret must contain at least 32 characters (got %d)", len(value))
	}
	jwtSecretMu.Lock()
	jwtSecret = value
	jwtSecretMu.Unlock()
	return nil
}

// JWTSecret returns the deployment JWT secret. Authentication must fail closed:
// silently falling back to a public value would allow anyone to forge tokens.
func JWTSecret() string {
	jwtSecretMu.RLock()
	value := jwtSecret
	jwtSecretMu.RUnlock()
	if value == "" {
		value = strings.TrimSpace(os.Getenv("CWXU_JWT_SECRET"))
	}
	if len(value) < 32 {
		panic(fmt.Sprintf("server.jwt_secret must be configured with at least 32 characters (got %d)", len(value)))
	}
	return value
}
