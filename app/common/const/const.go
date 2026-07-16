package _const

import (
	"fmt"
	"os"
	"strings"
	"sync"
)

var (
	jwtSecretOnce sync.Once
	jwtSecret     string
)

// JWTSecret returns the deployment JWT secret. Authentication must fail closed:
// silently falling back to a public value would allow anyone to forge tokens.
func JWTSecret() string {
	jwtSecretOnce.Do(func() {
		jwtSecret = strings.TrimSpace(os.Getenv("CWXU_JWT_SECRET"))
		if len(jwtSecret) < 32 {
			panic(fmt.Sprintf("CWXU_JWT_SECRET must be configured with at least 32 characters (got %d)", len(jwtSecret)))
		}
	})
	return jwtSecret
}
