package _const

import (
	"os"
	"strings"
	"sync"
)

const defaultJWTSecret = "CwxuAlgo-JWT"

var (
	jwtSecretOnce sync.Once
	jwtSecret     string
)

// JWTSecret 优先读环境变量 CWXU_JWT_SECRET，未设置时回退默认值（兼容现网）。
func JWTSecret() string {
	jwtSecretOnce.Do(func() {
		if v := strings.TrimSpace(os.Getenv("CWXU_JWT_SECRET")); v != "" {
			jwtSecret = v
			return
		}
		jwtSecret = defaultJWTSecret
	})
	return jwtSecret
}
