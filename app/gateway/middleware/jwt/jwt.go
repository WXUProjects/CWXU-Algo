package jwt

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"os"
	"path"
	"strings"

	config "github.com/go-kratos/gateway/api/gateway/config/v1"
	"github.com/go-kratos/gateway/middleware"
	"github.com/golang-jwt/jwt/v5"
)

func init() {
	middleware.Register("jwt", Middleware)
}

func jwtSecret() ([]byte, error) {
	v := strings.TrimSpace(os.Getenv("CWXU_JWT_SECRET"))
	if len(v) < 32 {
		return nil, errors.New("CWXU_JWT_SECRET must be configured with at least 32 characters")
	}
	return []byte(v), nil
}

// exact public path suffixes (after cleaning)
var publicExact = map[string]struct{}{
	"/v1/user/auth/login":       {},
	"/v1/user/auth/register":    {},
	"/v1/user/auth/send-code":   {},
	"/v1/user/auth/reset-password": {},
	"/v1/user/profile/get-by-id":   {},
	"/v1/user/profile/get-by-name": {},
	"/v1/user/role/list":        {},
	"/v1/user/paste/get":        {},
	"/api/user/paste/get":       {},
	"/v1/user/site/config":      {},
	"/api/user/site/config":     {},
	"/v1/user/site/visit-ping":  {},
	"/api/user/site/visit-ping": {},
	"/v1/core/submit-log/get-by-id": {},
	"/v1/core/contest/list":         {},
	"/v1/core/contest/ranking":      {},
	"/v1/core/statistic/heatmap":    {},
	"/v1/core/statistic/period":     {},
	"/v1/core/statistic/rank":       {},
	"/v1/core/bulletin/get":         {},
	"/v1/core/bulletin/list":        {},
	"/v1/core/emergency/active":     {},
	"/v1/core/problem/list":         {},
	"/v1/core/problem/tags":         {},
	"/v1/core/problem/get":          {},
	"/v1/core/problem/submissions":  {},
	"/v1/core/problem/user-profile": {},
}

// Middleware jwt 校验中间件
func Middleware(c *config.Middleware) (middleware.Middleware, error) {
	return func(next http.RoundTripper) http.RoundTripper {
		return middleware.RoundTripperFunc(func(request *http.Request) (*http.Response, error) {
			uriPath := path.Clean(request.URL.Path)
			_, isPublic := publicExact[uriPath]
			// 静态资源公开
			if strings.HasPrefix(uriPath, "/v1/user/static/") || strings.HasPrefix(uriPath, "/api/user/static/") {
				isPublic = true
			}
			// 健康检查
			if uriPath == "/healthz" || uriPath == "/readyz" {
				isPublic = true
			}

			authHeader := request.Header.Get("Authorization")
			if isPublic && strings.TrimSpace(authHeader) == "" {
				return next.RoundTrip(request)
			}
			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
			if tokenStr == authHeader || tokenStr == "" {
				return buildUnauthorizedResp("JWT Token not found"), nil
			}
			token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
				if token.Method != jwt.SigningMethodHS256 {
					return nil, jwt.ErrSignatureInvalid
				}
				return jwtSecret()
			}, jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}), jwt.WithExpirationRequired())
			if err != nil || !token.Valid {
				return buildUnauthorizedResp("JWT Token invalid"), nil
			}
			return next.RoundTrip(request)
		})
	}, nil
}

func buildUnauthorizedResp(msg string) *http.Response {
	return &http.Response{
		StatusCode: http.StatusUnauthorized,
		Body:       io.NopCloser(bytes.NewBufferString(msg)),
		Header:     make(http.Header),
	}
}
