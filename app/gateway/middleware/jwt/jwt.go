package jwt

import (
	"bytes"
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

func jwtSecret() []byte {
	if v := strings.TrimSpace(os.Getenv("CWXU_JWT_SECRET")); v != "" {
		return []byte(v)
	}
	return []byte("CwxuAlgo-JWT")
}

// exact public path suffixes (after cleaning)
var publicExact = map[string]struct{}{
	"/v1/user/auth/login":    {},
	"/v1/user/auth/register": {},
	"/v1/user/role/list":     {},
}

// Middleware jwt 校验中间件
func Middleware(c *config.Middleware) (middleware.Middleware, error) {
	return func(next http.RoundTripper) http.RoundTripper {
		return middleware.RoundTripperFunc(func(request *http.Request) (*http.Response, error) {
			uriPath := path.Clean(request.URL.Path)
			if _, ok := publicExact[uriPath]; ok {
				return next.RoundTrip(request)
			}
			// 静态资源公开
			if strings.HasPrefix(uriPath, "/v1/user/static/") || strings.HasPrefix(uriPath, "/api/user/static/") {
				return next.RoundTrip(request)
			}
			// 健康检查
			if uriPath == "/healthz" || uriPath == "/readyz" {
				return next.RoundTrip(request)
			}

			authHeader := request.Header.Get("Authorization")
			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
			if tokenStr == authHeader || tokenStr == "" {
				return buildUnauthorizedResp("JWT Token not found"), nil
			}
			token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
				if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
					return nil, jwt.ErrSignatureInvalid
				}
				return jwtSecret(), nil
			})
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
