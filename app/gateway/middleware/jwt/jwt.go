package jwt

import (
	"bytes"
	"io"
	"net/http"
	"strings"

	config "github.com/go-kratos/gateway/api/gateway/config/v1"
	"github.com/go-kratos/gateway/middleware"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/golang-jwt/jwt/v5"
)

func init() {
	middleware.Register("jwt", Middleware)
}

// Middleware jwt 校验中间件
func Middleware(c *config.Middleware) (middleware.Middleware, error) {
	return func(next http.RoundTripper) http.RoundTripper {
		return middleware.RoundTripperFunc(func(request *http.Request) (*http.Response, error) {

			if strings.Contains(request.RequestURI, "login") || strings.Contains(request.RequestURI, "register") {
				// 如果是登陆 则放行
				return next.RoundTrip(request)
			}
			authHeader := request.Header.Get("Authorization")
			tokenStr := strings.TrimPrefix(authHeader, "Bearer ")
			if tokenStr == authHeader {
				return buildUnauthorizedResp("JWT Token not found"), nil
			}
			log.Info(tokenStr)
			token, err := jwt.Parse(tokenStr, func(token *jwt.Token) (interface{}, error) {
				return []byte("CwxuAlgo-JWT"), nil
			})
			if err != nil || !token.Valid {
				return buildUnauthorizedResp("JWT Token invalid"), nil
			}
			return next.RoundTrip(request)
		})
	}, nil

}

func buildUnauthorizedResp(msg string) *http.Response {
	return &http.Response{StatusCode: http.StatusUnauthorized, Body: io.NopCloser(bytes.NewBufferString(msg))}
}
