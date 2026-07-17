package auth

import (
	"context"
	"net/http"
	"strings"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
)

const SessionCookieName = "goalgo_session"

// CookieBearer lets browser clients use an HttpOnly session cookie while the
// existing services continue to consume the standard Authorization header.
// Also accepts access_token query (for native browser file downloads that cannot set Authorization).
func CookieBearer() middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			if tr, ok := transport.FromServerContext(ctx); ok && tr.RequestHeader().Get("Authorization") == "" {
				r := &http.Request{Header: http.Header{"Cookie": tr.RequestHeader().Values("Cookie")}}
				if cookie, err := r.Cookie(SessionCookieName); err == nil && strings.TrimSpace(cookie.Value) != "" {
					tr.RequestHeader().Set("Authorization", "Bearer "+cookie.Value)
				} else if ht, ok := tr.(*khttp.Transport); ok && ht.Request() != nil {
					if t := strings.TrimSpace(ht.Request().URL.Query().Get("access_token")); t != "" {
						tr.RequestHeader().Set("Authorization", "Bearer "+t)
					}
				}
			}
			return handler(ctx, req)
		}
	}
}
