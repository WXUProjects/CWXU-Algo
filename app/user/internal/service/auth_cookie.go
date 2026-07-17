package service

import (
	"context"
	"net/http"
	"time"

	commonauth "cwxu-algo/app/common/utils/auth"

	"github.com/go-kratos/kratos/v2/transport"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
)

func setSessionCookie(ctx context.Context, token string) {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return
	}
	cookie := (&http.Cookie{
		Name: commonauth.SessionCookieName, Value: token, Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode,
		MaxAge: int(JWTAccessTTL.Seconds()), Expires: time.Now().Add(JWTAccessTTL),
	}).String()
	tr.ReplyHeader().Add("Set-Cookie", cookie)
}

func clearSessionCookie(ctx context.Context) {
	tr, ok := transport.FromServerContext(ctx)
	if !ok {
		return
	}
	cookie := (&http.Cookie{
		Name: commonauth.SessionCookieName, Value: "", Path: "/",
		HttpOnly: true, Secure: true, SameSite: http.SameSiteStrictMode,
		MaxAge: -1, Expires: time.Unix(1, 0),
	}).String()
	tr.ReplyHeader().Add("Set-Cookie", cookie)
}

func RegisterAuthSessionRoutes(srv *khttp.Server) {
	srv.Route("/").POST("/v1/user/auth/logout", func(ctx khttp.Context) error {
		clearSessionCookie(ctx)
		return ctx.JSON(http.StatusOK, map[string]interface{}{
			"code": 0, "success": true, "message": "已退出登录",
		})
	})
}
