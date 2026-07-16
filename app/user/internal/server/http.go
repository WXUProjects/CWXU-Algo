package server

import (
	"context"
	"cwxu-algo/api/user/v1/auth"
	"cwxu-algo/api/user/v1/group"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/api/user/v1/role"
	"cwxu-algo/api/user/v1/site"
	"cwxu-algo/app/common/conf"
	_const "cwxu-algo/app/common/const"
	"cwxu-algo/app/common/opsmetrics"
	authutil "cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/common/utils/health"
	"cwxu-algo/app/common/utils/safeerrors"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/service"
	"strings"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/auth/jwt"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/selector"
	"github.com/go-kratos/kratos/v2/transport/http"
	jwt2 "github.com/golang-jwt/jwt/v5"
)

func NewWhiteListMatcher() selector.MatchFunc {
	whiteList := map[string]string{
		"/api.user.v1.Auth/Login":                    "",
		"/api.user.v1.Auth/Register":                 "",
		"/api.user.v1.Auth/SendCode":                 "",
		"/api.user.v1.Auth/ResetPassword":            "",
		"/api.user.v1.Profile/GetById":               "",
		"/api.user.v1.Profile/GetByUsername":         "",
		"/api.user.v1.Profile/GetByName":             "",
		"/api.user.v1.Profile/GetFollowingIds":       "",
		"/api.user.v1.Profile/FilterPublicFeedUserIds": "",
		"/api.user.v1.role.Role/List":                "",
		"/api.user.v1.site.Site/GetConfig":           "",
		"/api.user.v1.site.Site/VisitPing":           "",
	}
	return func(ctx context.Context, operation string) bool {
		if strings.Contains(operation, "auth/logout") {
			return false
		}
		// 静态资源公开
		if strings.Contains(operation, "static") {
			return false
		}
		// 粘贴板公开查看
		if strings.Contains(operation, "paste/get") {
			return false
		}
		// 社交：搜索/列表/计数公开（关注操作仍需登录）
		if strings.Contains(operation, "social/search") ||
			strings.Contains(operation, "social/following") ||
			strings.Contains(operation, "social/followers") ||
			strings.Contains(operation, "social/counts") ||
			strings.Contains(operation, "social/relation") ||
			strings.Contains(operation, "privacy/status") {
			return false
		}
		if _, ok := whiteList[operation]; ok {
			return false
		}
		return true
	}
}

// NewHTTPServer new an HTTP server.
func NewHTTPServer(
	c *conf.Server,
	d *data.Data,
	authService *service.AuthService,
	profileService *service.ProfileService,
	groupService *service.GroupService,
	roleService *service.RoleService,
	siteService *service.SiteService,
	orgService *service.OrgService,
	pasteService *service.PasteService,
	socialService *service.SocialService,
	logger log.Logger,

) *http.Server {
	var opts = []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
			safeerrors.Middleware(),
			opsmetrics.Middleware(d.RDB, "user"),
			authutil.CookieBearer(),
			selector.Server(jwt.Server(func(token *jwt2.Token) (interface{}, error) {
				if token.Method != jwt2.SigningMethodHS256 {
					return nil, jwt2.ErrSignatureInvalid
				}
				return []byte(_const.JWTSecret()), nil
			})).Match(NewWhiteListMatcher()).Build(),
		),
	}
	if c.Http.Network != "" {
		opts = append(opts, http.Network(c.Http.Network))
	}
	if c.Http.Addr != "" {
		opts = append(opts, http.Address(c.Http.Addr))
	}
	if c.Http.Timeout != nil {
		opts = append(opts, http.Timeout(c.Http.Timeout.AsDuration()))
	}
	srv := http.NewServer(opts...)
	health.Register(srv, health.Checker{DB: d.DB, RDB: d.RDB})
	auth.RegisterAuthHTTPServer(srv, authService)
	service.RegisterAuthSessionRoutes(srv)
	profile.RegisterProfileHTTPServer(srv, profileService)
	group.RegisterGroupHTTPServer(srv, groupService)
	role.RegisterRoleHTTPServer(srv, roleService)
	site.RegisterSiteHTTPServer(srv, siteService)
	service.RegisterUploadRoutes(srv)
	service.RegisterOrgRoutes(srv, orgService)
	service.RegisterPasteRoutes(srv, pasteService)
	service.RegisterSocialRoutes(srv, socialService)
	service.RegisterBackupRoutes(srv, d)
	return srv
}
