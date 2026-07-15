package server

import (
	"context"
	"cwxu-algo/api/core/v1/bulletin"
	"cwxu-algo/api/core/v1/contest_log"
	"cwxu-algo/api/core/v1/problem"
	"cwxu-algo/api/core/v1/spider"
	statistic2 "cwxu-algo/api/core/v1/statistic"
	"cwxu-algo/api/core/v1/submit_log"
	"cwxu-algo/app/common/conf"
	_const "cwxu-algo/app/common/const"
	"cwxu-algo/app/common/utils/health"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/service"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/auth/jwt"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/middleware/selector"
	"github.com/go-kratos/kratos/v2/transport/http"
	jwt2 "github.com/golang-jwt/jwt/v5"
)

func NewWhiteListMatcher() selector.MatchFunc {
	whiteList := map[string]string{
		"/api.core.v1.submit_log.Submit/GetSubmitLog":         "",
		"/api.core.v1.contest_log.Contest/GetContestList":     "",
		"/api.core.v1.contest_log.Contest/GetContestRanking":  "",
		"/api.core.v1.spider.Spider/GetSpider":                "",
		"/api.core.v1.statistic.Statistic/Heatmap":            "",
		"/api.core.v1.statistic.Statistic/PeriodCount":        "",
		"/api.core.v1.statistic.Statistic/Rank":               "",
		"/api.core.v1.bulletin.Bulletin/Get":                  "",
		"/api.core.v1.bulletin.Bulletin/List":                 "",
		"/api.core.v1.problem.Problem/List":                   "",
		"/api.core.v1.problem.Problem/ListTags":               "",
		"/api.core.v1.problem.Problem/Get":                    "",
		"/api.core.v1.problem.Problem/ListSubmissions":        "",
		"/api.core.v1.problem.Problem/UserProfile":            "",
	}
	return func(ctx context.Context, operation string) bool {
		//log.Info(operation)
		if _, ok := whiteList[operation]; ok {
			return false
		}
		return true
	}
}

// NewHTTPServer new an HTTP server.
func NewHTTPServer(c *conf.Server, logger log.Logger, d *data.Data, submitService *service.SubmitLogService, spiderService *service.SpiderService, statisticService *service.StatisticService, contestLogService *service.ContestLogService, bulletinService *service.BulletinService, problemService *service.ProblemService) *http.Server {
	var opts = []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
			selector.Server(jwt.Server(func(token *jwt2.Token) (interface{}, error) {
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
	submit_log.RegisterSubmitHTTPServer(srv, submitService)
	spider.RegisterSpiderHTTPServer(srv, spiderService)
	statistic2.RegisterStatisticHTTPServer(srv, statisticService)
	contest_log.RegisterContestHTTPServer(srv, contestLogService)
	bulletin.RegisterBulletinHTTPServer(srv, bulletinService)
	problem.RegisterProblemHTTPServer(srv, problemService)
	return srv
}
