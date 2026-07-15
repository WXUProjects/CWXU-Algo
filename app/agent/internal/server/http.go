package server

import (
	"cwxu-algo/api/agent/v1/summary"
	"cwxu-algo/app/agent/internal/data"
	"cwxu-algo/app/agent/internal/service"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/opsmetrics"
	"cwxu-algo/app/common/utils/health"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware/recovery"
	"github.com/go-kratos/kratos/v2/transport/http"
)

// NewHTTPServer new an HTTP server.
func NewHTTPServer(c *conf.Server, logger log.Logger, d *data.Data, summaryService *service.SummaryService) *http.Server {
	var opts = []http.ServerOption{
		http.Middleware(
			recovery.Recovery(),
			opsmetrics.Middleware(d.RDB, "agent"),
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
	health.Register(srv, health.Checker{RDB: d.RDB})
	summary.RegisterSummaryHTTPServer(srv, summaryService)
	return srv
}
