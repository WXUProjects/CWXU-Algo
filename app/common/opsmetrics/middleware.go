package opsmetrics

import (
	"context"

	"github.com/go-kratos/kratos/v2/middleware"
	"github.com/go-kratos/kratos/v2/transport"
	"github.com/redis/go-redis/v9"
)

// Middleware 统计 API 请求量与并发峰值（按服务名）
func Middleware(rdb *redis.Client, service string) middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			// 跳过健康检查
			if tr, ok := transport.FromServerContext(ctx); ok {
				op := tr.Operation()
				if op == "" || containsHealth(op) {
					return handler(ctx, req)
				}
			}
			done := RecordAPIRequest(ctx, rdb, service)
			defer done()
			return handler(ctx, req)
		}
	}
}

func containsHealth(op string) bool {
	return op == "/healthz" || op == "/readyz" ||
		len(op) >= 7 && (op[len(op)-7:] == "healthz" || op[len(op)-6:] == "readyz")
}
