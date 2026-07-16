package safeerrors

import (
	"context"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/middleware"
)

// Middleware prevents database, network, and provider details from being
// serialized to clients while retaining the full error in server logs.
func Middleware() middleware.Middleware {
	return func(handler middleware.Handler) middleware.Handler {
		return func(ctx context.Context, req interface{}) (interface{}, error) {
			response, err := handler(ctx, req)
			if err == nil {
				return response, nil
			}
			serviceErr := errors.FromError(err)
			if serviceErr.Code < 500 {
				return response, err
			}
			log.Errorf("internal request failure reason=%s: %v", serviceErr.Reason, err)
			return nil, errors.New(int(serviceErr.Code), serviceErr.Reason, "服务暂时不可用")
		}
	}
}
