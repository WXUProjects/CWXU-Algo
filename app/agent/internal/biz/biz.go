package biz

import (
	"cwxu-algo/app/agent/internal/biz/service"

	"github.com/google/wire"
)

var ProviderSet = wire.NewSet(service.NewSummaryUseCase, service.NewConsumer)
