package discovery

import (
	"cwxu-algo/app/common/conf"

	"github.com/go-kratos/kratos/contrib/registry/consul/v2"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/google/wire"
	"github.com/hashicorp/consul/api"
)

type Register struct {
	Reg registry.Registrar
}

func NewConsulRegister(data *conf.Server) *Register {
	client, err := api.NewClient(&api.Config{Address: data.RegDsn})
	if err != nil {
		panic("注册中心链接失败" + err.Error())
	}
	return &Register{Reg: consul.New(client)}
}

var ProvideSet = wire.NewSet(NewConsulRegister)
