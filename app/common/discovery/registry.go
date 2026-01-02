package discovery

import (
	"github.com/go-kratos/kratos/contrib/registry/consul/v2"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/google/wire"
	"github.com/hashicorp/consul/api"
)

type Register struct {
	Reg registry.Registrar
}

func NewConsulRegister() *Register {
	client, err := api.NewClient(&api.Config{Address: "127.0.0.1:8500"})
	if err != nil {
		panic("注册中心链接失败" + err.Error())
	}
	return &Register{Reg: consul.New(client)}
}

var ProvideSet = wire.NewSet(NewConsulRegister)
