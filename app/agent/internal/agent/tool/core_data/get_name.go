package core_data

import (
	"context"
	"cwxu-algo/api/user/v1/profile"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	grpc2 "google.golang.org/grpc"
)

func (c *GetNameById) userRPC() (*grpc2.ClientConn, error) {
	return grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*c.reg).(registry.Discovery)),
		grpc.WithTimeout(20*time.Second),
	)
}

type GetNameByIdJson struct {
	UserId int `json:"userId"`
}

type GetNameById struct {
	reg *registry.Registrar
}

func NewGetProfileById(reg *registry.Registrar) *GetNameById {
	return &GetNameById{
		reg: reg,
	}
}
func (c *GetNameById) Description() *model.Tool {
	return &model.Tool{
		Type: model.ToolTypeFunction,
		Function: &model.FunctionDefinition{
			Name:        "get_profile_by_user_id",
			Description: "根据用户id，获取用户资料包括姓名，邮箱等，将返回json",
			Parameters: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"userId": map[string]interface{}{
						"type":        "int",
						"description": "用户id",
					},
				},
			},
		},
	}
}

func (c *GetNameById) AiInterface(jsonStr string) string {
	scp := SubmitCntParms{}
	if err := json.Unmarshal([]byte(jsonStr), &scp); err != nil {
		return "参数错误"
	}
	res, err := c.Handle(scp.UserId)
	if err != nil {
		return "查询失败"
	}
	return res
}

func (c *GetNameById) Handle(userId int) (string, error) {
	if userId == 0 {
		return "用户id不可以为0", errors.New("用户id不可以为0")
	}
	conn, err := c.userRPC()
	if err != nil {
		return "", err
	}
	sb := profile.NewProfileClient(conn)
	res, err := sb.GetById(
		context.Background(),
		&profile.GetByIdReq{UserId: int64(userId)},
	)
	if err != nil {
		log.Error(err)
		return "", err
	}
	respJson, _ := json.Marshal(res)
	return fmt.Sprintf("用户id为%d的资料信息是%s", userId, respJson), nil
}
