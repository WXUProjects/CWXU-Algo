package core_data

import (
	"context"
	"cwxu-algo/api/core/v1/submit_log"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	grpc2 "google.golang.org/grpc"
)

func (c *SubmitLog) coreDataRPC() (*grpc2.ClientConn, error) {
	return grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///core-data"),
		grpc.WithDiscovery((*c.reg).(registry.Discovery)),
		grpc.WithTimeout(20*time.Second),
	)
}

type SubmitLogParms struct {
	StartDate string `json:"startDate"`
	EndDate   string `json:"endDate"`
	UserId    int    `json:"userId"`
	Limit     int    `json:"limit"`
}

type SubmitLog struct {
	reg *registry.Registrar
}

func NewSubmitLog(reg *registry.Registrar) *SubmitLog {
	return &SubmitLog{
		reg: reg,
	}
}
func (c *SubmitLog) Description() *model.Tool {
	return &model.Tool{
		Type: model.ToolTypeFunction,
		Function: &model.FunctionDefinition{
			Name: "submit_log",
			Description: "获取指定用户id，获取从endDate开始，向前退limit条提交数据，需要注意的是，" +
				"Nowcoder 的平台如果contest出现main字样则代表我们只能拉取到正确的提交记录，不能说明用户只交了一次就对了，其他的平台不需要遵守这一条",
			Parameters: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"endDate": map[string]interface{}{
						"type":        "string",
						"description": "结束时间，例如 20220101",
					},
					"limit": map[string]interface{}{
						"type":        "int",
						"description": "从endDate开始，向前退limit条",
					},
					"userId": map[string]interface{}{
						"type":        "int",
						"description": "用户id",
					},
				},
			},
		},
	}
}

func (c *SubmitLog) AiInterface(jsonStr string) string {
	scp := SubmitLogParms{}
	if err := json.Unmarshal([]byte(jsonStr), &scp); err != nil {
		return "参数错误"
	}
	res, err := c.Handle(scp.EndDate, scp.UserId, scp.Limit)
	if err != nil {
		return "查询失败，请返回：查询失败"
	}
	return res
}

func (c *SubmitLog) Handle(endDate string, userId int, limit int) (string, error) {
	conn, err := c.coreDataRPC()
	if err != nil {
		return "", err
	}
	sb := submit_log.NewSubmitClient(conn)
	t, _ := time.Parse("20060102", endDate)
	t = t.AddDate(0, 0, 1)
	res, err := sb.GetSubmitLog(
		context.Background(),
		&submit_log.GetSubmitLogReq{Limit: int64(limit), Cursor: t.Unix(), UserId: int64(userId)},
	)
	if err != nil {
		log.Error(err)
		return "", err
	}
	respJson, err := json.Marshal(res.Data)
	if err != nil {
		log.Error(err)
		return "", err
	}
	return fmt.Sprintf("用户id为%d在%s之前的最近%d条提交记录数据如下%s", userId, endDate, limit, string(respJson)), nil
}
