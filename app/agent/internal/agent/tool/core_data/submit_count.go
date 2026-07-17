package core_data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"cwxu-algo/api/core/v1/statistic"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	grpc2 "google.golang.org/grpc"
)

type SubmitCntParms struct {
	StartDate string `json:"startDate"`
	EndDate   string `json:"endDate"`
	UserId    int    `json:"userId"`
}

type SubmitCnt struct {
	reg *registry.Registrar
	ctx context.Context
}

func NewSubmitCnt(reg *registry.Registrar, ctxs ...context.Context) *SubmitCnt {
	return &SubmitCnt{reg: reg, ctx: firstCtx(ctxs...)}
}

func (c *SubmitCnt) AuthContext() context.Context { return toolRPCContext(c.ctx) }

func (c *SubmitCnt) coreDataRPC() (*grpc2.ClientConn, error) {
	if c == nil || c.reg == nil {
		return nil, fmt.Errorf("registry 未配置")
	}
	return grpc.DialInsecure(
		toolRPCContext(c.ctx),
		grpc.WithEndpoint("discovery:///core-data"),
		grpc.WithDiscovery((*c.reg).(registry.Discovery)),
		grpc.WithTimeout(20*time.Second),
	)
}

func (c *SubmitCnt) Description() *model.Tool {
	return &model.Tool{
		Type: model.ToolTypeFunction,
		Function: &model.FunctionDefinition{
			Name:        "submit_cnt",
			Description: "获取指定用户id，指定日期区间的提交次数,只有日期，没有count字段的记为0",
			Parameters: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"startDate": map[string]interface{}{
						"type":        "string",
						"description": "起始时间，例如 20220101",
					},
					"endDate": map[string]interface{}{
						"type":        "string",
						"description": "结束时间，例如 20220101",
					},
					"userId": map[string]interface{}{
						"type":        "integer",
						"description": "用户id 0为全局维度",
					},
				},
				"required": []string{"startDate", "endDate", "userId"},
			},
		},
	}
}

func (c *SubmitCnt) AiInterface(jsonStr string) string {
	scp := SubmitCntParms{}
	if err := json.Unmarshal([]byte(jsonStr), &scp); err != nil {
		return "参数错误"
	}
	res, err := c.Handle(scp.StartDate, scp.EndDate, scp.UserId)
	if err != nil {
		return "查询失败" + err.Error()
	}
	return res
}

func (c *SubmitCnt) Handle(startDate, endDate string, userId int) (string, error) {
	conn, err := c.coreDataRPC()
	if err != nil {
		return "", err
	}
	defer conn.Close()
	sb := statistic.NewStatisticClient(conn)
	res, err := sb.Heatmap(
		toolRPCContext(c.ctx),
		&statistic.HeatmapReq{StartDate: startDate, EndDate: endDate, UserId: int64(userId)},
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
	return fmt.Sprintf("用户id为%d的提交记录如下%s", userId, string(respJson)), nil
}
