package agent

import (
	"context"
	"cwxu-algo/app/agent/internal/agent/tool"
	"cwxu-algo/app/common/conf"
	"errors"
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
)

type Chat struct {
	conf   *conf.Agent
	client *arkruntime.Client
}

func NewChat(conf *conf.Agent) *Chat {
	client := arkruntime.NewClientWithApiKey(
		conf.Secret,
	)
	return &Chat{conf: conf, client: client}
}

func (c *Chat) Chat(messages []*model.ChatCompletionMessage, tools ...tool.AgentToolFactory) (string, error) {
	ctx := context.Background()
	finalResp := ""
	reg := map[string]tool.AgentToolFactory{}
	toolUse := make([]*model.Tool, 0)
	for _, t := range tools {
		reg[t.Description().Function.Name] = t
		toolUse = append(toolUse, t.Description())
	}
	for {
		req := model.CreateChatCompletionRequest{
			Model:    c.conf.Model,
			Messages: messages,
			Tools:    toolUse,
		}
		resp, err := c.client.CreateChatCompletion(ctx, &req)
		if err != nil {
			return "", err
		}
		if len(resp.Choices) == 0 {
			return "", errors.New("模型返回空")
		}
		if resp.Choices[0].Message.Content != nil {
			finalResp = *resp.Choices[0].Message.Content.StringValue
		}
		if resp.Choices[0].Message.ReasoningContent != nil {
			fmt.Println(*resp.Choices[0].Message.ReasoningContent)
		}
		if resp.Choices[0].FinishReason != model.FinishReasonToolCalls || len(resp.Choices[0].Message.ToolCalls) == 0 {
			break
		}
		messages = append(messages, &resp.Choices[0].Message)
		for _, toolCall := range resp.Choices[0].Message.ToolCalls {
			log.Infof("执行%s %s", toolCall.Function.Name, toolCall.Function.Arguments)
			toolMsg := reg[toolCall.Function.Name].AiInterface(toolCall.Function.Arguments)
			log.Infof("结果%s", toolMsg)
			messages = append(messages, &model.ChatCompletionMessage{
				Role:       model.ChatMessageRoleTool,
				Content:    &model.ChatCompletionMessageContent{StringValue: volcengine.String(toolMsg)},
				ToolCallID: toolCall.ID,
			})
		}
	}
	return finalResp, nil
}
