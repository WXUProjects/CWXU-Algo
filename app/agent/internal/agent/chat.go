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

const defaultMaxRounds = 8

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

// Complete 纯文本补全（不携带工具），用于预取数据后的文案生成。
func (c *Chat) Complete(ctx context.Context, messages []*model.ChatCompletionMessage) (string, error) {
	return c.Chat(ctx, messages)
}

// Chat 支持可选工具调用。maxRounds 防止无限循环；未知工具返回错误文本而非 panic。
func (c *Chat) Chat(ctx context.Context, messages []*model.ChatCompletionMessage, tools ...tool.AgentToolFactory) (string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	finalResp := ""
	reg := map[string]tool.AgentToolFactory{}
	toolUse := make([]*model.Tool, 0, len(tools))
	for _, t := range tools {
		desc := t.Description()
		if desc == nil || desc.Function == nil || desc.Function.Name == "" {
			continue
		}
		reg[desc.Function.Name] = t
		toolUse = append(toolUse, desc)
	}

	for round := 0; round < defaultMaxRounds; round++ {
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
		choice := resp.Choices[0]
		if choice.Message.Content != nil && choice.Message.Content.StringValue != nil {
			finalResp = *choice.Message.Content.StringValue
		}
		if choice.Message.ReasoningContent != nil {
			log.Infof("reasoning: %s", *choice.Message.ReasoningContent)
		}
		if choice.FinishReason != model.FinishReasonToolCalls || len(choice.Message.ToolCalls) == 0 {
			return finalResp, nil
		}
		messages = append(messages, &choice.Message)
		for _, toolCall := range choice.Message.ToolCalls {
			name := toolCall.Function.Name
			args := toolCall.Function.Arguments
			log.Infof("执行工具 %s %s", name, args)
			toolMsg := ""
			if t, ok := reg[name]; ok {
				toolMsg = t.AiInterface(args)
			} else {
				toolMsg = fmt.Sprintf("工具不存在: %s", name)
				log.Warnf("未知工具调用: %s", name)
			}
			log.Infof("工具结果 %s: %s", name, truncate(toolMsg, 500))
			messages = append(messages, &model.ChatCompletionMessage{
				Role:       model.ChatMessageRoleTool,
				Content:    &model.ChatCompletionMessageContent{StringValue: volcengine.String(toolMsg)},
				ToolCallID: toolCall.ID,
			})
		}
	}
	return finalResp, fmt.Errorf("工具调用超过最大轮次 %d", defaultMaxRounds)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
