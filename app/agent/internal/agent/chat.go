package agent

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"

	"cwxu-algo/app/agent/internal/agent/tool"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/sitesettings"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
)

const defaultMaxRounds = 8

type Chat struct {
	yaml   *conf.Agent
	rdb    *redis.Client
	mu     sync.Mutex
	client *arkruntime.Client
	model  string
	secret string
}

func NewChat(yaml *conf.Agent, rdb *redis.Client) *Chat {
	c := &Chat{yaml: yaml, rdb: rdb}
	c.reload(context.Background())
	return c
}

func (c *Chat) runtime(ctx context.Context) *sitesettings.Runtime {
	rt := sitesettings.Load(ctx, c.rdb, nil)
	return rt.MergeFallback(nil, c.yaml, nil)
}

func (c *Chat) reload(ctx context.Context) {
	rt := c.runtime(ctx)
	modelID := strings.TrimSpace(rt.AgentModel)
	secret := strings.TrimSpace(rt.AgentSecret)
	c.mu.Lock()
	defer c.mu.Unlock()
	if secret == c.secret && modelID == c.model && c.client != nil {
		return
	}
	c.model = modelID
	c.secret = secret
	if secret == "" {
		c.client = nil
		return
	}
	c.client = arkruntime.NewClientWithApiKey(secret)
}

func (c *Chat) ensureClient(ctx context.Context) (*arkruntime.Client, string, error) {
	c.reload(ctx)
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.client == nil || c.model == "" {
		return nil, "", errors.New("AI 总结模型未配置（请在站点设置中填写）")
	}
	return c.client, c.model, nil
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
	client, modelID, err := c.ensureClient(ctx)
	if err != nil {
		return "", err
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
			Model:    modelID,
			Messages: messages,
			Tools:    toolUse,
		}
		resp, err := client.CreateChatCompletion(ctx, &req)
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
