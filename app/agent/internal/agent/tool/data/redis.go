package data

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
)

type RedisSetParams struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type RedisSet struct {
	rdb *redis.Client
}

func NewRedisSet(rdb *redis.Client) *RedisSet {
	return &RedisSet{
		rdb: rdb,
	}
}

func (c *RedisSet) Description() *model.Tool {
	return &model.Tool{
		Type: model.ToolTypeFunction,
		Function: &model.FunctionDefinition{
			Name:        "redis_set",
			Description: "将给定的key-value对写入Redis",
			Parameters: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"key": map[string]interface{}{
						"type":        "string",
						"description": "Redis的key",
					},
					"value": map[string]interface{}{
						"type":        "string",
						"description": "Redis的value",
					},
				},
			},
		},
	}
}

func (c *RedisSet) AiInterface(jsonStr string) string {
	rsp := RedisSetParams{}
	if err := json.Unmarshal([]byte(jsonStr), &rsp); err != nil {
		return "参数错误"
	}
	res, err := c.Handle(rsp.Key, rsp.Value)
	if err != nil {
		return "写入Redis失败"
	}
	return res
}

func (c *RedisSet) Handle(key, value string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.rdb.Set(ctx, key, value, 0).Err()
	if err != nil {
		log.Error(err)
		return "", err
	}
	return fmt.Sprintf("成功写入Redis: key=%s, value=%s", key, value), nil
}