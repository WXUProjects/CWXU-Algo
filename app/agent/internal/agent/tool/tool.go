package tool

import (
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
)

// AgentToolFactory 工具工厂
type AgentToolFactory interface {
	Description() *model.Tool
	AiInterface(json string) string
}
