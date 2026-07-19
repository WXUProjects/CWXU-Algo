package core_data

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
)

// OrgBlogsTool 组织内公开博客摘要（无正文）
type OrgBlogsTool struct {
	reg *registry.Registrar
	ctx context.Context
}

func NewOrgBlogsTool(reg *registry.Registrar, ctxs ...context.Context) *OrgBlogsTool {
	return &OrgBlogsTool{reg: reg, ctx: firstCtx(ctxs...)}
}

func (c *OrgBlogsTool) AuthContext() context.Context { return toolRPCContext(c.ctx) }

func (c *OrgBlogsTool) Description() *model.Tool {
	return &model.Tool{
		Type: model.ToolTypeFunction,
		Function: &model.FunctionDefinition{
			Name: "org_blogs",
			Description: "组织内公开博客列表（标题/摘要/作者/时间，不含正文）。" +
				"用于分析知识沉淀与题解产出。orgId 默认取 elevated JWT 中的组织。",
			Parameters: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"orgId":    map[string]interface{}{"type": "integer", "description": "组织 id，0 时用 JWT org"},
					"page":     map[string]interface{}{"type": "integer", "description": "页码，默认 1"},
					"pageSize": map[string]interface{}{"type": "integer", "description": "默认 10，最大 15"},
				},
			},
		},
	}
}

func (c *OrgBlogsTool) AiInterface(jsonStr string) string {
	var p struct {
		OrgId    int64 `json:"orgId"`
		Page     int   `json:"page"`
		PageSize int   `json:"pageSize"`
	}
	if err := json.Unmarshal([]byte(jsonStr), &p); err != nil {
		return "参数错误"
	}
	if p.Page <= 0 {
		p.Page = 1
	}
	if p.PageSize <= 0 {
		p.PageSize = 10
	}
	if p.PageSize > 15 {
		p.PageSize = 15
	}
	q := url.Values{}
	q.Set("page", strconv.Itoa(p.Page))
	q.Set("pageSize", strconv.Itoa(p.PageSize))
	if p.OrgId > 0 {
		q.Set("orgId", strconv.FormatInt(p.OrgId, 10))
	}
	path := "/v1/user/blog/recommend?" + q.Encode()
	body, code, err := discoveryHTTPGet(c.ctx, c.reg, "user", path)
	if err != nil {
		return "连接失败: " + err.Error()
	}
	if code >= 400 {
		return fmt.Sprintf("查询失败 HTTP %d: %s", code, truncateStr(string(body), 300))
	}
	// 尽量只保留摘要字段
	var raw map[string]interface{}
	if err := json.Unmarshal(body, &raw); err != nil {
		return "组织博客: " + truncateStr(string(body), 8000)
	}
	data, _ := raw["data"].(map[string]interface{})
	list, _ := data["list"].([]interface{})
	type brief struct {
		ID        interface{} `json:"id,omitempty"`
		Title     interface{} `json:"title,omitempty"`
		Summary   interface{} `json:"summary,omitempty"`
		Author    interface{} `json:"author,omitempty"`
		Published interface{} `json:"publishedAt,omitempty"`
		Created   interface{} `json:"createdAt,omitempty"`
	}
	out := make([]brief, 0, len(list))
	for _, it := range list {
		m, ok := it.(map[string]interface{})
		if !ok {
			continue
		}
		b := brief{
			ID: m["id"], Title: m["title"], Summary: m["summary"],
			Published: m["publishedAt"], Created: m["createdAt"],
		}
		if a, ok := m["author"].(map[string]interface{}); ok {
			if n, ok := a["name"]; ok {
				b.Author = n
			} else if n, ok := a["username"]; ok {
				b.Author = n
			}
		} else if n, ok := m["authorName"]; ok {
			b.Author = n
		}
		// 摘要截断
		if s, ok := b.Summary.(string); ok && len(s) > 120 {
			b.Summary = s[:120] + "…"
		}
		out = append(out, b)
	}
	total := data["total"]
	jb, _ := json.Marshal(out)
	return fmt.Sprintf("组织博客 total=%v: %s", total, string(jb))
}
