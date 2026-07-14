package service

import (
	"context"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/core_data/internal/data/model"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/shared"
)

// ProblemTagger 使用官方 openai-go SDK 调用 OpenAI 兼容 Chat Completions。
// 与 app/agent、火山 Ark 完全隔离，仅读取 ai_analyze 配置。
type ProblemTagger struct {
	client openai.Client
	model  string
	ready  bool
}

func NewProblemTagger(c *conf.AiAnalyze) *ProblemTagger {
	t := &ProblemTagger{}
	if c == nil || strings.TrimSpace(c.Secret) == "" || strings.TrimSpace(c.Endpoint) == "" {
		return t
	}
	base := normalizeOpenAIBaseURL(c.Endpoint)
	httpClient := &http.Client{Timeout: 240 * time.Second}
	t.client = openai.NewClient(
		option.WithAPIKey(c.Secret),
		option.WithBaseURL(base),
		option.WithHTTPClient(httpClient),
	)
	t.model = c.Model
	t.ready = true
	return t
}

type aiAnalyzeResult struct {
	ProblemType        string               `json:"problem_type"`
	Difficulty         string               `json:"difficulty"`
	AlgorithmTags      []string             `json:"algorithm_tags"`
	SuggestedSolutions []model.SolutionMeta `json:"suggested_solutions"`
	ContentMD          string               `json:"content_md"` // 可选：AI 优化排版后的题面
}

func (t *ProblemTagger) Analyze(ctx context.Context, title, contentMD string) (*aiAnalyzeResult, error) {
	if !t.ready {
		return nil, fmt.Errorf("ai_analyze 未配置")
	}
	// 节约 token：截断超长题面（翻译+排版需要更多上下文）
	content := contentMD
	if len(content) > 18000 {
		content = content[:18000] + "\n...(truncated)"
	}
	system := `你是算法题目标签分析器与题面编辑器。快速分析即可，不必深入推导，不要长篇推理。
仅输出 JSON，不要 markdown 代码块，不要解释过程。

【最高优先级】所有字符串字段的可见文字必须是中文，禁止英文单词/短语作为展示内容。
包括但不限于：problem_type、difficulty、algorithm_tags、suggested_solutions 的 name/brief_explanation、content_md 全文。
英文算法名必须译成中文，例如：DP→动态规划，BFS→广度优先搜索，DFS→深度优先搜索，Dijkstra→最短路，Binary Search→二分查找。
复杂度字段 time_complexity / space_complexity 可用 O(n)、O(n log n) 等数学写法。

【题面 content_md — 必须输出，禁止空字符串】
1. 若原题面为英文或中英混杂：将题意、输入、输出、样例说明、约束等全部译成通顺中文；专有名词（人名、平台名）可保留原文一次并附中文。
2. 若原题面已是中文：优化排版与分段，修正明显乱码/粘连，不要无故改写题意。
3. 统一 Markdown 结构，章节标题用中文：
   # 标题
   ## 题意
   ## 输入
   ## 输出
   ## 样例（多个时用 ### 样例 1 / 样例 2）
   ## 说明（可选）
4. 样例输入输出用 fenced code block（三反引号）原样保留数字与格式，不要翻译样例数据。
5. 数学公式必须用 $...$ 或 $$...$$（KaTeX 兼容）；禁止使用 $$$；禁止把公式拆成纯文字。
6. 保留原题全部条件与约束，不得删减关键数据范围。

字段：
- problem_type: 中文模块名（图论、动态规划、数据结构、数学、字符串、贪心等）
- difficulty: 只能是 简单 / 中等 / 困难
- algorithm_tags: 中文算法标签数组（2~6 个）
- suggested_solutions: 1~2 个，含 name, time_complexity, space_complexity, brief_explanation（中文，各一两句）
- content_md: 中文 Markdown 题面（见上，必填）
禁止分析用户代码；不要输出除 JSON 外的任何文字。`
	user := fmt.Sprintf("标题: %s\n\n题面:\n%s", title, content)

	params := openai.ChatCompletionNewParams{
		Model: shared.ChatModel(t.model),
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(system),
			openai.UserMessage(user),
		},
		Temperature: openai.Float(0.2),
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONObject: ptrJSONObject(),
		},
	}

	chat, err := t.client.Chat.Completions.New(ctx, params)
	if err != nil {
		// 部分兼容网关不支持 response_format，降级重试
		params.ResponseFormat = openai.ChatCompletionNewParamsResponseFormatUnion{}
		chat, err = t.client.Chat.Completions.New(ctx, params)
		if err != nil {
			return nil, fmt.Errorf("openai chat completion: %w", err)
		}
	}
	if len(chat.Choices) == 0 {
		return nil, fmt.Errorf("AI 返回空 choices")
	}
	contentStr := stripJSONFence(strings.TrimSpace(chat.Choices[0].Message.Content))
	var result aiAnalyzeResult
	if err := json.Unmarshal([]byte(contentStr), &result); err != nil {
		return nil, fmt.Errorf("反序列化 AI JSON 失败: %w body=%s", err, truncateStr(contentStr, 400))
	}
	result.Difficulty = normalizeDifficulty(result.Difficulty)
	result.AlgorithmTags = normalizeChineseTags(result.AlgorithmTags)
	result.ProblemType = strings.TrimSpace(result.ProblemType)
	// content_md 必填意图：若模型返回空则保留爬取原文（由调用方决定是否覆盖）
	result.ContentMD = strings.TrimSpace(result.ContentMD)
	// 清理 $$$ 为 $
	if result.ContentMD != "" {
		result.ContentMD = strings.ReplaceAll(result.ContentMD, "$$$", "$")
	}
	return &result, nil
}

// normalizeChineseTags 去掉空白、过短、明显纯英文标签
func normalizeChineseTags(tags []string) []string {
	out := make([]string, 0, len(tags))
	seen := map[string]bool{}
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == "" || len([]rune(t)) < 2 {
			continue
		}
		if seen[t] {
			continue
		}
		// 纯 ASCII 英文短语大概率是漏译，丢弃
		if isASCIIWord(t) {
			continue
		}
		seen[t] = true
		out = append(out, t)
		if len(out) >= 6 {
			break
		}
	}
	return out
}

func isASCIIWord(s string) bool {
	hasLetter := false
	for _, r := range s {
		if r > 127 {
			return false
		}
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') {
			hasLetter = true
		} else if r == ' ' || r == '-' || r == '_' {
			continue
		} else if r >= '0' && r <= '9' {
			continue
		} else {
			return false
		}
	}
	return hasLetter
}

// normalizeOpenAIBaseURL 将配置 endpoint 规范为 openai-go 的 BaseURL（需含 /v1/ 前缀路径）。
// SDK 会再拼 chat/completions → 最终 .../v1/chat/completions
//
// 支持:
//   - https://api.openai.com/v1
//   - http://host:8001/api        → http://host:8001/api/v1/
//   - http://host/v1/chat/completions → http://host/v1/
func normalizeOpenAIBaseURL(endpoint string) string {
	ep := strings.TrimRight(strings.TrimSpace(endpoint), "/")
	if strings.HasSuffix(ep, "/chat/completions") {
		ep = strings.TrimSuffix(ep, "/chat/completions")
		ep = strings.TrimRight(ep, "/")
	}
	if !strings.HasSuffix(ep, "/v1") {
		if strings.HasSuffix(ep, "/api") {
			ep = ep + "/v1"
		} else {
			ep = ep + "/v1"
		}
	}
	return ep + "/"
}

func stripJSONFence(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```json")
		s = strings.TrimPrefix(s, "```JSON")
		s = strings.TrimPrefix(s, "```")
		s = strings.TrimSuffix(s, "```")
		s = strings.TrimSpace(s)
	}
	return s
}

func normalizeDifficulty(d string) string {
	d = strings.TrimSpace(d)
	switch strings.ToLower(d) {
	case "easy", "简单", "入门":
		return "简单"
	case "medium", "中等", "中级":
		return "中等"
	case "hard", "困难", "高级":
		return "困难"
	default:
		if d == "" {
			return "中等"
		}
		return d
	}
}

func truncateStr(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

func ptrJSONObject() *shared.ResponseFormatJSONObjectParam {
	p := shared.NewResponseFormatJSONObjectParam()
	return &p
}
