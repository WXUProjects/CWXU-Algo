package service

import (
	"fmt"
	"html"
	"strings"
)

// RenderRuleTemplateHTML 非 AI：基于真实统计套规则模板生成 HTML
func RenderRuleTemplateHTML(data *TrainingReportData, brand string) string {
	if data == nil {
		return ""
	}
	if brand == "" {
		brand = "GoAlgo"
	}
	delta := data.TotalSubmits - data.PrevTotalSubmits
	deltaStr := fmt.Sprintf("%+d", delta)
	trendEmoji := "➡️"
	if delta > 0 {
		trendEmoji = "🔥"
	} else if delta < 0 {
		trendEmoji = "⚠️"
	}
	statusEmoji := "❄️"
	if data.MemberCount > 0 {
		ratio := float64(data.ActiveMembers) / float64(data.MemberCount)
		if ratio >= 0.6 && delta >= 0 {
			statusEmoji = "🔥"
		} else if ratio >= 0.3 {
			statusEmoji = "⚠️"
		}
	}

	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">`)
	b.WriteString(`<style>
body{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Microsoft YaHei",sans-serif;margin:0;padding:16px;background:#f6f7fb;color:#1a1a1a}
.card{max-width:720px;margin:0 auto;background:#fff;border-radius:12px;padding:20px 22px;box-shadow:0 1px 4px rgba(0,0,0,.06)}
h1{font-size:20px;margin:0 0 8px}
.meta{color:#666;font-size:13px;margin-bottom:16px}
.grid{display:grid;grid-template-columns:1fr 1fr;gap:10px;margin:12px 0 18px}
.stat{background:#f3f5fa;border-radius:10px;padding:12px}
.stat .n{font-size:22px;font-weight:700}
.stat .l{font-size:12px;color:#666;margin-top:2px}
h2{font-size:15px;margin:18px 0 8px}
table{width:100%;border-collapse:collapse;font-size:13px}
td,th{padding:8px 6px;border-bottom:1px solid #eee;text-align:left}
.tag{display:inline-block;background:#eef2ff;color:#334;border-radius:999px;padding:2px 8px;font-size:12px;margin:2px}
.foot{margin-top:18px;font-size:12px;color:#888}
@media(max-width:520px){.grid{grid-template-columns:1fr}}
</style></head><body><div class="card">`)

	fmt.Fprintf(&b, `<h1>%s 训练报告 %s</h1>`, html.EscapeString(brand), statusEmoji)
	fmt.Fprintf(&b, `<div class="meta">区间 %s ~ %s · 范围 %s · 成员 %d 人</div>`,
		html.EscapeString(data.StartDate), html.EscapeString(data.EndDate),
		html.EscapeString(data.ScopeLabel), data.MemberCount)

	b.WriteString(`<div class="grid">`)
	fmt.Fprintf(&b, `<div class="stat"><div class="n">%d</div><div class="l">本区间总提交 %s %s</div></div>`,
		data.TotalSubmits, trendEmoji, deltaStr)
	fmt.Fprintf(&b, `<div class="stat"><div class="n">%d</div><div class="l">本区间 AC 次数</div></div>`, data.TotalAC)
	fmt.Fprintf(&b, `<div class="stat"><div class="n">%d</div><div class="l">有提交成员</div></div>`, data.ActiveMembers)
	fmt.Fprintf(&b, `<div class="stat"><div class="n">%d</div><div class="l">对比上期提交</div></div>`, data.PrevTotalSubmits)
	b.WriteString(`</div>`)

	// 日趋势
	b.WriteString(`<h2>每日提交走势</h2><table><tr><th>日期</th><th>提交</th></tr>`)
	for _, d := range data.DailyTrend {
		fmt.Fprintf(&b, `<tr><td>%s</td><td>%d</td></tr>`, html.EscapeString(d.Date), d.Count)
	}
	b.WriteString(`</table>`)

	// Top 提交
	b.WriteString(`<h2>提交 Top</h2>`)
	if len(data.TopSubmit) == 0 {
		b.WriteString(`<p style="color:#888;font-size:13px">本区间暂无提交记录。</p>`)
	} else {
		b.WriteString(`<table><tr><th>#</th><th>成员</th><th>提交</th></tr>`)
		for _, r := range data.TopSubmit {
			fmt.Fprintf(&b, `<tr><td>%d</td><td>%s</td><td>%d</td></tr>`,
				r.Rank, html.EscapeString(r.Name), r.Score)
		}
		b.WriteString(`</table>`)
	}

	// Top AC
	b.WriteString(`<h2>AC Top</h2>`)
	if len(data.TopAC) == 0 {
		b.WriteString(`<p style="color:#888;font-size:13px">本区间暂无 AC 记录。</p>`)
	} else {
		b.WriteString(`<table><tr><th>#</th><th>成员</th><th>AC</th></tr>`)
		for _, r := range data.TopAC {
			fmt.Fprintf(&b, `<tr><td>%d</td><td>%s</td><td>%d</td></tr>`,
				r.Rank, html.EscapeString(r.Name), r.Score)
		}
		b.WriteString(`</table>`)
	}

	// 不活跃
	b.WriteString(`<h2>本区间未提交成员</h2>`)
	if len(data.InactiveMembers) == 0 {
		b.WriteString(`<p style="color:#888;font-size:13px">全员都有提交，给力！</p>`)
	} else {
		for _, n := range data.InactiveMembers {
			fmt.Fprintf(&b, `<span class="tag">%s</span>`, html.EscapeString(n))
		}
	}

	// 规则建议
	b.WriteString(`<h2>规则建议</h2><ul style="font-size:13px;line-height:1.6;padding-left:18px">`)
	if data.TotalSubmits == 0 {
		b.WriteString(`<li>本区间零提交，建议组织统一训练日并检查账号绑定。</li>`)
	} else if delta < 0 {
		b.WriteString(`<li>提交量环比下降，可重点关注 Top 之外的中腰部成员。</li>`)
	} else {
		b.WriteString(`<li>提交量稳定或上升，继续保持节奏并巩固 AC 质量。</li>`)
	}
	if len(data.InactiveMembers) > 0 {
		fmt.Fprintf(&b, `<li>有 %d 名成员本区间未提交，建议一对一跟进。</li>`, len(data.InactiveMembers))
	}
	if len(data.TopAC) > 0 {
		fmt.Fprintf(&b, `<li>AC 之星：%s（%d），可请其分享题解或带训。</li>`,
			html.EscapeString(data.TopAC[0].Name), data.TopAC[0].Score)
	}
	b.WriteString(`</ul>`)

	fmt.Fprintf(&b, `<div class="foot">由 %s 规则模板生成 · 仅使用真实统计数据，未编造名单与数字。</div>`, html.EscapeString(brand))
	b.WriteString(`</div></body></html>`)
	return b.String()
}

// trainingReportSystemPrompt AI 训练报告（复用周报风格）
// 工具含 problem_tags：可查标签表/成员标签画像/题目标签。
func trainingReportSystemPrompt() string {
	return `你是算法训练平台的教练助手，为教练/队长写组织训练报告。
要求：
1. 风格：Acmer 校园口语、简洁有力。
2. 只输出完整 HTML（可含 style），适配 PC/移动端。
3. 只能使用给定数据与工具返回的真实名单/数字/标签，禁止编造成员姓名、排行或知识点。
4. 可调用工具补充查询：成员列表、热力、排行、提交明细、以及 problem_tags（标签表/成员标签画像/按题 id 取标签）。
5. 建议结合标签做知识点维度点评（如团队 DP 薄弱、某人图论突出）。
6. 不要输出提示词，不要 Markdown 代码围栏。`
}

func trainingReportUserPrompt(data *TrainingReportData) string {
	// 预置关键数据，减少工具往返；工具可用于深挖
	return fmt.Sprintf(`请根据以下组织训练数据生成训练报告 HTML，结构建议包含：
1. 区间总提交 vs 上期（箭头升降）
2. Top 活跃（topSubmit）
3. 未提交/不活跃名单（inactiveMembers）
4. AC 表现（topAc）
5. 知识点/标签观察（可对 top 成员调用 problem_tags.user_profile，或 list 对照全站标签）
6. 给教练的鼓励/鞭策建议
7. 团队状态 emoji（🔥/⚠️/❄️）

范围：%s · 日期 %s ~ %s · 组织 %d · 组 %d
成员数 %d，有提交 %d，总提交 %d（上期 %d），总 AC %d

预置 JSON 数据（真实）：
%s`,
		data.ScopeLabel, data.StartDate, data.EndDate, data.OrgID, data.GroupID,
		data.MemberCount, data.ActiveMembers, data.TotalSubmits, data.PrevTotalSubmits, data.TotalAC,
		mustJSON(data))
}

func mustJSON(v interface{}) string {
	b, err := jsonIndent(v)
	if err != nil {
		return "{}"
	}
	return b
}
