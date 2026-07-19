package service

import (
	"fmt"
	"html"
	"strings"
)

// RenderRuleTemplateHTML 非 AI：基于真实统计套规则模板生成 HTML。
// mode: full（后台详版）| compact（教练周报简版）；维度一致，篇幅不同。
func RenderRuleTemplateHTML(data *TrainingReportData, brand string, mode ...string) string {
	if data == nil {
		return ""
	}
	if brand == "" {
		brand = "GoAlgo"
	}
	detail := DetailModeFull
	if len(mode) > 0 && mode[0] != "" {
		detail = mode[0]
	}
	compact := detail == DetailModeCompact

	delta := data.TotalSubmits - data.PrevTotalSubmits
	deltaStr := fmt.Sprintf("%+d", delta)
	trendEmoji := "➡️"
	if delta > 0 {
		trendEmoji = "🔥"
	} else if delta < 0 {
		trendEmoji = "⚠️"
	}
	statusEmoji, dimLines, advice := ruleComprehensiveEval(data, delta)

	topN := 10
	inactiveN := 50
	feedN := 12
	contestN := 8
	blogN := 8
	if compact {
		topN = 5
		inactiveN = 15
		feedN = 6
		contestN = 3
		blogN = 5
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
.eval{background:#f8fafc;border-radius:10px;padding:12px 14px;font-size:13px;line-height:1.65}
@media(max-width:520px){.grid{grid-template-columns:1fr}}
</style></head><body><div class="card">`)

	title := "训练报告"
	if compact {
		title = "教练周报"
	}
	fmt.Fprintf(&b, `<h1>%s %s %s</h1>`, html.EscapeString(brand), title, statusEmoji)
	fmt.Fprintf(&b, `<div class="meta">区间 %s ~ %s · 范围 %s · 成员 %d 人`,
		html.EscapeString(data.StartDate), html.EscapeString(data.EndDate),
		html.EscapeString(data.ScopeLabel), data.MemberCount)
	if compact {
		b.WriteString(` · 简版`)
	} else {
		b.WriteString(` · 详版`)
	}
	b.WriteString(`</div>`)

	// 1 活跃度
	b.WriteString(`<h2>1. 活跃度与趋势</h2><div class="grid">`)
	fmt.Fprintf(&b, `<div class="stat"><div class="n">%d</div><div class="l">本区间总提交 %s %s</div></div>`,
		data.TotalSubmits, trendEmoji, deltaStr)
	fmt.Fprintf(&b, `<div class="stat"><div class="n">%d</div><div class="l">本区间 AC 次数</div></div>`, data.TotalAC)
	fmt.Fprintf(&b, `<div class="stat"><div class="n">%d</div><div class="l">有提交成员</div></div>`, data.ActiveMembers)
	fmt.Fprintf(&b, `<div class="stat"><div class="n">%d</div><div class="l">对比上期提交</div></div>`, data.PrevTotalSubmits)
	b.WriteString(`</div>`)

	if !compact {
		b.WriteString(`<h2>每日提交走势</h2><table><tr><th>日期</th><th>提交</th></tr>`)
		for _, d := range data.DailyTrend {
			fmt.Fprintf(&b, `<tr><td>%s</td><td>%d</td></tr>`, html.EscapeString(d.Date), d.Count)
		}
		b.WriteString(`</table>`)
	} else if len(data.DailyTrend) > 0 {
		// 简版：一行汇总
		parts := make([]string, 0, len(data.DailyTrend))
		for _, d := range data.DailyTrend {
			parts = append(parts, fmt.Sprintf("%s:%d", d.Date[5:], d.Count))
		}
		fmt.Fprintf(&b, `<p style="font-size:13px;color:#444">日走势：%s</p>`, html.EscapeString(strings.Join(parts, " · ")))
	}

	// 2 排行榜
	b.WriteString(`<h2>2. 排行榜结构</h2>`)
	b.WriteString(`<h3 style="font-size:13px;margin:8px 0 4px">提交 Top</h3>`)
	writeRankTable(&b, data.TopSubmit, topN, "提交")
	b.WriteString(`<h3 style="font-size:13px;margin:8px 0 4px">AC Top</h3>`)
	writeRankTable(&b, data.TopAC, topN, "AC")

	// 3 知识点（规则侧无全量标签预取时提示可用 AI）
	b.WriteString(`<h2>3. 知识点 / 标签</h2>`)
	b.WriteString(`<p style="font-size:13px;color:#666">规则模板根据提交动态中的标签抽样观察；开启 AI 可调用 problem_tags 深挖成员画像。</p>`)
	tagHits := map[string]int{}
	for _, f := range data.OrgSubmitSample {
		for _, t := range f.Tags {
			t = strings.TrimSpace(t)
			if t != "" {
				tagHits[t]++
			}
		}
	}
	if len(tagHits) == 0 {
		b.WriteString(`<p style="color:#888;font-size:13px">本区间动态未带标签信息。</p>`)
	} else {
		type kv struct {
			k string
			v int
		}
		arr := make([]kv, 0, len(tagHits))
		for k, v := range tagHits {
			arr = append(arr, kv{k, v})
		}
		// 简单选择排序前 12
		for i := 0; i < len(arr); i++ {
			for j := i + 1; j < len(arr); j++ {
				if arr[j].v > arr[i].v {
					arr[i], arr[j] = arr[j], arr[i]
				}
			}
		}
		maxShow := 12
		if compact {
			maxShow = 6
		}
		for i, x := range arr {
			if i >= maxShow {
				break
			}
			fmt.Fprintf(&b, `<span class="tag">%s ×%d</span>`, html.EscapeString(x.k), x.v)
		}
	}

	// 4 提交动态
	b.WriteString(`<h2>4. 提交动态画像</h2>`)
	if len(data.OrgSubmitSample) == 0 {
		b.WriteString(`<p style="color:#888;font-size:13px">区间内暂无组织提交动态抽样。</p>`)
	} else {
		b.WriteString(`<table><tr><th>时间</th><th>成员</th><th>题</th><th>状态</th><th>平台</th></tr>`)
		n := 0
		for _, f := range data.OrgSubmitSample {
			if n >= feedN {
				break
			}
			name := f.UserName
			if name == "" {
				name = fmt.Sprintf("用户%d", f.UserID)
			}
			prob := f.Title
			if prob == "" {
				prob = f.Problem
			}
			fmt.Fprintf(&b, `<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>%s</td></tr>`,
				html.EscapeString(f.Time), html.EscapeString(name), html.EscapeString(prob),
				html.EscapeString(f.Status), html.EscapeString(f.Platform))
			n++
		}
		b.WriteString(`</table>`)
	}

	// 5 比赛
	b.WriteString(`<h2>5. 比赛表现</h2>`)
	if len(data.Contests) == 0 {
		b.WriteString(`<p style="color:#888;font-size:13px">区间内暂无组织比赛记录。</p>`)
	} else {
		b.WriteString(`<table><tr><th>比赛</th><th>平台</th><th>过题</th><th>日期</th></tr>`)
		n := 0
		for _, c := range data.Contests {
			if n >= contestN {
				break
			}
			fmt.Fprintf(&b, `<tr><td>%s</td><td>%s</td><td>%d/%d</td><td>%s</td></tr>`,
				html.EscapeString(c.ContestName), html.EscapeString(c.Platform),
				c.ACCount, c.TotalCount, html.EscapeString(c.Time))
			n++
		}
		b.WriteString(`</table>`)
	}
	if len(data.ContestRankings) > 0 {
		maxR := len(data.ContestRankings)
		if compact && maxR > 2 {
			maxR = 2
		}
		for i := 0; i < maxR; i++ {
			snap := data.ContestRankings[i]
			fmt.Fprintf(&b, `<h3 style="font-size:13px;margin:10px 0 4px">%s 组织榜 Top（共 %d 人）</h3>`,
				html.EscapeString(snap.ContestName), snap.Total)
			b.WriteString(`<table><tr><th>#</th><th>成员</th><th>过题</th><th>分</th></tr>`)
			rowN := 8
			if compact {
				rowN = 5
			}
			for j, r := range snap.Top {
				if j >= rowN {
					break
				}
				fmt.Fprintf(&b, `<tr><td>%d</td><td>%s</td><td>%d/%d</td><td>%d</td></tr>`,
					r.Rank, html.EscapeString(r.Name), r.ACCount, r.TotalCount, r.Score)
			}
			b.WriteString(`</table>`)
		}
	}

	// 6 博客
	b.WriteString(`<h2>6. 知识沉淀（博客）</h2>`)
	if len(data.RecentBlogs) == 0 {
		b.WriteString(`<p style="color:#888;font-size:13px">暂无组织博客摘要（AI 可调用 org_blogs 再查）。</p>`)
	} else {
		b.WriteString(`<table><tr><th>标题</th><th>作者</th><th>摘要</th></tr>`)
		n := 0
		for _, bl := range data.RecentBlogs {
			if n >= blogN {
				break
			}
			fmt.Fprintf(&b, `<tr><td>%s</td><td>%s</td><td>%s</td></tr>`,
				html.EscapeString(bl.Title), html.EscapeString(bl.Author), html.EscapeString(bl.Summary))
			n++
		}
		b.WriteString(`</table>`)
	}

	// 7 风险
	b.WriteString(`<h2>7. 风险成员（区间未提交）</h2>`)
	if len(data.InactiveMembers) == 0 {
		b.WriteString(`<p style="color:#888;font-size:13px">全员都有提交，给力！</p>`)
	} else {
		n := 0
		for _, name := range data.InactiveMembers {
			if n >= inactiveN {
				fmt.Fprintf(&b, `<span class="tag">…等共 %d 人</span>`, len(data.InactiveMembers))
				break
			}
			fmt.Fprintf(&b, `<span class="tag">%s</span>`, html.EscapeString(name))
			n++
		}
	}

	// 8 综合维度评价
	b.WriteString(`<h2>8. 综合维度评价</h2><div class="eval">`)
	for _, line := range dimLines {
		fmt.Fprintf(&b, `<div>%s</div>`, html.EscapeString(line))
	}
	fmt.Fprintf(&b, `<div style="margin-top:8px"><strong>总评 %s</strong></div>`, statusEmoji)
	b.WriteString(`<ul style="margin:8px 0 0;padding-left:18px">`)
	for _, a := range advice {
		fmt.Fprintf(&b, `<li>%s</li>`, html.EscapeString(a))
	}
	b.WriteString(`</ul></div>`)

	fmt.Fprintf(&b, `<div class="foot">由 %s 规则模板生成 · 仅使用真实统计数据，未编造名单与数字。</div>`, html.EscapeString(brand))
	b.WriteString(`</div></body></html>`)
	return b.String()
}

func writeRankTable(b *strings.Builder, rows []RankEntry, topN int, label string) {
	if len(rows) == 0 {
		fmt.Fprintf(b, `<p style="color:#888;font-size:13px">本区间暂无 %s 记录。</p>`, html.EscapeString(label))
		return
	}
	b.WriteString(`<table><tr><th>#</th><th>成员</th><th>`)
	b.WriteString(html.EscapeString(label))
	b.WriteString(`</th></tr>`)
	for i, r := range rows {
		if i >= topN {
			break
		}
		fmt.Fprintf(b, `<tr><td>%d</td><td>%s</td><td>%d</td></tr>`,
			r.Rank, html.EscapeString(r.Name), r.Score)
	}
	b.WriteString(`</table>`)
}

// ruleComprehensiveEval 规则侧综合维度评价
func ruleComprehensiveEval(data *TrainingReportData, delta int64) (emoji string, lines []string, advice []string) {
	emoji = "❄️"
	activeRatio := 0.0
	if data.MemberCount > 0 {
		activeRatio = float64(data.ActiveMembers) / float64(data.MemberCount)
	}
	if data.MemberCount > 0 && activeRatio >= 0.6 && delta >= 0 {
		emoji = "🔥"
	} else if activeRatio >= 0.3 || data.TotalSubmits > 0 {
		emoji = "⚠️"
	}

	lines = append(lines, fmt.Sprintf("· 活跃度：有提交 %d/%d（%.0f%%），提交环比 %+d",
		data.ActiveMembers, data.MemberCount, activeRatio*100, delta))
	lines = append(lines, fmt.Sprintf("· 正确率/AC：区间 AC 次数 %d，AC 榜首位 %s",
		data.TotalAC, firstRankName(data.TopAC)))
	lines = append(lines, "· 知识点：见上方标签抽样；可结合 AI 标签画像深挖薄弱点")
	lines = append(lines, fmt.Sprintf("· 比赛：区间记录 %d 场，重点榜 %d 场",
		len(data.Contests), len(data.ContestRankings)))
	lines = append(lines, fmt.Sprintf("· 博客沉淀：%d 篇摘要", len(data.RecentBlogs)))
	lines = append(lines, fmt.Sprintf("· 排行结构：提交 Top 人数 %d，AC Top 人数 %d",
		len(data.TopSubmit), len(data.TopAC)))
	lines = append(lines, fmt.Sprintf("· 风险：未提交 %d 人", len(data.InactiveMembers)))

	if data.TotalSubmits == 0 {
		advice = append(advice, "本区间零提交，建议组织统一训练日并检查账号绑定。")
	} else if delta < 0 {
		advice = append(advice, "提交量环比下降，可重点关注中腰部成员而非仅盯 Top。")
	} else {
		advice = append(advice, "提交量稳定或上升，继续保持节奏并巩固 AC 质量。")
	}
	if len(data.InactiveMembers) > 0 {
		advice = append(advice, fmt.Sprintf("有 %d 名成员本区间未提交，建议一对一跟进。", len(data.InactiveMembers)))
	}
	if len(data.Contests) == 0 {
		advice = append(advice, "区间内比赛记录偏少，可鼓励参加周赛/校赛积累榜单数据。")
	}
	if len(data.RecentBlogs) == 0 {
		advice = append(advice, "博客产出偏少，可推动 Top 选手写题解沉淀。")
	}
	if len(data.TopAC) > 0 {
		advice = append(advice, fmt.Sprintf("AC 之星 %s（%d），可请其分享题解或带训。", data.TopAC[0].Name, data.TopAC[0].Score))
	}
	if len(advice) > 3 {
		advice = advice[:3]
	}
	return emoji, lines, advice
}

func firstRankName(rows []RankEntry) string {
	if len(rows) == 0 {
		return "—"
	}
	return fmt.Sprintf("%s（%d）", rows[0].Name, rows[0].Score)
}

// trainingReportSystemPrompt AI 训练报告 / 周报
func trainingReportSystemPrompt(mode string) string {
	compact := mode == DetailModeCompact
	depth := "详版：可含表格、多场比赛、成员级点评。"
	if compact {
		depth = "简版（教练周报）：维度一个不少，但每维 2～4 句短评，名单 Top5，比赛最多 2～3 场。"
	}
	return fmt.Sprintf(`你是算法训练平台的教练助手，为教练/队长写组织训练报告。
要求：
1. 风格：Acmer 校园口语、简洁有力。
2. 只输出完整 HTML（可含 style），适配 PC/移动端。
3. 只能使用给定数据与工具返回的真实名单/数字/标签，禁止编造。
4. 可调用工具：org_members、rank、heatmap、submit_log、org_submit_feed、problem_tags、
   contest_list、contest_ranking、contest_board、contest_history、org_blogs 等。
5. 必须覆盖全部维度（即使某维数据为空也要说明「暂无」）：
   (1) 活跃度与趋势 (2) 排行榜结构 (3) 知识点/标签 (4) 提交动态画像
   (5) 比赛表现（过题数与组织榜） (6) 知识沉淀/博客 (7) 风险成员
   (8) 综合维度评价（必选收尾：各维一句 + 总评 🔥/⚠️/❄️ + 2～3 条可执行建议）
6. %s
7. 不要输出提示词，不要 Markdown 代码围栏。`, depth)
}

func trainingReportUserPrompt(data *TrainingReportData, mode string) string {
	label := "详版训练报告"
	if mode == DetailModeCompact {
		label = "简版教练周报（上周）"
	}
	return fmt.Sprintf(`请根据以下组织训练数据生成 %s HTML。
务必按 8 个维度组织，并以「综合维度评价」收尾。
需要更细数据时可 function call（如对 Top 成员 problem_tags.user_profile、对某场 contest_ranking/contest_board、org_blogs、org_submit_feed）。

范围：%s · 日期 %s ~ %s · 组织 %d · 组 %d
成员数 %d，有提交 %d，总提交 %d（上期 %d），总 AC %d
比赛场次预取 %d，重点榜 %d，博客 %d，动态抽样 %d

预置 JSON 数据（真实）：
%s`,
		label,
		data.ScopeLabel, data.StartDate, data.EndDate, data.OrgID, data.GroupID,
		data.MemberCount, data.ActiveMembers, data.TotalSubmits, data.PrevTotalSubmits, data.TotalAC,
		len(data.Contests), len(data.ContestRankings), len(data.RecentBlogs), len(data.OrgSubmitSample),
		mustJSON(data))
}

func mustJSON(v interface{}) string {
	b, err := jsonIndent(v)
	if err != nil {
		return "{}"
	}
	return b
}
