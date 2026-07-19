package service

import (
	"fmt"
	"html"
	"strings"
)

// RenderRuleTemplateHTML 非 AI：固定精美 HTML 模板 + 真实数据回填（PC/移动端响应式）。
// mode: full（后台详版）| compact（教练周报简版）。
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
	trendLabel, trendClass := "持平", "flat"
	if delta > 0 {
		trendLabel, trendClass = "上升", "up"
	} else if delta < 0 {
		trendLabel, trendClass = "下降", "down"
	}
	statusEmoji, dimLines, advice := ruleComprehensiveEval(data, delta)
	activeRatio := 0.0
	if data.MemberCount > 0 {
		activeRatio = float64(data.ActiveMembers) / float64(data.MemberCount) * 100
	}
	acRate := 0.0
	if data.TotalSubmits > 0 {
		acRate = float64(data.TotalAC) / float64(data.TotalSubmits) * 100
	}

	// 展示截断：详版尽量全量活跃榜；简版仍列活跃榜但上限
	rankCap, inactiveN, feedN, contestN, blogN, probN, tagN := 200, 50, 16, 8, 10, 18, 20
	if compact {
		rankCap, inactiveN, feedN, contestN, blogN, probN, tagN = 30, 15, 8, 3, 5, 10, 12
	}

	title, badge := "训练报告", "详版 · 规则模板"
	if compact {
		title, badge = "教练周报", "简版 · 上周"
	}

	maxDay := int64(1)
	for _, d := range data.DailyTrend {
		if d.Count > maxDay {
			maxDay = d.Count
		}
	}

	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">`)
	b.WriteString(`<meta name="viewport" content="width=device-width,initial-scale=1,viewport-fit=cover">`)
	b.WriteString(`<meta name="color-scheme" content="light">`)
	b.WriteString(`<title>`)
	b.WriteString(html.EscapeString(brand + " " + title))
	b.WriteString(`</title><style>`)
	b.WriteString(reportTemplateCSS())
	b.WriteString(`</style></head><body><div class="page">`)

	// Hero
	b.WriteString(`<header class="hero">`)
	fmt.Fprintf(&b, `<div class="hero-top"><span class="brand">%s</span><span class="badge">%s</span></div>`,
		html.EscapeString(brand), html.EscapeString(badge))
	fmt.Fprintf(&b, `<h1>%s <span class="emoji">%s</span></h1>`, html.EscapeString(title), statusEmoji)
	fmt.Fprintf(&b, `<p class="hero-meta">%s ~ %s · %s · 成员 %d · 活跃 %d · AC 率 %.1f%%</p>`,
		html.EscapeString(data.StartDate), html.EscapeString(data.EndDate),
		html.EscapeString(data.ScopeLabel), data.MemberCount, data.ActiveMembers, acRate)
	b.WriteString(`</header>`)

	// KPI — 响应式 2×2 / 4 列
	b.WriteString(`<section class="kpis" aria-label="核心指标">`)
	fmt.Fprintf(&b, `<div class="kpi"><div class="kpi-n">%d</div><div class="kpi-l">总提交</div><div class="kpi-sub %s">环比 %s %s</div></div>`,
		data.TotalSubmits, trendClass, deltaStr, trendLabel)
	fmt.Fprintf(&b, `<div class="kpi"><div class="kpi-n">%d</div><div class="kpi-l">AC 次数</div><div class="kpi-sub">AC 率 %.1f%%</div></div>`,
		data.TotalAC, acRate)
	fmt.Fprintf(&b, `<div class="kpi"><div class="kpi-n">%d<span class="kpi-den">/%d</span></div><div class="kpi-l">活跃成员</div><div class="kpi-sub">活跃率 %.0f%%</div></div>`,
		data.ActiveMembers, data.MemberCount, activeRatio)
	fmt.Fprintf(&b, `<div class="kpi"><div class="kpi-n">%d</div><div class="kpi-l">未提交</div><div class="kpi-sub">已剔除出活跃榜</div></div>`,
		len(data.InactiveMembers))
	b.WriteString(`</section>`)

	// 1 走势
	b.WriteString(`<section class="block"><div class="block-h"><h2>1. 活跃度与趋势</h2></div>`)
	if len(data.DailyTrend) > 0 {
		b.WriteString(`<div class="bars" role="img" aria-label="每日提交柱状图">`)
		for i, d := range data.DailyTrend {
			h := 8
			if maxDay > 0 {
				h = int(8 + float64(d.Count)/float64(maxDay)*72)
			}
			ac := int64(0)
			if i < len(data.DailyACTrend) {
				ac = data.DailyACTrend[i].Count
			}
			label := d.Date
			if len(label) >= 5 {
				label = label[5:]
			}
			fmt.Fprintf(&b, `<div class="bar-col" title="%s 提交%d AC%d"><div class="bar" style="height:%dpx"></div><div class="bar-v">%d</div><div class="bar-ac">AC %d</div><div class="bar-d">%s</div></div>`,
				html.EscapeString(d.Date), d.Count, ac, h, d.Count, ac, html.EscapeString(label))
		}
		b.WriteString(`</div>`)
	} else {
		b.WriteString(`<p class="empty">暂无日走势数据</p>`)
	}
	b.WriteString(`</section>`)

	// 2 全员活跃排行（剔除不活跃）
	b.WriteString(`<section class="block"><div class="block-h"><h2>2. 活跃成员排行榜</h2></div>`)
	b.WriteString(`<p class="hint">仅列出区间内有提交的成员；0 提交已剔除，见第 7 节。</p>`)
	ranking := data.ActiveRanking
	if len(ranking) == 0 {
		// 兼容旧数据：用 topSubmit 拼
		for _, r := range data.TopSubmit {
			ranking = append(ranking, MemberStat{Rank: r.Rank, UserID: r.UserID, Name: r.Name, Submits: r.Score})
		}
	}
	if len(ranking) == 0 {
		b.WriteString(`<p class="empty">本区间无人提交</p>`)
	} else {
		b.WriteString(`<div class="table-wrap"><table class="rank-table"><thead><tr>`)
		b.WriteString(`<th>#</th><th>成员</th><th>提交</th><th>AC</th><th>AC率</th><th class="hide-sm">占比</th>`)
		b.WriteString(`</tr></thead><tbody>`)
		for i, m := range ranking {
			if i >= rankCap {
				fmt.Fprintf(&b, `<tr><td colspan="6" class="muted">…共 %d 名活跃成员</td></tr>`, len(ranking))
				break
			}
			medal := ""
			if m.Rank == 1 {
				medal = " medal-g"
			} else if m.Rank == 2 {
				medal = " medal-s"
			} else if m.Rank == 3 {
				medal = " medal-b"
			}
			fmt.Fprintf(&b, `<tr><td class="rank%s">%d</td><td class="name">%s</td><td class="mono">%d</td><td class="mono">%d</td><td class="mono">%.1f%%</td><td class="mono hide-sm">%.1f%%</td></tr>`,
				medal, m.Rank, html.EscapeString(m.Name), m.Submits, m.AC, m.ACRate, m.Share)
		}
		b.WriteString(`</tbody></table></div>`)
	}
	b.WriteString(`</section>`)

	// 3 标签
	b.WriteString(`<section class="block"><div class="block-h"><h2>3. 知识点 / 题目标签</h2></div>`)
	tags := data.TeamTags
	if len(tags) == 0 {
		// 从动态现算
		tags = aggregateTeamTags(data.OrgSubmitSample, tagN)
	}
	if len(tags) == 0 {
		b.WriteString(`<p class="empty">区间动态未带标签（可能题目未入库关联）。开启 AI 可对 Top 成员调 problem_tags 画像。</p>`)
	} else {
		b.WriteString(`<div class="tags">`)
		for i, t := range tags {
			if i >= tagN {
				break
			}
			fmt.Fprintf(&b, `<span class="tag">%s <em>%d</em></span>`, html.EscapeString(t.Tag), t.Count)
		}
		b.WriteString(`</div>`)
	}
	b.WriteString(`</section>`)

	// 4 做题概览 + 动态
	b.WriteString(`<section class="block"><div class="block-h"><h2>4. 做题概览与提交动态</h2></div>`)
	probs := data.ProblemOverview
	if len(probs) == 0 {
		probs = aggregateProblemOverview(data.OrgSubmitSample, probN)
	}
	if len(probs) == 0 {
		b.WriteString(`<p class="empty">暂无做题抽样（区间内组织动态为空）。</p>`)
	} else {
		b.WriteString(`<h3 class="subh">热门题目（谁在交）</h3>`)
		b.WriteString(`<div class="table-wrap"><table><thead><tr><th>题目</th><th class="hide-sm">标签</th><th>尝试</th><th>AC</th><th>AC 成员</th></tr></thead><tbody>`)
		for i, p := range probs {
			if i >= probN {
				break
			}
			tagStr := strings.Join(p.Tags, " · ")
			if tagStr == "" {
				tagStr = "—"
			}
			acUsers := strings.Join(p.ACUsers, "、")
			if acUsers == "" {
				acUsers = "—"
			}
			fmt.Fprintf(&b, `<tr><td><div class="prob-t">%s</div><div class="muted hide-md">%s</div></td><td class="hide-sm muted">%s</td><td class="mono">%d</td><td class="mono">%d</td><td class="sm">%s</td></tr>`,
				html.EscapeString(p.Problem), html.EscapeString(p.Platform),
				html.EscapeString(tagStr), p.Submitters, p.ACCount, html.EscapeString(acUsers))
		}
		b.WriteString(`</tbody></table></div>`)
	}

	b.WriteString(`<h3 class="subh">近期提交动态（抽样）</h3>`)
	if len(data.OrgSubmitSample) == 0 {
		b.WriteString(`<p class="empty">暂无动态抽样。</p>`)
	} else {
		b.WriteString(`<div class="table-wrap"><table><thead><tr><th>时间</th><th>成员</th><th>题目</th><th>状态</th><th class="hide-sm">平台</th></tr></thead><tbody>`)
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
			stClass := "st-other"
			us := strings.ToUpper(f.Status)
			if strings.Contains(us, "AC") || us == "OK" || us == "ACCEPT" {
				stClass = "st-ac"
			} else if strings.Contains(us, "WA") || strings.Contains(us, "WRONG") {
				stClass = "st-wa"
			}
			fmt.Fprintf(&b, `<tr><td class="mono nowrap">%s</td><td>%s</td><td>%s</td><td><span class="pill %s">%s</span></td><td class="hide-sm">%s</td></tr>`,
				html.EscapeString(f.Time), html.EscapeString(name), html.EscapeString(prob),
				stClass, html.EscapeString(f.Status), html.EscapeString(f.Platform))
			n++
		}
		b.WriteString(`</tbody></table></div>`)
	}
	b.WriteString(`</section>`)

	// 5 比赛
	b.WriteString(`<section class="block"><div class="block-h"><h2>5. 比赛表现</h2></div>`)
	if len(data.Contests) == 0 {
		b.WriteString(`<p class="empty">区间内暂无组织比赛记录。</p>`)
	} else {
		b.WriteString(`<div class="table-wrap"><table><thead><tr><th>比赛</th><th>平台</th><th>过题</th><th>日期</th></tr></thead><tbody>`)
		n := 0
		for _, c := range data.Contests {
			if n >= contestN {
				break
			}
			fmt.Fprintf(&b, `<tr><td>%s</td><td>%s</td><td class="mono">%d<span class="muted">/%d</span></td><td class="mono nowrap">%s</td></tr>`,
				html.EscapeString(c.ContestName), html.EscapeString(c.Platform),
				c.ACCount, c.TotalCount, html.EscapeString(c.Time))
			n++
		}
		b.WriteString(`</tbody></table></div>`)
	}
	if len(data.ContestRankings) > 0 {
		maxR := len(data.ContestRankings)
		if compact && maxR > 2 {
			maxR = 2
		}
		for i := 0; i < maxR; i++ {
			snap := data.ContestRankings[i]
			fmt.Fprintf(&b, `<h3 class="subh">%s · 组织榜 <span class="muted">共 %d 人</span></h3>`,
				html.EscapeString(snap.ContestName), snap.Total)
			b.WriteString(`<div class="table-wrap"><table><thead><tr><th>#</th><th>成员</th><th>过题</th><th>得分</th></tr></thead><tbody>`)
			rowN := 12
			if compact {
				rowN = 6
			}
			for j, r := range snap.Top {
				if j >= rowN {
					break
				}
				fmt.Fprintf(&b, `<tr><td class="rank">%d</td><td>%s</td><td class="mono">%d<span class="muted">/%d</span></td><td class="mono">%d</td></tr>`,
					r.Rank, html.EscapeString(r.Name), r.ACCount, r.TotalCount, r.Score)
			}
			b.WriteString(`</tbody></table></div>`)
		}
	}
	b.WriteString(`</section>`)

	// 6 博客
	b.WriteString(`<section class="block"><div class="block-h"><h2>6. 知识沉淀（博客 / 推文）</h2></div>`)
	if len(data.RecentBlogs) == 0 {
		b.WriteString(`<p class="empty">暂无组织公开博客摘要。可鼓励队员写题解沉淀。</p>`)
	} else {
		b.WriteString(`<div class="blog-list">`)
		n := 0
		for _, bl := range data.RecentBlogs {
			if n >= blogN {
				break
			}
			fmt.Fprintf(&b, `<article class="blog-card"><h4>%s</h4><p class="blog-meta">%s</p><p class="blog-sum">%s</p></article>`,
				html.EscapeString(bl.Title), html.EscapeString(bl.Author), html.EscapeString(bl.Summary))
			n++
		}
		b.WriteString(`</div>`)
	}
	b.WriteString(`</section>`)

	// 7 不活跃（已从榜剔除）
	b.WriteString(`<section class="block"><div class="block-h"><h2>7. 不活跃成员（已从排行榜剔除）</h2></div>`)
	if len(data.InactiveMembers) == 0 {
		b.WriteString(`<p class="ok">全员都有提交，给力！</p>`)
	} else {
		b.WriteString(`<div class="tags">`)
		n := 0
		for _, name := range data.InactiveMembers {
			if n >= inactiveN {
				fmt.Fprintf(&b, `<span class="tag warn">…共 %d 人</span>`, len(data.InactiveMembers))
				break
			}
			fmt.Fprintf(&b, `<span class="tag warn">%s</span>`, html.EscapeString(name))
			n++
		}
		b.WriteString(`</div>`)
	}
	b.WriteString(`</section>`)

	// 8 综合评价
	b.WriteString(`<section class="block eval-block"><div class="block-h"><h2>8. 综合维度评价</h2></div><div class="eval">`)
	for _, line := range dimLines {
		fmt.Fprintf(&b, `<div class="eval-line">%s</div>`, html.EscapeString(line))
	}
	fmt.Fprintf(&b, `<div class="eval-total">总评 %s</div><ul class="advice">`, statusEmoji)
	for _, a := range advice {
		fmt.Fprintf(&b, `<li>%s</li>`, html.EscapeString(a))
	}
	b.WriteString(`</ul></div></section>`)

	fmt.Fprintf(&b, `<footer class="foot">由 %s 规则模板生成 · 响应式适配手机/PC · 仅回填真实数据</footer>`,
		html.EscapeString(brand))
	b.WriteString(`</div></body></html>`)
	return b.String()
}

func reportTemplateCSS() string {
	return `
*{box-sizing:border-box}
html{-webkit-text-size-adjust:100%}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;background:linear-gradient(165deg,#eef2ff 0%,#f8fafc 42%,#f1f5f9 100%);color:#0f172a;line-height:1.55;-webkit-font-smoothing:antialiased}
.page{max-width:820px;margin:0 auto;padding:16px 12px 36px;padding-left:max(12px,env(safe-area-inset-left));padding-right:max(12px,env(safe-area-inset-right))}
.hero{background:linear-gradient(135deg,#4f46e5 0%,#7c3aed 55%,#6366f1 100%);color:#fff;border-radius:16px;padding:18px 16px 16px;box-shadow:0 10px 32px rgba(79,70,229,.26);margin-bottom:12px}
.hero-top{display:flex;justify-content:space-between;align-items:center;gap:8px;flex-wrap:wrap;margin-bottom:6px}
.brand{font-weight:700;letter-spacing:.02em}
.badge{font-size:11px;background:rgba(255,255,255,.18);padding:4px 10px;border-radius:999px;white-space:nowrap}
.hero h1{margin:0 0 6px;font-size:clamp(1.15rem,4.5vw,1.5rem);font-weight:700;word-break:break-word}
.hero .emoji{font-size:1.1em}
.hero-meta{margin:0;font-size:12px;opacity:.92;line-height:1.45}
.kpis{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:8px;margin-bottom:12px}
.kpi{background:#fff;border-radius:12px;padding:12px 10px;box-shadow:0 1px 3px rgba(15,23,42,.06);border:1px solid rgba(15,23,42,.04);min-width:0}
.kpi-n{font-size:clamp(1.25rem,5vw,1.65rem);font-weight:800;letter-spacing:-.02em;color:#1e1b4b;word-break:break-all}
.kpi-den{font-size:.55em;font-weight:600;color:#94a3b8}
.kpi-l{font-size:12px;color:#64748b;margin-top:2px}
.kpi-sub{font-size:11px;margin-top:4px;color:#94a3b8}
.kpi-sub.up{color:#059669}.kpi-sub.down{color:#dc2626}
.block{background:#fff;border-radius:14px;padding:14px 12px 16px;margin-bottom:10px;box-shadow:0 1px 3px rgba(15,23,42,.05);border:1px solid rgba(15,23,42,.04)}
.block-h h2{margin:0 0 10px;font-size:14px;font-weight:700;color:#1e293b;display:flex;align-items:center;gap:8px}
.block-h h2:before{content:"";flex-shrink:0;width:4px;height:14px;border-radius:2px;background:linear-gradient(180deg,#6366f1,#8b5cf6)}
h3.subh,.subh{margin:12px 0 8px;font-size:13px;color:#334155;font-weight:600}
.hint{margin:0 0 10px;font-size:12px;color:#64748b}
.bars{display:flex;align-items:flex-end;gap:4px;min-height:120px;padding:6px 0 0;overflow-x:auto;-webkit-overflow-scrolling:touch;scrollbar-width:thin}
.bar-col{flex:1 0 36px;min-width:32px;display:flex;flex-direction:column;align-items:center;gap:2px}
.bar{width:100%;max-width:40px;background:linear-gradient(180deg,#818cf8,#4f46e5);border-radius:6px 6px 2px 2px;min-height:8px}
.bar-v{font-size:10px;color:#64748b;font-variant-numeric:tabular-nums}
.bar-ac{font-size:9px;color:#94a3b8}
.bar-d{font-size:10px;color:#94a3b8}
.table-wrap{overflow-x:auto;-webkit-overflow-scrolling:touch;border-radius:10px;border:1px solid #e2e8f0;max-width:100%}
table{width:100%;border-collapse:collapse;font-size:12.5px;min-width:280px}
.rank-table{min-width:420px}
th{text-align:left;padding:9px 8px;background:#f8fafc;color:#64748b;font-weight:600;font-size:11px;border-bottom:1px solid #e2e8f0;white-space:nowrap}
td{padding:8px;border-bottom:1px solid #f1f5f9;vertical-align:middle}
tr:last-child td{border-bottom:none}
.name{font-weight:600;color:#1e293b}
.mono{font-variant-numeric:tabular-nums;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
.muted{color:#94a3b8}
.sm{font-size:11px;line-height:1.4;word-break:break-word}
.nowrap{white-space:nowrap}
.rank{font-weight:700;color:#64748b}
.rank.medal-g{color:#d97706}.rank.medal-s{color:#64748b}.rank.medal-b{color:#b45309}
.tags{display:flex;flex-wrap:wrap;gap:6px}
.tag{display:inline-flex;align-items:center;gap:4px;background:#eef2ff;color:#3730a3;border-radius:999px;padding:4px 10px;font-size:12px;max-width:100%}
.tag em{font-style:normal;font-weight:700;color:#4f46e5}
.tag.warn{background:#fef3c7;color:#92400e}
.pill{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600;white-space:nowrap}
.st-ac{background:#d1fae5;color:#065f46}.st-wa{background:#fee2e2;color:#991b1b}.st-other{background:#f1f5f9;color:#475569}
.prob-t{font-weight:600;word-break:break-word}
.blog-list{display:grid;gap:8px}
.blog-card{border:1px solid #e2e8f0;border-radius:12px;padding:12px;background:#fafbff}
.blog-card h4{margin:0 0 4px;font-size:14px;word-break:break-word}
.blog-meta{margin:0 0 4px;font-size:12px;color:#64748b}
.blog-sum{margin:0;font-size:12px;color:#475569;word-break:break-word}
.empty{margin:0;font-size:13px;color:#94a3b8}
.ok{margin:0;font-size:13px;color:#059669;font-weight:600}
.eval-block{border:1px solid #c7d2fe;background:linear-gradient(180deg,#fafafe,#fff)}
.eval{font-size:13px;line-height:1.7}
.eval-line{color:#334155;word-break:break-word}
.eval-total{margin-top:10px;font-size:15px;font-weight:700;color:#312e81}
.advice{margin:8px 0 0;padding-left:18px;color:#475569}
.foot{text-align:center;font-size:11px;color:#94a3b8;margin-top:6px;padding:8px;line-height:1.4}
/* 手机默认隐藏次要列 */
.hide-sm{display:none}
.hide-md{display:block}
@media(min-width:480px){
  .kpis{grid-template-columns:repeat(4,minmax(0,1fr));gap:10px}
  .page{padding:20px 16px 40px}
  .block{padding:16px 16px 18px;border-radius:16px}
  .hero{padding:22px 20px 18px;border-radius:18px}
  table{font-size:13px}
  th,td{padding:9px 10px}
}
@media(min-width:640px){
  .hide-sm{display:table-cell}
  .hide-md{display:none}
  .bar-col{flex:1;min-width:28px}
}
@media(max-width:380px){
  .kpi-n{font-size:1.15rem}
  .bar-col{flex:1 0 30px;min-width:28px}
}
`
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

	acRate := 0.0
	if data.TotalSubmits > 0 {
		acRate = float64(data.TotalAC) / float64(data.TotalSubmits) * 100
	}
	topName := "—"
	if len(data.ActiveRanking) > 0 {
		topName = fmt.Sprintf("%s（提交 %d / AC %d）", data.ActiveRanking[0].Name, data.ActiveRanking[0].Submits, data.ActiveRanking[0].AC)
	} else if len(data.TopAC) > 0 {
		topName = firstRankName(data.TopAC)
	}

	lines = append(lines, fmt.Sprintf("· 活跃度：有提交 %d/%d（%.0f%%），提交环比 %+d",
		data.ActiveMembers, data.MemberCount, activeRatio*100, delta))
	lines = append(lines, fmt.Sprintf("· 正确率/AC：区间 AC %d，整体 AC 率 %.1f%%，榜首 %s",
		data.TotalAC, acRate, topName))
	tagN := len(data.TeamTags)
	if tagN == 0 {
		tagN = len(aggregateTeamTags(data.OrgSubmitSample, 99))
	}
	lines = append(lines, fmt.Sprintf("· 知识点：团队标签 %d 种（见第 3 节）", tagN))
	lines = append(lines, fmt.Sprintf("· 做题/动态：概览 %d 题，动态抽样 %d 条",
		len(data.ProblemOverview), len(data.OrgSubmitSample)))
	lines = append(lines, fmt.Sprintf("· 比赛：区间 %d 场，重点榜 %d 场",
		len(data.Contests), len(data.ContestRankings)))
	lines = append(lines, fmt.Sprintf("· 博客沉淀：%d 篇", len(data.RecentBlogs)))
	lines = append(lines, fmt.Sprintf("· 排行：活跃榜 %d 人（已剔除 0 提交），不活跃 %d 人",
		len(data.ActiveRanking), len(data.InactiveMembers)))

	if data.TotalSubmits == 0 {
		advice = append(advice, "本区间零提交，建议组织统一训练日并检查账号绑定。")
	} else if delta < 0 {
		advice = append(advice, "提交量环比下降，可重点关注中腰部成员而非仅盯 Top。")
	} else {
		advice = append(advice, "提交量稳定或上升，继续保持节奏并巩固 AC 质量。")
	}
	if len(data.InactiveMembers) > 0 {
		advice = append(advice, fmt.Sprintf("有 %d 名成员本区间未提交（已从榜剔除），建议一对一跟进。", len(data.InactiveMembers)))
	}
	if tagN == 0 {
		advice = append(advice, "标签数据偏少，可督促做题关联题库以便知识点分析。")
	}
	if len(data.RecentBlogs) == 0 {
		advice = append(advice, "博客产出偏少，可推动 Top 选手写题解沉淀。")
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
	depth := `详版：必须输出完整 activeRanking 表格（全部有提交成员，禁止只写 Top5/Top10）；
表格含 提交/AC/AC率；不活跃只放第 7 节。知识点必须用 teamTags 或调 problem_tags。
做题概览用 problemOverview；动态用 orgSubmitSample；博客用 recentBlogs。`
	if compact {
		depth = `简版（教练周报）：维度齐全但篇幅短；活跃榜可截前 15 名并注明总数；
不活跃单独一节；标签/做题/动态/博客都要点到。`
	}
	return fmt.Sprintf(`你是算法训练平台的教练助手，写组织训练报告 HTML。

【硬性要求】
1. 只输出完整 HTML（含 <style>），禁止 Markdown 围栏、禁止编造数据。
2. 响应式：必须含 viewport meta；用 flex/grid + media query；手机单列/两列 KPI，宽屏多列；
   表格包在 overflow-x:auto 容器内，避免手机横向撑破；字号用 clamp 或 rem；padding 适配小屏。
3. 视觉：分区卡片、清晰 h2、表格、标签 pill、AC/WA 状态色。
4. 数据源：只用用户消息 JSON 与工具返回。字段说明：
   - activeRanking：全部活跃成员（已剔除 0 提交）——排行榜必须用这个
   - inactiveMembers：不活跃，不得混进排行榜
   - teamTags / problemOverview / orgSubmitSample / recentBlogs / contests
5. 必须 8 个章节（空数据写「暂无」并说明原因）：
   (1) 活跃度与趋势（可用 dailyTrend + dailyAcTrend）
   (2) 活跃成员排行榜（全表或简版截断，禁止假装只有 10 人）
   (3) 知识点/题目标签（teamTags；不足可对 Top 成员 problem_tags.user_profile）
   (4) 做题概览 + 提交动态（谁交了啥题；orgSubmitSample）
   (5) 比赛（过题数、组织榜）
   (6) 博客/推文
   (7) 不活跃成员（已从榜剔除）
   (8) 综合维度评价（各维一句 + 🔥/⚠️/❄️ + 2～3 条建议）
6. %s
7. 可调工具：problem_tags、org_submit_feed、org_blogs、contest_list、contest_ranking、contest_board、rank、heatmap。
8. 不要输出提示词本身。`, depth)
}

func trainingReportUserPrompt(data *TrainingReportData, mode string) string {
	label := "详版训练报告"
	if mode == DetailModeCompact {
		label = "简版教练周报（上周）"
	}
	return fmt.Sprintf(`生成 %s 的完整响应式 HTML。

【排行榜】activeRanking 共 %d 人（全部有提交），必须完整呈现或按简版规则截断并写「共 N 人」。
【不活跃】inactiveMembers 共 %d 人，单独成节，禁止出现在排行榜。
【标签】teamTags 共 %d；【做题】problemOverview 共 %d；【动态】orgSubmitSample 共 %d；【博客】recentBlogs 共 %d。
若标签为空可对 activeRanking 前 3 名调 problem_tags.user_profile。

范围：%s · %s ~ %s · org=%d group=%d
成员 %d · 活跃 %d · 提交 %d（上期 %d）· AC %d

预置 JSON：
%s`,
		label,
		len(data.ActiveRanking), len(data.InactiveMembers),
		len(data.TeamTags), len(data.ProblemOverview), len(data.OrgSubmitSample), len(data.RecentBlogs),
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
