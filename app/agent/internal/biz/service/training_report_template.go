package service

import (
	"fmt"
	"html"
	"strings"
)

// RenderRuleTemplateHTML 非 AI：固定精美 HTML 模板 + 真实数据回填。
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
	trendLabel := "持平"
	trendClass := "flat"
	if delta > 0 {
		trendLabel = "上升"
		trendClass = "up"
	} else if delta < 0 {
		trendLabel = "下降"
		trendClass = "down"
	}
	statusEmoji, dimLines, advice := ruleComprehensiveEval(data, delta)
	activeRatio := 0.0
	if data.MemberCount > 0 {
		activeRatio = float64(data.ActiveMembers) / float64(data.MemberCount) * 100
	}

	topN, inactiveN, feedN, contestN, blogN := 10, 40, 12, 8, 8
	if compact {
		topN, inactiveN, feedN, contestN, blogN = 5, 12, 6, 3, 5
	}

	title := "训练报告"
	badge := "详版 · 规则模板"
	if compact {
		title = "教练周报"
		badge = "简版 · 上周"
	}

	// 日走势迷你柱
	maxDay := int64(1)
	for _, d := range data.DailyTrend {
		if d.Count > maxDay {
			maxDay = d.Count
		}
	}

	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html lang="zh-CN"><head><meta charset="utf-8">`)
	b.WriteString(`<meta name="viewport" content="width=device-width,initial-scale=1">`)
	b.WriteString(`<title>`)
	b.WriteString(html.EscapeString(brand + " " + title))
	b.WriteString(`</title><style>`)
	b.WriteString(reportTemplateCSS())
	b.WriteString(`</style></head><body><div class="page">`)

	// Header
	b.WriteString(`<header class="hero">`)
	fmt.Fprintf(&b, `<div class="hero-top"><span class="brand">%s</span><span class="badge">%s</span></div>`,
		html.EscapeString(brand), html.EscapeString(badge))
	fmt.Fprintf(&b, `<h1>%s <span class="emoji">%s</span></h1>`, html.EscapeString(title), statusEmoji)
	fmt.Fprintf(&b, `<p class="hero-meta">%s ~ %s · %s · 成员 %d 人 · 活跃 %.0f%%</p>`,
		html.EscapeString(data.StartDate), html.EscapeString(data.EndDate),
		html.EscapeString(data.ScopeLabel), data.MemberCount, activeRatio)
	b.WriteString(`</header>`)

	// KPI cards
	b.WriteString(`<section class="kpis">`)
	fmt.Fprintf(&b, `<div class="kpi"><div class="kpi-n">%d</div><div class="kpi-l">总提交</div><div class="kpi-sub %s">环比 %s %s</div></div>`,
		data.TotalSubmits, trendClass, deltaStr, trendLabel)
	fmt.Fprintf(&b, `<div class="kpi"><div class="kpi-n">%d</div><div class="kpi-l">AC 次数</div><div class="kpi-sub">上期提交 %d</div></div>`,
		data.TotalAC, data.PrevTotalSubmits)
	fmt.Fprintf(&b, `<div class="kpi"><div class="kpi-n">%d<span class="kpi-den">/%d</span></div><div class="kpi-l">有提交成员</div><div class="kpi-sub">活跃率 %.0f%%</div></div>`,
		data.ActiveMembers, data.MemberCount, activeRatio)
	fmt.Fprintf(&b, `<div class="kpi"><div class="kpi-n">%d</div><div class="kpi-l">未提交</div><div class="kpi-sub">需跟进名单见下</div></div>`,
		len(data.InactiveMembers))
	b.WriteString(`</section>`)

	// 1 走势
	b.WriteString(`<section class="block"><div class="block-h"><h2>1. 活跃度与趋势</h2></div>`)
	if len(data.DailyTrend) > 0 {
		b.WriteString(`<div class="bars">`)
		for _, d := range data.DailyTrend {
			h := 8
			if maxDay > 0 {
				h = int(8 + float64(d.Count)/float64(maxDay)*72)
			}
			label := d.Date
			if len(label) >= 5 {
				label = label[5:]
			}
			fmt.Fprintf(&b, `<div class="bar-col" title="%s: %d"><div class="bar" style="height:%dpx"></div><div class="bar-v">%d</div><div class="bar-d">%s</div></div>`,
				html.EscapeString(d.Date), d.Count, h, d.Count, html.EscapeString(label))
		}
		b.WriteString(`</div>`)
	} else {
		b.WriteString(`<p class="empty">暂无日走势数据</p>`)
	}
	b.WriteString(`</section>`)

	// 2 排行
	b.WriteString(`<section class="block"><div class="block-h"><h2>2. 排行榜结构</h2></div><div class="split">`)
	b.WriteString(`<div><h3>提交 Top</h3>`)
	writePrettyRank(&b, data.TopSubmit, topN, "次")
	b.WriteString(`</div><div><h3>AC Top</h3>`)
	writePrettyRank(&b, data.TopAC, topN, "题次")
	b.WriteString(`</div></div></section>`)

	// 3 标签
	b.WriteString(`<section class="block"><div class="block-h"><h2>3. 知识点 / 标签</h2></div>`)
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
		b.WriteString(`<p class="empty">本区间动态未带标签；开启 AI 可深挖成员标签画像。</p>`)
	} else {
		type kv struct {
			k string
			v int
		}
		arr := make([]kv, 0, len(tagHits))
		for k, v := range tagHits {
			arr = append(arr, kv{k, v})
		}
		for i := 0; i < len(arr); i++ {
			for j := i + 1; j < len(arr); j++ {
				if arr[j].v > arr[i].v {
					arr[i], arr[j] = arr[j], arr[i]
				}
			}
		}
		maxShow := 14
		if compact {
			maxShow = 8
		}
		b.WriteString(`<div class="tags">`)
		for i, x := range arr {
			if i >= maxShow {
				break
			}
			fmt.Fprintf(&b, `<span class="tag">%s <em>%d</em></span>`, html.EscapeString(x.k), x.v)
		}
		b.WriteString(`</div>`)
	}
	b.WriteString(`</section>`)

	// 4 动态
	b.WriteString(`<section class="block"><div class="block-h"><h2>4. 提交动态画像</h2></div>`)
	if len(data.OrgSubmitSample) == 0 {
		b.WriteString(`<p class="empty">区间内暂无组织提交动态抽样。</p>`)
	} else {
		b.WriteString(`<div class="table-wrap"><table><thead><tr><th>时间</th><th>成员</th><th>题目</th><th>状态</th><th>平台</th></tr></thead><tbody>`)
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
			fmt.Fprintf(&b, `<tr><td class="mono">%s</td><td>%s</td><td>%s</td><td><span class="pill %s">%s</span></td><td>%s</td></tr>`,
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
			fmt.Fprintf(&b, `<tr><td>%s</td><td>%s</td><td class="mono">%d<span class="muted">/%d</span></td><td class="mono">%s</td></tr>`,
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
			fmt.Fprintf(&b, `<h3 class="subh">%s · 组织榜 Top <span class="muted">共 %d 人</span></h3>`,
				html.EscapeString(snap.ContestName), snap.Total)
			b.WriteString(`<div class="table-wrap"><table><thead><tr><th>#</th><th>成员</th><th>过题</th><th>得分</th></tr></thead><tbody>`)
			rowN := 8
			if compact {
				rowN = 5
			}
			for j, r := range snap.Top {
				if j >= rowN {
					break
				}
				medal := ""
				if j == 0 {
					medal = " medal-g"
				} else if j == 1 {
					medal = " medal-s"
				} else if j == 2 {
					medal = " medal-b"
				}
				fmt.Fprintf(&b, `<tr><td class="rank%s">%d</td><td>%s</td><td class="mono">%d<span class="muted">/%d</span></td><td class="mono">%d</td></tr>`,
					medal, r.Rank, html.EscapeString(r.Name), r.ACCount, r.TotalCount, r.Score)
			}
			b.WriteString(`</tbody></table></div>`)
		}
	}
	b.WriteString(`</section>`)

	// 6 博客
	b.WriteString(`<section class="block"><div class="block-h"><h2>6. 知识沉淀（博客）</h2></div>`)
	if len(data.RecentBlogs) == 0 {
		b.WriteString(`<p class="empty">暂无组织博客摘要。可鼓励队员写题解沉淀。</p>`)
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

	// 7 风险
	b.WriteString(`<section class="block"><div class="block-h"><h2>7. 风险成员（区间未提交）</h2></div>`)
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

	fmt.Fprintf(&b, `<footer class="foot">由 %s 规则模板生成 · 仅回填真实统计，未编造名单与数字</footer>`,
		html.EscapeString(brand))
	b.WriteString(`</div></body></html>`)
	return b.String()
}

func reportTemplateCSS() string {
	return `
*{box-sizing:border-box}
body{margin:0;font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"PingFang SC","Hiragino Sans GB","Microsoft YaHei",sans-serif;background:linear-gradient(165deg,#eef2ff 0%,#f8fafc 40%,#f1f5f9 100%);color:#0f172a;line-height:1.55}
.page{max-width:760px;margin:0 auto;padding:20px 14px 40px}
.hero{background:linear-gradient(135deg,#4f46e5 0%,#7c3aed 55%,#6366f1 100%);color:#fff;border-radius:18px;padding:22px 22px 20px;box-shadow:0 12px 40px rgba(79,70,229,.28);margin-bottom:16px}
.hero-top{display:flex;justify-content:space-between;align-items:center;margin-bottom:8px}
.brand{font-weight:700;letter-spacing:.02em;opacity:.95}
.badge{font-size:11px;background:rgba(255,255,255,.18);padding:4px 10px;border-radius:999px;backdrop-filter:blur(4px)}
.hero h1{margin:0 0 6px;font-size:24px;font-weight:700}
.hero .emoji{font-size:22px}
.hero-meta{margin:0;font-size:13px;opacity:.9}
.kpis{display:grid;grid-template-columns:repeat(4,1fr);gap:10px;margin-bottom:14px}
.kpi{background:#fff;border-radius:14px;padding:14px 12px;box-shadow:0 1px 3px rgba(15,23,42,.06);border:1px solid rgba(15,23,42,.04)}
.kpi-n{font-size:26px;font-weight:800;letter-spacing:-.02em;color:#1e1b4b}
.kpi-den{font-size:14px;font-weight:600;color:#94a3b8}
.kpi-l{font-size:12px;color:#64748b;margin-top:2px}
.kpi-sub{font-size:11px;margin-top:6px;color:#94a3b8}
.kpi-sub.up{color:#059669}.kpi-sub.down{color:#dc2626}
.block{background:#fff;border-radius:16px;padding:16px 18px 18px;margin-bottom:12px;box-shadow:0 1px 3px rgba(15,23,42,.05);border:1px solid rgba(15,23,42,.04)}
.block-h h2{margin:0 0 12px;font-size:15px;font-weight:700;color:#1e293b;display:flex;align-items:center;gap:8px}
.block-h h2:before{content:"";width:4px;height:14px;border-radius:2px;background:linear-gradient(180deg,#6366f1,#8b5cf6)}
h3{margin:0 0 8px;font-size:13px;color:#475569;font-weight:600}
.subh{margin:14px 0 8px;font-size:13px;color:#334155}
.split{display:grid;grid-template-columns:1fr 1fr;gap:14px}
.bars{display:flex;align-items:flex-end;gap:6px;min-height:110px;padding:8px 0 0;overflow-x:auto}
.bar-col{flex:1;min-width:28px;display:flex;flex-direction:column;align-items:center;gap:4px}
.bar{width:100%;max-width:36px;background:linear-gradient(180deg,#818cf8,#4f46e5);border-radius:6px 6px 2px 2px;min-height:8px}
.bar-v{font-size:10px;color:#64748b;font-variant-numeric:tabular-nums}
.bar-d{font-size:10px;color:#94a3b8}
.table-wrap{overflow-x:auto;border-radius:10px;border:1px solid #e2e8f0}
table{width:100%;border-collapse:collapse;font-size:13px}
th{text-align:left;padding:10px 10px;background:#f8fafc;color:#64748b;font-weight:600;font-size:12px;border-bottom:1px solid #e2e8f0}
td{padding:9px 10px;border-bottom:1px solid #f1f5f9;vertical-align:middle}
tr:last-child td{border-bottom:none}
.mono{font-variant-numeric:tabular-nums;font-family:ui-monospace,SFMono-Regular,Menlo,monospace;font-size:12px}
.muted{color:#94a3b8}
.rank{font-weight:700;color:#64748b}
.rank.medal-g{color:#d97706}.rank.medal-s{color:#64748b}.rank.medal-b{color:#b45309}
.tags{display:flex;flex-wrap:wrap;gap:6px}
.tag{display:inline-flex;align-items:center;gap:4px;background:#eef2ff;color:#3730a3;border-radius:999px;padding:4px 10px;font-size:12px}
.tag em{font-style:normal;font-weight:700;color:#4f46e5}
.tag.warn{background:#fef3c7;color:#92400e}
.pill{display:inline-block;padding:2px 8px;border-radius:999px;font-size:11px;font-weight:600}
.st-ac{background:#d1fae5;color:#065f46}.st-wa{background:#fee2e2;color:#991b1b}.st-other{background:#f1f5f9;color:#475569}
.blog-list{display:grid;gap:8px}
.blog-card{border:1px solid #e2e8f0;border-radius:12px;padding:12px 14px;background:#fafbff}
.blog-card h4{margin:0 0 4px;font-size:14px}
.blog-meta{margin:0 0 4px;font-size:12px;color:#64748b}
.blog-sum{margin:0;font-size:12px;color:#475569}
.empty{margin:0;font-size:13px;color:#94a3b8}
.ok{margin:0;font-size:13px;color:#059669;font-weight:600}
.eval-block{border:1px solid #c7d2fe;background:linear-gradient(180deg,#fafafe,#fff)}
.eval{font-size:13px;line-height:1.7}
.eval-line{color:#334155}
.eval-total{margin-top:10px;font-size:15px;font-weight:700;color:#312e81}
.advice{margin:8px 0 0;padding-left:18px;color:#475569}
.foot{text-align:center;font-size:11px;color:#94a3b8;margin-top:8px;padding:8px}
@media(max-width:560px){
  .kpis{grid-template-columns:1fr 1fr}
  .split{grid-template-columns:1fr}
  .hero h1{font-size:20px}
  .kpi-n{font-size:22px}
}
`
}

func writePrettyRank(b *strings.Builder, rows []RankEntry, topN int, unit string) {
	if len(rows) == 0 {
		b.WriteString(`<p class="empty">本区间暂无记录</p>`)
		return
	}
	b.WriteString(`<ol class="rank-list">`)
	max := int64(1)
	for _, r := range rows {
		if r.Score > max {
			max = r.Score
		}
	}
	for i, r := range rows {
		if i >= topN {
			break
		}
		pct := 8
		if max > 0 {
			pct = int(8 + float64(r.Score)/float64(max)*92)
		}
		fmt.Fprintf(b, `<li><div class="rl-row"><span class="rl-name">%s</span><span class="rl-score">%d %s</span></div><div class="rl-track"><div class="rl-fill" style="width:%d%%"></div></div></li>`,
			html.EscapeString(r.Name), r.Score, html.EscapeString(unit), pct)
	}
	b.WriteString(`</ol><style>
.rank-list{list-style:none;margin:0;padding:0}
.rank-list li{margin-bottom:10px}
.rl-row{display:flex;justify-content:space-between;font-size:13px;margin-bottom:4px}
.rl-name{font-weight:600;color:#1e293b}
.rl-score{color:#64748b;font-variant-numeric:tabular-nums;font-size:12px}
.rl-track{height:6px;background:#eef2ff;border-radius:999px;overflow:hidden}
.rl-fill{height:100%;background:linear-gradient(90deg,#818cf8,#4f46e5);border-radius:999px}
</style>`)
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
	depth := "详版：可含表格、多场比赛、成员级点评。HTML 请做得美观（卡片/表格/清晰标题），不要纯纯文本堆砌。"
	if compact {
		depth = "简版（教练周报）：维度一个不少，但每维 2～4 句短评，名单 Top5，比赛最多 2～3 场。HTML 简洁美观。"
	}
	return fmt.Sprintf(`你是算法训练平台的教练助手，为教练/队长写组织训练报告。
要求：
1. 风格：Acmer 校园口语、简洁有力。
2. 只输出完整 HTML（可含 style），适配 PC/移动端，视觉清晰（分区卡片、表格、标签）。
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
