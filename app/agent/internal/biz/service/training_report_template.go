package service

import (
	"fmt"
	"html"
	"strings"
)

// RenderRuleTemplateHTML 非 AI：邮件/QQ 可渲染的表格布局 + 内联样式 + 主站链接。
// mode: full | compact
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
	if delta > 0 {
		trendLabel = "上升"
	} else if delta < 0 {
		trendLabel = "下降"
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

	rankCap, inactiveN, feedN, contestN, blogN, probN, tagN := 80, 40, 12, 10, 8, 12, 16
	if compact {
		rankCap, inactiveN, feedN, contestN, blogN, probN, tagN = 20, 12, 6, 5, 5, 8, 10
	}

	title, badge := "训练报告", "规则模板"
	if compact {
		title, badge = "教练周报", "上周简版"
	}

	moreAct := SiteBaseURL + "/all-activities"
	moreContest := SiteBaseURL + "/contest"
	moreBlog := SiteBaseURL + "/blog-plaza"
	moreHome := SiteBaseURL + "/"

	// —— 纯表格 + 内联样式（QQ 内置浏览器 / 邮件客户端兼容）——
	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html><head><meta charset="utf-8">`)
	b.WriteString(`<meta name="viewport" content="width=device-width,initial-scale=1">`)
	fmt.Fprintf(&b, `<title>%s %s</title></head>`, html.EscapeString(brand), html.EscapeString(title))
	b.WriteString(`<body style="margin:0;padding:0;background:#fafafa;font-family:ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue","PingFang SC","Microsoft YaHei",Arial,sans-serif;font-size:14px;line-height:1.5;color:#0a0a0a;">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#fafafa;"><tr><td align="center" style="padding:12px 8px;">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;width:100%;background:#ffffff;border:1px solid #e5e5e5;border-radius:10px;overflow:hidden;">`)

	// Header — shadcn primary
	b.WriteString(`<tr><td style="background:#171717;color:#fafafa;padding:20px 18px;">`)
	fmt.Fprintf(&b, `<div style="font-size:12px;font-weight:500;opacity:0.85;">%s · %s</div>`, html.EscapeString(brand), html.EscapeString(badge))
	fmt.Fprintf(&b, `<div style="font-size:20px;font-weight:600;letter-spacing:-0.02em;margin:6px 0 4px;">%s %s</div>`, html.EscapeString(title), statusEmoji)
	fmt.Fprintf(&b, `<div style="font-size:12px;opacity:0.8;">%s ~ %s · %s · 成员%d · 活跃%d · AC率%.1f%%</div>`,
		html.EscapeString(data.StartDate), html.EscapeString(data.EndDate), html.EscapeString(data.ScopeLabel),
		data.MemberCount, data.ActiveMembers, acRate)
	fmt.Fprintf(&b, `<div style="font-size:11px;margin-top:10px;"><a href="%s" style="color:#fafafa;text-decoration:underline;text-underline-offset:2px;">打开主站</a></div>`, moreHome)
	b.WriteString(`</td></tr>`)

	// KPI 2x2 table（table 布局，QQ 友好）
	b.WriteString(`<tr><td style="padding:16px 14px 4px;background:#ffffff;">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="6" border="0"><tr>`)
	fmt.Fprintf(&b, `<td width="50%%" valign="top" style="background:#f5f5f5;border:1px solid #e5e5e5;border-radius:10px;padding:12px 10px;"><div style="font-size:22px;font-weight:600;color:#0a0a0a;">%d</div><div style="font-size:12px;color:#737373;">总提交</div><div style="font-size:11px;color:#737373;margin-top:4px;">环比 %s %s · 上期 %d</div></td>`,
		data.TotalSubmits, html.EscapeString(deltaStr), html.EscapeString(trendLabel), data.PrevTotalSubmits)
	fmt.Fprintf(&b, `<td width="50%%" valign="top" style="background:#f5f5f5;border:1px solid #e5e5e5;border-radius:10px;padding:12px 10px;"><div style="font-size:22px;font-weight:600;color:#0a0a0a;">%d</div><div style="font-size:12px;color:#737373;">AC 次数</div><div style="font-size:11px;color:#737373;margin-top:4px;">AC率 %.1f%%</div></td>`,
		data.TotalAC, acRate)
	b.WriteString(`</tr><tr>`)
	fmt.Fprintf(&b, `<td width="50%%" valign="top" style="background:#f5f5f5;border:1px solid #e5e5e5;border-radius:10px;padding:12px 10px;"><div style="font-size:22px;font-weight:600;color:#0a0a0a;">%d<span style="font-size:12px;color:#737373;">/%d</span></div><div style="font-size:12px;color:#737373;">活跃成员</div><div style="font-size:11px;color:#737373;margin-top:4px;">活跃率 %.0f%%</div></td>`,
		data.ActiveMembers, data.MemberCount, activeRatio)
	fmt.Fprintf(&b, `<td width="50%%" valign="top" style="background:#f5f5f5;border:1px solid #e5e5e5;border-radius:10px;padding:12px 10px;"><div style="font-size:22px;font-weight:600;color:#0a0a0a;">%d</div><div style="font-size:12px;color:#737373;">未提交</div><div style="font-size:11px;color:#737373;margin-top:4px;">已剔除出活跃榜</div></td>`,
		len(data.InactiveMembers))
	b.WriteString(`</tr></table></td></tr>`)

	// 1 走势
	sectionStart(&b, "1. 活跃度与趋势")
	if len(data.DailyTrend) == 0 {
		emptyRow(&b, "暂无日走势")
	} else {
		b.WriteString(`<table width="100%" cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse;font-size:13px;">`)
		b.WriteString(`<tr style="background:#f5f5f5;"><th align="left" style="border-bottom:1px solid #e5e5e5;">日期</th><th align="right" style="border-bottom:1px solid #e5e5e5;">提交</th><th align="right" style="border-bottom:1px solid #e5e5e5;">AC</th></tr>`)
		for i, d := range data.DailyTrend {
			ac := int64(0)
			if i < len(data.DailyACTrend) {
				ac = data.DailyACTrend[i].Count
			}
			fmt.Fprintf(&b, `<tr><td style="border-bottom:1px solid #e5e5e5;">%s</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%d</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%d</td></tr>`,
				html.EscapeString(d.Date), d.Count, ac)
		}
		b.WriteString(`</table>`)
	}
	sectionEnd(&b)

	// 2 活跃榜
	sectionStart(&b, "2. 活跃成员排行榜")
	b.WriteString(`<p style="margin:0 0 8px;font-size:12px;color:#737373;">仅含区间有提交成员；教练与 0 提交已剔除。点姓名看资料。</p>`)
	ranking := data.ActiveRanking
	if len(ranking) == 0 {
		for _, r := range data.TopSubmit {
			ranking = append(ranking, MemberStat{Rank: r.Rank, UserID: r.UserID, Name: r.Name, Submits: r.Score})
		}
	}
	if len(ranking) == 0 {
		emptyRow(&b, "本区间无人提交")
	} else {
		b.WriteString(`<table width="100%" cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse;font-size:13px;">`)
		b.WriteString(`<tr style="background:#f5f5f5;"><th align="left" style="border-bottom:1px solid #e5e5e5;">#</th><th align="left" style="border-bottom:1px solid #e5e5e5;">成员</th><th align="right" style="border-bottom:1px solid #e5e5e5;">提交</th><th align="right" style="border-bottom:1px solid #e5e5e5;">AC</th><th align="right" style="border-bottom:1px solid #e5e5e5;">AC率</th></tr>`)
		shown := 0
		for _, m := range ranking {
			if shown >= rankCap {
				fmt.Fprintf(&b, `<tr><td colspan="5" style="padding:8px;color:#737373;font-size:12px;">…共 %d 人活跃 · <a href="%s" style="color:#171717;">主站查看更多</a></td></tr>`, len(ranking), moreAct)
				break
			}
			nameHTML := nameLink(m.Name, m.Username, m.UserID, m.ProfileURL)
			fmt.Fprintf(&b, `<tr><td style="border-bottom:1px solid #e5e5e5;">%d</td><td style="border-bottom:1px solid #e5e5e5;">%s</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%d</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%d</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%.1f%%</td></tr>`,
				m.Rank, nameHTML, m.Submits, m.AC, m.ACRate)
			shown++
		}
		if shown >= len(ranking) && len(ranking) > 0 {
			fmt.Fprintf(&b, `<tr><td colspan="5" style="padding:8px;font-size:12px;"><a href="%s" style="color:#171717;">主站动态 / 更多数据 →</a></td></tr>`, moreAct)
		}
		b.WriteString(`</table>`)
	}
	sectionEnd(&b)

	// 3 标签
	sectionStart(&b, "3. 知识点 / 题目标签")
	tags := data.TeamTags
	if len(tags) == 0 {
		tags = aggregateTeamTags(data.OrgSubmitSample, tagN)
	}
	if len(tags) == 0 {
		emptyRow(&b, "暂无标签（题目可能未关联题库）。")
	} else {
		b.WriteString(`<p style="margin:0;line-height:1.9;">`)
		for i, t := range tags {
			if i >= tagN {
				break
			}
			fmt.Fprintf(&b, `<span style="display:inline-block;background:#f5f5f5;color:#0a0a0a;border-radius:9999px;padding:3px 10px;margin:2px 4px 2px 0;font-size:12px;">%s <b>%d</b></span>`,
				html.EscapeString(t.Tag), t.Count)
		}
		b.WriteString(`</p>`)
		fmt.Fprintf(&b, `<p style="margin:8px 0 0;font-size:12px;"><a href="%s" style="color:#171717;">题库 / 主站 →</a></p>`, SiteBaseURL+"/question-bank")
	}
	sectionEnd(&b)

	// 4 做题 + 动态
	sectionStart(&b, "4. 做题概览与提交动态")
	probs := data.ProblemOverview
	if len(probs) == 0 {
		probs = aggregateProblemOverview(data.OrgSubmitSample, probN)
	}
	if len(probs) > 0 {
		b.WriteString(`<p style="margin:0 0 6px;font-size:13px;font-weight:600;color:#0a0a0a;">热门题目（谁在交）</p>`)
		b.WriteString(`<table width="100%" cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse;font-size:12px;">`)
		b.WriteString(`<tr style="background:#f5f5f5;"><th align="left" style="border-bottom:1px solid #e5e5e5;">题目</th><th align="right" style="border-bottom:1px solid #e5e5e5;">尝试</th><th align="right" style="border-bottom:1px solid #e5e5e5;">AC</th><th align="left" style="border-bottom:1px solid #e5e5e5;">AC成员</th></tr>`)
		for i, p := range probs {
			if i >= probN {
				break
			}
			acUsers := strings.Join(p.ACUsers, "、")
			if acUsers == "" {
				acUsers = "—"
			}
			fmt.Fprintf(&b, `<tr><td style="border-bottom:1px solid #e5e5e5;word-break:break-all;">%s</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%d</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%d</td><td style="border-bottom:1px solid #e5e5e5;font-size:11px;color:#737373;">%s</td></tr>`,
				html.EscapeString(p.Problem), p.Submitters, p.ACCount, html.EscapeString(acUsers))
		}
		b.WriteString(`</table>`)
	} else {
		emptyRow(&b, "暂无做题抽样。")
	}

	b.WriteString(`<p style="margin:12px 0 6px;font-size:13px;font-weight:600;color:#0a0a0a;">近期提交动态</p>`)
	if len(data.OrgSubmitSample) == 0 {
		emptyRow(&b, "暂无动态抽样。")
	} else {
		b.WriteString(`<table width="100%" cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse;font-size:12px;">`)
		b.WriteString(`<tr style="background:#f5f5f5;"><th align="left" style="border-bottom:1px solid #e5e5e5;">时间</th><th align="left" style="border-bottom:1px solid #e5e5e5;">成员</th><th align="left" style="border-bottom:1px solid #e5e5e5;">题目</th><th align="left" style="border-bottom:1px solid #e5e5e5;">状态</th></tr>`)
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
			stColor := "#737373"
			us := strings.ToUpper(f.Status)
			if strings.Contains(us, "AC") || us == "OK" {
				stColor = "#15803d"
			} else if strings.Contains(us, "WA") {
				stColor = "#dc2626"
			}
			fmt.Fprintf(&b, `<tr><td style="border-bottom:1px solid #e5e5e5;white-space:nowrap;">%s</td><td style="border-bottom:1px solid #e5e5e5;">%s</td><td style="border-bottom:1px solid #e5e5e5;word-break:break-all;">%s</td><td style="border-bottom:1px solid #e5e5e5;color:%s;font-weight:600;">%s</td></tr>`,
				html.EscapeString(f.Time), html.EscapeString(name), html.EscapeString(prob), stColor, html.EscapeString(f.Status))
			n++
		}
		b.WriteString(`</table>`)
		fmt.Fprintf(&b, `<p style="margin:8px 0 0;font-size:12px;"><a href="%s" style="color:#171717;">查看全部动态 →</a></p>`, moreAct)
	}
	sectionEnd(&b)

	// 5 比赛
	sectionStart(&b, "5. 比赛表现")
	if len(data.Contests) == 0 {
		emptyRow(&b, "区间内未匹配到比赛记录。")
		fmt.Fprintf(&b, `<p style="margin:8px 0 0;font-size:12px;"><a href="%s" style="color:#171717;">打开主站比赛页核对 →</a></p>`, moreContest)
	} else {
		b.WriteString(`<table width="100%" cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse;font-size:12px;">`)
		b.WriteString(`<tr style="background:#f5f5f5;"><th align="left" style="border-bottom:1px solid #e5e5e5;">比赛</th><th align="left" style="border-bottom:1px solid #e5e5e5;">平台</th><th align="right" style="border-bottom:1px solid #e5e5e5;">过题</th><th align="left" style="border-bottom:1px solid #e5e5e5;">日期</th></tr>`)
		n := 0
		for _, c := range data.Contests {
			if n >= contestN {
				break
			}
			name := html.EscapeString(c.ContestName)
			if c.ID > 0 {
				name = fmt.Sprintf(`<a href="%s/contest/%d" style="color:#171717;text-decoration:none;">%s</a>`, SiteBaseURL, c.ID, name)
			}
			fmt.Fprintf(&b, `<tr><td style="border-bottom:1px solid #e5e5e5;">%s</td><td style="border-bottom:1px solid #e5e5e5;">%s</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%d/%d</td><td style="border-bottom:1px solid #e5e5e5;">%s</td></tr>`,
				name, html.EscapeString(c.Platform), c.ACCount, c.TotalCount, html.EscapeString(c.Time))
			n++
		}
		b.WriteString(`</table>`)
		for i, snap := range data.ContestRankings {
			if compact && i >= 2 {
				break
			}
			if i >= 4 {
				break
			}
			fmt.Fprintf(&b, `<p style="margin:12px 0 6px;font-size:13px;font-weight:600;">%s · 组织榜（%d人）</p>`,
				html.EscapeString(snap.ContestName), snap.Total)
			b.WriteString(`<table width="100%" cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse;font-size:12px;">`)
			b.WriteString(`<tr style="background:#f5f5f5;"><th align="left" style="border-bottom:1px solid #e5e5e5;">#</th><th align="left" style="border-bottom:1px solid #e5e5e5;">成员</th><th align="right" style="border-bottom:1px solid #e5e5e5;">过题</th><th align="right" style="border-bottom:1px solid #e5e5e5;">分</th></tr>`)
			rowN := 10
			if compact {
				rowN = 5
			}
			for j, r := range snap.Top {
				if j >= rowN {
					break
				}
				fmt.Fprintf(&b, `<tr><td style="border-bottom:1px solid #e5e5e5;">%d</td><td style="border-bottom:1px solid #e5e5e5;">%s</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%d/%d</td><td align="right" style="border-bottom:1px solid #e5e5e5;">%d</td></tr>`,
					r.Rank, html.EscapeString(r.Name), r.ACCount, r.TotalCount, r.Score)
			}
			b.WriteString(`</table>`)
		}
		fmt.Fprintf(&b, `<p style="margin:8px 0 0;font-size:12px;"><a href="%s" style="color:#171717;">主站比赛列表 →</a></p>`, moreContest)
	}
	sectionEnd(&b)

	// 6 博客
	sectionStart(&b, "6. 知识沉淀（博客）")
	if len(data.RecentBlogs) == 0 {
		emptyRow(&b, "暂无组织公开博客。")
		fmt.Fprintf(&b, `<p style="margin:8px 0 0;font-size:12px;"><a href="%s" style="color:#171717;">博客广场 →</a></p>`, moreBlog)
	} else {
		n := 0
		for _, bl := range data.RecentBlogs {
			if n >= blogN {
				break
			}
			fmt.Fprintf(&b, `<div style="border:1px solid #e5e5e5;border-radius:10px;padding:10px;margin-bottom:8px;background:#fafbff;">`)
			fmt.Fprintf(&b, `<div style="font-weight:600;font-size:14px;">%s</div>`, html.EscapeString(bl.Title))
			fmt.Fprintf(&b, `<div style="font-size:12px;color:#737373;margin:4px 0;">%s</div>`, html.EscapeString(bl.Author))
			if bl.Summary != "" {
				fmt.Fprintf(&b, `<div style="font-size:12px;color:#737373;">%s</div>`, html.EscapeString(bl.Summary))
			}
			b.WriteString(`</div>`)
			n++
		}
		fmt.Fprintf(&b, `<p style="margin:4px 0 0;font-size:12px;"><a href="%s" style="color:#171717;">更多博客 →</a></p>`, moreBlog)
	}
	sectionEnd(&b)

	// 7 不活跃
	sectionStart(&b, "7. 不活跃成员（已从排行榜剔除）")
	if len(data.InactiveMembers) == 0 {
		b.WriteString(`<p style="margin:0;color:#15803d;font-weight:600;">全员都有提交，给力！</p>`)
	} else {
		b.WriteString(`<p style="margin:0;line-height:1.9;">`)
		n := 0
		for _, name := range data.InactiveMembers {
			if n >= inactiveN {
				fmt.Fprintf(&b, `<span style="display:inline-block;background:#fef2f2;color:#dc2626;border-radius:9999px;padding:3px 10px;margin:2px 4px 2px 0;font-size:12px;">…共%d人</span>`, len(data.InactiveMembers))
				break
			}
			fmt.Fprintf(&b, `<span style="display:inline-block;background:#fef2f2;color:#dc2626;border-radius:9999px;padding:3px 10px;margin:2px 4px 2px 0;font-size:12px;">%s</span>`, html.EscapeString(name))
			n++
		}
		b.WriteString(`</p>`)
	}
	sectionEnd(&b)

	// 8 评价
	sectionStart(&b, "8. 综合维度评价")
	for _, line := range dimLines {
		fmt.Fprintf(&b, `<div style="font-size:13px;color:#0a0a0a;margin:2px 0;">%s</div>`, html.EscapeString(line))
	}
	fmt.Fprintf(&b, `<div style="margin-top:10px;font-size:15px;font-weight:600;color:#0a0a0a;">总评 %s</div>`, statusEmoji)
	b.WriteString(`<ul style="margin:8px 0 0;padding-left:18px;color:#737373;font-size:13px;">`)
	for _, a := range advice {
		fmt.Fprintf(&b, `<li style="margin-bottom:4px;">%s</li>`, html.EscapeString(a))
	}
	b.WriteString(`</ul>`)
	sectionEnd(&b)

	// Footer
	fmt.Fprintf(&b, `<tr><td style="padding:12px 14px 18px;text-align:center;font-size:11px;color:#737373;border-top:1px solid #e5e5e5;">由 %s 规则模板生成 · 教练不计入统计 · <a href="%s" style="color:#171717;">主站</a></td></tr>`,
		html.EscapeString(brand), moreHome)

	b.WriteString(`</table></td></tr></table></body></html>`)
	return b.String()
}

func sectionStart(b *strings.Builder, title string) {
	b.WriteString(`<tr><td style="padding:4px 14px 12px;background:#ffffff;">`)
	b.WriteString(`<div style="background:#ffffff;border:1px solid #e5e5e5;border-radius:10px;padding:14px 12px;">`)
	fmt.Fprintf(b, `<div style="font-size:14px;font-weight:600;color:#0a0a0a;margin:0 0 10px;padding-bottom:8px;border-bottom:1px solid #e5e5e5;">%s</div>`, html.EscapeString(title))
}

func sectionEnd(b *strings.Builder) {
	b.WriteString(`</div></td></tr>`)
}

func emptyRow(b *strings.Builder, msg string) {
	fmt.Fprintf(b, `<p style="margin:0;font-size:13px;color:#737373;">%s</p>`, html.EscapeString(msg))
}

func nameLink(name, username string, userID int64, profileURL string) string {
	n := html.EscapeString(name)
	href := profileURL
	if href == "" {
		href = profileURLFn(username, userID)
	}
	if href == "" {
		return n
	}
	return fmt.Sprintf(`<a href="%s" style="color:#171717;text-decoration:underline;text-underline-offset:2px;font-weight:600;">%s</a>`, html.EscapeString(href), n)
}

func profileURLFn(username string, userID int64) string {
	return profileURL(username, userID)
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
	}

	lines = append(lines, fmt.Sprintf("· 活跃度：有提交 %d/%d（%.0f%%），提交环比 %+d",
		data.ActiveMembers, data.MemberCount, activeRatio*100, delta))
	lines = append(lines, fmt.Sprintf("· 正确率/AC：区间 AC %d，整体 AC 率 %.1f%%，榜首 %s",
		data.TotalAC, acRate, topName))
	tagN := len(data.TeamTags)
	if tagN == 0 {
		tagN = len(aggregateTeamTags(data.OrgSubmitSample, 99))
	}
	lines = append(lines, fmt.Sprintf("· 知识点：团队标签 %d 种", tagN))
	lines = append(lines, fmt.Sprintf("· 做题/动态：概览 %d 题，动态抽样 %d 条",
		len(data.ProblemOverview), len(data.OrgSubmitSample)))
	lines = append(lines, fmt.Sprintf("· 比赛：%d 场，重点榜 %d 场",
		len(data.Contests), len(data.ContestRankings)))
	lines = append(lines, fmt.Sprintf("· 博客：%d 篇", len(data.RecentBlogs)))
	lines = append(lines, fmt.Sprintf("· 排行：活跃 %d 人（已剔除教练与 0 提交），不活跃 %d 人",
		len(data.ActiveRanking), len(data.InactiveMembers)))

	if data.TotalSubmits == 0 {
		advice = append(advice, "本区间零提交，建议组织统一训练日并检查账号绑定。")
	} else if delta < 0 {
		advice = append(advice, "提交量环比下降，可重点关注中腰部成员。")
	} else {
		advice = append(advice, "提交量稳定或上升，继续保持节奏并巩固 AC 质量。")
	}
	if len(data.InactiveMembers) > 0 {
		advice = append(advice, fmt.Sprintf("有 %d 名成员本区间未提交，建议一对一跟进。", len(data.InactiveMembers)))
	}
	if len(data.Contests) == 0 {
		advice = append(advice, "本报告未匹配到区间比赛，可到主站比赛页核对同步情况。")
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

func trainingReportSystemPrompt(mode string) string {
	return trainingReportSystemPromptStrict(mode)
}

func trainingReportSystemPromptStrict(mode string) string {
	compact := mode == DetailModeCompact
	depth := "详版：activeRanking 全表列出有提交成员；不活跃单独一节；标签/做题/动态/博客/比赛都要有实质内容（无数据则写暂无）。"
	if compact {
		depth = "简版：活跃榜最多 15 行并写「共 N 人」；八个章节都要有，每节简短。"
	}
	return fmt.Sprintf(`你是 HTML 报告生成器，不是聊天助手。

【输出格式 — 违反即失败】
1. 你的回复必须且只能是一份完整 HTML 文档。
2. 第一个非空白字符必须是「<」（推荐以 <!DOCTYPE html> 或 <html 开头）。
3. 禁止输出：思考过程、分析说明、工具调用说明、「现在我已获取…」、Markdown、`+"```"+` 代码围栏、任何 HTML 之外的文字。
4. 禁止输出半截标签、未闭合的主要结构；必须以 </html> 结束（若以片段输出则至少含完整 table 结构与 8 个章节标题）。

【版式 — shadcn/ui 浅色主题 + QQ/邮件兼容】
- table 布局 + 元素上写 style="..." 内联样式
- 禁止 CSS Grid / Flex / clamp / 复杂 @media
- 外层 max-width:640px；字号用 px
- 配色（必须）：页面背景 #fafafa；卡片 #ffffff；主色/按钮 #171717；主色文字 #fafafa；正文 #0a0a0a；次要文字 #737373；边框 #e5e5e5；muted 面 #f5f5f5
- 卡片：1px 边框 + border-radius:10px；按钮：背景 #171717、白字、radius 8px
- 禁止 indigo/蓝紫系（如 #4f46e5、#6366f1）

【数据 — 只用用户消息里的 JSON】
- activeRanking：活跃榜（已剔除教练与 0 提交），禁止只写 Top5 假装全员
- inactiveMembers：不活跃，禁止进榜
- teamTags / problemOverview / orgSubmitSample / recentBlogs / contests / dailyTrend
- 数字与姓名必须与 JSON 一致，禁止编造
- 若某字段为空数组，对应章节写「暂无」并给主站链接，不要编造比赛/博客

【链接】
- 姓名：profileUrl 或 https://algo.zhiyuansofts.cn/profile/{username}
- 更多：动态 /all-activities · 比赛 /contest · 博客 /blog-plaza（域名 https://algo.zhiyuansofts.cn）

【必须 8 节（h2 或加粗标题文字需包含关键词）】
1 活跃度与趋势  2 活跃成员排行榜  3 知识点  4 做题概览与提交动态
5 比赛表现  6 知识沉淀  7 不活跃成员  8 综合维度评价
%s

可选：仅当 JSON 标签为空时，可调用 problem_tags；工具失败则忽略，继续用 JSON，不要在 HTML 里写工具错误。`, depth)
}

func trainingReportUserPrompt(data *TrainingReportData, mode string) string {
	label := "详版训练报告"
	if mode == DetailModeCompact {
		label = "简版教练周报"
	}
	// 压缩 JSON：去掉过长 memberIds 减少跑题
	payload := *data
	payload.MemberIDs = nil

	return fmt.Sprintf(`请直接输出 %s 的完整 HTML（从 <!DOCTYPE html> 开始，到 </html> 结束）。

【禁止】任何前言、后记、Markdown、代码围栏、工具状态说明。
【必须使用下列真实数据】activeRanking=%d inactive=%d tags=%d problems=%d feed=%d blogs=%d contests=%d
范围 %s %s~%s org=%d 提交%d(上期%d) AC%d

数据 JSON：
%s`,
		label,
		len(data.ActiveRanking), len(data.InactiveMembers), len(data.TeamTags),
		len(data.ProblemOverview), len(data.OrgSubmitSample), len(data.RecentBlogs), len(data.Contests),
		data.ScopeLabel, data.StartDate, data.EndDate, data.OrgID,
		data.TotalSubmits, data.PrevTotalSubmits, data.TotalAC,
		mustJSON(payload))
}

func mustJSON(v interface{}) string {
	b, err := jsonIndent(v)
	if err != nil {
		return "{}"
	}
	return b
}

// SanitizeAndValidateReportHTML 清洗 LLM 输出并校验是否为可用报告 HTML。
// 返回 ok=false 时 reason 说明失败原因（供重试/回退规则模板）。
func SanitizeAndValidateReportHTML(raw string) (htmlOut string, ok bool, reason string) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", false, "空输出"
	}
	// 先抽代码围栏，再去前言（顺序很重要：结尾的 ``` 不能当起点）
	s = extractHTMLDocument(s)
	s = stripCodeFence(s)
	s = stripLLMPreamble(s)
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false, "清洗后为空"
	}
	// 必须以标签开头
	if !strings.HasPrefix(s, "<") {
		return "", false, "未以 HTML 标签开头"
	}
	lower := strings.ToLower(s)
	// 禁止残留围栏/明显前言
	if strings.Contains(s, "```") {
		return "", false, "仍含 Markdown 代码围栏"
	}
	for _, bad := range []string{"现在我已获取", "开始生成", "工具返回", "服务不可用", "预置 json", "function call"} {
		if strings.Contains(lower, strings.ToLower(bad)) {
			// 允许出现在正文数据里的极少情况；若出现在文档前 200 字则判失败
			head := lower
			if len(head) > 400 {
				head = head[:400]
			}
			if strings.Contains(head, strings.ToLower(bad)) {
				return "", false, "含模型废话: " + bad
			}
		}
	}
	// 结构：至少有 table 或完整 html
	hasTable := strings.Contains(lower, "<table")
	hasHTML := strings.Contains(lower, "<html") || strings.HasPrefix(lower, "<!doctype")
	if !hasTable && !hasHTML {
		return "", false, "缺少 table/html 结构"
	}
	// 关键章节关键词（至少命中 5/8）
	keys := []string{"活跃", "排行", "标签", "做题", "动态", "比赛", "博客", "不活跃", "综合"}
	hit := 0
	for _, k := range keys {
		if strings.Contains(s, k) {
			hit++
		}
	}
	if hit < 5 {
		return "", false, fmt.Sprintf("章节不全(命中%d)", hit)
	}
	// 长度：过短视为残缺
	if len(s) < 800 {
		return "", false, fmt.Sprintf("HTML 过短(%d)", len(s))
	}
	// 未闭合的明显残缺：以 </ 中间切断
	if strings.HasSuffix(strings.TrimSpace(s), "</") || strings.Contains(s, "</\n") {
		return "", false, "疑似截断的闭合标签"
	}
	// 若无 html 外壳，包一层保证邮件可读
	if !hasHTML {
		s = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head><body style="margin:0;padding:0;background:#fafafa;font-family:ui-sans-serif,system-ui,sans-serif;font-size:14px;line-height:1.5;color:#0a0a0a;">` +
			s + `</body></html>`
	}
	return s, true, ""
}

// stripLLMPreamble 去掉「现在我…」「分析如下」等直到第一个 HTML 标签
func stripLLMPreamble(s string) string {
	s = strings.TrimSpace(s)
	// 找第一个 <!DOCTYPE / <html / <table / <div / <body
	lower := strings.ToLower(s)
	markers := []string{"<!doctype", "<html", "<table", "<div", "<body", "<meta"}
	pos := -1
	for _, m := range markers {
		if i := strings.Index(lower, m); i >= 0 {
			if pos < 0 || i < pos {
				pos = i
			}
		}
	}
	if pos > 0 {
		return strings.TrimSpace(s[pos:])
	}
	return s
}

// extractHTMLDocument 从 ```html ... ``` 围栏中抽出 HTML；无围栏则原样返回。
func extractHTMLDocument(s string) string {
	s = strings.TrimSpace(s)
	open := strings.Index(s, "```")
	if open < 0 {
		return s
	}
	rest := s[open+3:]
	// 可选语言行：html / htm / 空
	if nl := strings.IndexByte(rest, '\n'); nl >= 0 {
		lang := strings.ToLower(strings.TrimSpace(rest[:nl]))
		if lang == "html" || lang == "htm" || lang == "" {
			rest = rest[nl+1:]
		}
	} else {
		// 单行围栏无意义
		return s
	}
	if close := strings.Index(rest, "```"); close >= 0 {
		rest = rest[:close]
	}
	out := strings.TrimSpace(rest)
	if out == "" {
		// 若抽空（例如只匹配到结尾围栏），回退原文
		return s
	}
	return out
}
