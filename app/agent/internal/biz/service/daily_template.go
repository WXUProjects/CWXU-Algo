package service

import (
	"fmt"
	"html"
	"sort"
	"strings"
)

// RenderDailyRuleHTML 非 AI 日报：table + 内联样式，邮件客户端可渲染。
func RenderDailyRuleHTML(data *DailyReportData, brand string) string {
	if data == nil {
		return ""
	}
	if brand == "" {
		brand = "GoAlgo"
	}
	name := data.Name
	if name == "" {
		name = "同学"
	}
	home := SiteBaseURL + "/"
	profile := SiteBaseURL + "/profile"

	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html><head><meta charset="utf-8">`)
	b.WriteString(`<meta name="viewport" content="width=device-width,initial-scale=1">`)
	fmt.Fprintf(&b, `<title>%s 日报</title></head>`, html.EscapeString(brand))
	b.WriteString(`<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;font-size:14px;line-height:1.5;color:#222;">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f0f2f5;"><tr><td align="center" style="padding:12px 8px;">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;width:100%;background:#ffffff;border-radius:8px;overflow:hidden;">`)

	// Header
	b.WriteString(`<tr><td style="background:#4f46e5;color:#ffffff;padding:16px 14px;">`)
	fmt.Fprintf(&b, `<div style="font-size:12px;opacity:0.9;">%s · 个人日报</div>`, html.EscapeString(brand))
	fmt.Fprintf(&b, `<div style="font-size:20px;font-weight:bold;margin:6px 0 4px;">你好，%s</div>`, html.EscapeString(name))
	fmt.Fprintf(&b, `<div style="font-size:12px;opacity:0.92;">%s 训练回顾</div>`, html.EscapeString(formatCNDate(data.Yesterday)))
	fmt.Fprintf(&b, `<div style="font-size:11px;margin-top:8px;"><a href="%s" style="color:#e0e7ff;">打开主站</a></div>`, home)
	b.WriteString(`</td></tr>`)

	// KPI
	b.WriteString(`<tr><td style="padding:12px 10px 4px;">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="6" border="0"><tr>`)
	fmt.Fprintf(&b, `<td width="50%%" valign="top" style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:12px 10px;"><div style="font-size:22px;font-weight:bold;color:#1e1b4b;">%d</div><div style="font-size:12px;color:#64748b;">昨日提交</div></td>`, data.YesterdayCount)
	zeroNote := "保持节奏"
	if data.YesterdayCount == 0 {
		zeroNote = fmt.Sprintf("已连续 %d 天未交", data.ConsecutiveZeros)
	}
	fmt.Fprintf(&b, `<td width="50%%" valign="top" style="background:#f8fafc;border:1px solid #e5e7eb;border-radius:8px;padding:12px 10px;"><div style="font-size:16px;font-weight:bold;color:#1e1b4b;">%s</div><div style="font-size:12px;color:#64748b;">状态</div></td>`, html.EscapeString(zeroNote))
	b.WriteString(`</tr></table></td></tr>`)

	// 近 7 日
	b.WriteString(`<tr><td style="padding:8px 14px 4px;"><div style="font-size:15px;font-weight:bold;color:#1e1b4b;margin-bottom:8px;">近 7 日提交走势</div>`)
	if len(data.Last7Days) == 0 {
		b.WriteString(`<p style="margin:0;color:#64748b;font-size:13px;">暂无数据</p>`)
	} else {
		b.WriteString(`<table width="100%" cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse;font-size:13px;">`)
		b.WriteString(`<tr style="background:#f8fafc;"><th align="left" style="border-bottom:1px solid #e5e7eb;">日期</th><th align="right" style="border-bottom:1px solid #e5e7eb;">提交</th></tr>`)
		for _, d := range data.Last7Days {
			fmt.Fprintf(&b, `<tr><td style="border-bottom:1px solid #f1f5f9;">%s</td><td align="right" style="border-bottom:1px solid #f1f5f9;">%d</td></tr>`,
				html.EscapeString(d.Date), d.Count)
		}
		b.WriteString(`</table>`)
	}
	b.WriteString(`</td></tr>`)

	// 昨日明细
	b.WriteString(`<tr><td style="padding:12px 14px 4px;"><div style="font-size:15px;font-weight:bold;color:#1e1b4b;margin-bottom:8px;">昨日提交明细</div>`)
	if len(data.YesterdayLogs) == 0 {
		msg := "昨天没有提交记录。"
		if data.ConsecutiveZeros > 0 {
			msg = fmt.Sprintf("昨天 0 提交，已连续 %d 天未交。今天开一题就好。", data.ConsecutiveZeros)
		}
		fmt.Fprintf(&b, `<p style="margin:0;color:#64748b;font-size:13px;">%s</p>`, html.EscapeString(msg))
	} else {
		b.WriteString(`<table width="100%" cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse;font-size:12px;">`)
		b.WriteString(`<tr style="background:#f8fafc;"><th align="left" style="border-bottom:1px solid #e5e7eb;">题目</th><th align="left" style="border-bottom:1px solid #e5e7eb;">平台</th><th align="left" style="border-bottom:1px solid #e5e7eb;">结果</th></tr>`)
		capN := 20
		for i, log := range data.YesterdayLogs {
			if i >= capN {
				fmt.Fprintf(&b, `<tr><td colspan="3" style="padding:8px;color:#64748b;font-size:12px;">…共 %d 条 · <a href="%s" style="color:#4f46e5;">主站查看</a></td></tr>`, len(data.YesterdayLogs), profile)
				break
			}
			title := log.Title
			if title == "" {
				title = log.Problem
			}
			if title == "" {
				title = "—"
			}
			fmt.Fprintf(&b, `<tr><td style="border-bottom:1px solid #f1f5f9;">%s</td><td style="border-bottom:1px solid #f1f5f9;">%s</td><td style="border-bottom:1px solid #f1f5f9;">%s</td></tr>`,
				html.EscapeString(title), html.EscapeString(log.Platform), html.EscapeString(log.Status))
		}
		b.WriteString(`</table>`)
	}
	b.WriteString(`</td></tr>`)

	// 标签
	b.WriteString(`<tr><td style="padding:12px 14px 4px;"><div style="font-size:15px;font-weight:bold;color:#1e1b4b;margin-bottom:8px;">知识点 / 标签</div>`)
	if len(data.YesterdayTagHits) > 0 {
		type kv struct {
			k string
			v int
		}
		var list []kv
		for k, v := range data.YesterdayTagHits {
			list = append(list, kv{k, v})
		}
		sort.Slice(list, func(i, j int) bool {
			if list[i].v != list[j].v {
				return list[i].v > list[j].v
			}
			return list[i].k < list[j].k
		})
		b.WriteString(`<p style="margin:0 0 6px;font-size:12px;color:#64748b;">昨日涉及：</p><p style="margin:0;line-height:1.9;">`)
		for i, it := range list {
			if i >= 12 {
				break
			}
			fmt.Fprintf(&b, `<span style="display:inline-block;background:#eef2ff;color:#3730a3;border-radius:12px;padding:3px 10px;margin:2px 4px 2px 0;font-size:12px;">%s <b>%d</b></span>`,
				html.EscapeString(it.k), it.v)
		}
		b.WriteString(`</p>`)
	} else if len(data.TagRadar) > 0 {
		b.WriteString(`<p style="margin:0;line-height:1.9;">`)
		for i, t := range data.TagRadar {
			if i >= 10 {
				break
			}
			fmt.Fprintf(&b, `<span style="display:inline-block;background:#eef2ff;color:#3730a3;border-radius:12px;padding:3px 10px;margin:2px 4px 2px 0;font-size:12px;">%s</span>`,
				html.EscapeString(t.Tag))
		}
		b.WriteString(`</p>`)
	} else {
		b.WriteString(`<p style="margin:0;color:#64748b;font-size:13px;">暂无标签画像</p>`)
	}
	b.WriteString(`</td></tr>`)

	// 比赛
	b.WriteString(`<tr><td style="padding:12px 14px 4px;"><div style="font-size:15px;font-weight:bold;color:#1e1b4b;margin-bottom:8px;">近期比赛</div>`)
	if len(data.RecentContests) == 0 {
		b.WriteString(`<p style="margin:0;color:#64748b;font-size:13px;">暂无近期比赛记录</p>`)
	} else {
		b.WriteString(`<table width="100%" cellpadding="6" cellspacing="0" border="0" style="border-collapse:collapse;font-size:12px;">`)
		b.WriteString(`<tr style="background:#f8fafc;"><th align="left" style="border-bottom:1px solid #e5e7eb;">比赛</th><th align="right" style="border-bottom:1px solid #e5e7eb;">名次</th><th align="right" style="border-bottom:1px solid #e5e7eb;">过题</th></tr>`)
		for i, c := range data.RecentContests {
			if i >= 8 {
				break
			}
			rank := "—"
			if c.Rank > 0 {
				rank = fmt.Sprintf("%d", c.Rank)
			}
			fmt.Fprintf(&b, `<tr><td style="border-bottom:1px solid #f1f5f9;">%s</td><td align="right" style="border-bottom:1px solid #f1f5f9;">%s</td><td align="right" style="border-bottom:1px solid #f1f5f9;">%d</td></tr>`,
				html.EscapeString(c.ContestName), rank, c.ACCount)
		}
		b.WriteString(`</table>`)
	}
	b.WriteString(`</td></tr>`)

	// 建议
	b.WriteString(`<tr><td style="padding:12px 14px 16px;"><div style="font-size:15px;font-weight:bold;color:#1e1b4b;margin-bottom:8px;">小结</div>`)
	if data.YesterdayCount == 0 {
		b.WriteString(`<p style="margin:0;font-size:13px;color:#334155;">昨天没动笔也没关系，今天挑一题热热身，保持节奏最重要。</p>`)
	} else {
		b.WriteString(`<p style="margin:0;font-size:13px;color:#334155;">昨天有提交，继续保持；可结合标签弱项补一题巩固。</p>`)
	}
	fmt.Fprintf(&b, `<p style="margin:12px 0 0;font-size:12px;"><a href="%s" style="color:#4f46e5;">在主站查看完整提交 →</a></p>`, home)
	b.WriteString(`</td></tr>`)

	b.WriteString(`</table></td></tr></table></body></html>`)
	return b.String()
}

// SanitizeDailyHTML 清洗日报 LLM 输出；失败时 ok=false，调用方应回退规则模板。
func SanitizeDailyHTML(raw string) (htmlOut string, ok bool, reason string) {
	s := strings.TrimSpace(raw)
	if s == "" {
		return "", false, "空输出"
	}
	s = extractHTMLDocument(s)
	s = stripCodeFence(s)
	s = stripLLMPreamble(s)
	s = strings.TrimSpace(s)
	if s == "" {
		return "", false, "清洗后为空"
	}
	if !strings.HasPrefix(s, "<") {
		return "", false, "未以 HTML 标签开头"
	}
	if strings.Contains(s, "```") {
		return "", false, "仍含 Markdown 代码围栏"
	}
	lower := strings.ToLower(s)
	// 拒绝纯 Markdown 痕迹占主导
	if strings.Count(s, "**") >= 3 && !strings.Contains(lower, "<table") && !strings.Contains(lower, "<div") {
		return "", false, "疑似 Markdown"
	}
	hasStruct := strings.Contains(lower, "<table") ||
		strings.Contains(lower, "<div") ||
		strings.Contains(lower, "<p") ||
		strings.Contains(lower, "<html")
	if !hasStruct {
		return "", false, "缺少 HTML 结构"
	}
	if len(s) < 80 {
		return "", false, fmt.Sprintf("过短(%d)", len(s))
	}
	// 无完整文档外壳则包一层
	if !strings.Contains(lower, "<html") && !strings.HasPrefix(lower, "<!doctype") {
		s = `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>` +
			`<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;font-size:14px;line-height:1.5;color:#222;">` +
			s + `</body></html>`
	}
	return s, true, ""
}
