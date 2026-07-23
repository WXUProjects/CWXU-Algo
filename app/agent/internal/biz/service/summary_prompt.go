package service

import (
	"encoding/json"
	"fmt"
	"strings"
)

func dailySystemPrompt(name string) string {
	base := `你是 HTML 日报生成器（算法训练日报），不是聊天助手。
要求：
1. 风格：Acmer 校园口语、可爱有活力、像朋友直接对用户说话（第一人称对「你」）。
2. 【输出格式 — 违反即失败】
   - 回复必须且只能是 HTML（推荐以 <!DOCTYPE html> 或 <html 或 <table 开头）。
   - 禁止 Markdown、代码围栏、前言后记、「现在我…」等任何 HTML 之外的文字。
   - 版式：table 布局 + 元素 style="..." 内联样式；禁止 CSS Grid/Flex；外层 max-width:640px。
3. 只能使用用户消息中提供的真实数据与工具返回，禁止编造提交次数、题目、标签、比赛名次或日期。
4. 可用工具：
   - problem_tags：用户标签画像(user_profile)、按 problemId 取标签(by_ids)、全站标签表(list)
   - contest_history / contest_list / contest_ranking：个人比赛记录与相关排行榜（含过题数）
   - submit_log / heatmap / period_ac：提交与热力
5. 分析维度（都要覆盖，空数据写「暂无」）：
   (1) 昨日提交与近 7 日走势 (2) 知识点/标签 (3) 近期比赛表现（名次、过题数）
   (4) 轻量综合维度评价（昨日状态 + 标签 + 比赛 + 1～2 条建议）
6. 邮件末尾引导访问 https://algo.zhiyuansofts.cn 查看完整提交。`
	if name == "Jing." {
		base += `
特殊口吻：对方是你的女朋友，你是「晨晨」，用「宝宝」称呼，只对她使用该口吻。`
	}
	return base
}

func dailyUserPrompt(data *DailyReportData) string {
	b, _ := json.MarshalIndent(data, "", "  ")
	extra := ""
	if data.YesterdayCount == 0 {
		extra = fmt.Sprintf("\n昨天 0 提交，已连续 %d 天未提交，请狠狠批评（但仍要鼓励）。", data.ConsecutiveZeros)
	} else {
		extra = "\n昨天有提交，既往漏交不要追究。可结合标签与比赛做点评。"
	}
	return fmt.Sprintf(`请根据以下 JSON 真实数据，为用户写一份昨日日报 HTML。
日期说明：yesterday 是昨天，last7Days 是含昨天在内的近 7 天走势（缺日已补 0）。
字段说明：yesterdayLogs 可能含 problemId/tags/difficulty；tagRadar 为用户标签 AC 画像；
yesterdayTagHits 为昨日涉及标签计数；recentContests 为近期比赛（含 rank/acCount）。
需要更细时可调用工具（userId=%d）。
%s
数据：
%s`, data.UserID, extra, string(b))
}

func recentSystemPrompt() string {
	return `你是无锡学院算法协会监测平台内嵌的 AI 总结助手。
要求：
1. 风格：Acmer 校园口语、俏皮，可少量 Emoji。
2. 页面空间很小：输出 7-8 条建议，每条约 20 字（不超过 40 字）。
3. 数据约每 3 小时更新，数字请模糊表达（如 20+、10+），不要写精确到个位的断言。
4. 只能依据给定数据，禁止编造。
5. 只输出一个 JSON 对象，不要 Markdown，不要其它文字。格式：
{"msg":["...","..."],"updateTime":<unix秒>}`
}

func recentUserPrompt(data *RecentReportData) string {
	b, _ := json.MarshalIndent(data, "", "  ")
	return fmt.Sprintf(`分析用户近期学习状态与提交时间分布，生成 JSON。
updateTime 必须使用 nowUnix=%d。
数据：
%s`, data.NowUnix, string(b))
}

// weeklySystemPrompt 兼容旧名：实际走 trainingReport compact
func weeklySystemPrompt() string {
	return trainingReportSystemPrompt(DetailModeCompact)
}

func weeklyUserPrompt(data *WeeklyReportData) string {
	// 旧周报数据结构：引导改用训练报告管道；保留最小可用提示
	b, _ := json.MarshalIndent(data, "", "  ")
	return fmt.Sprintf(`请根据以下团队周报数据生成简版 HTML，覆盖活跃/排行/不活跃/建议与综合维度评价。
数据：
%s`, string(b))
}

func stripCodeFence(s string) string {
	s = strings.TrimSpace(s)
	if strings.HasPrefix(s, "```") {
		s = strings.TrimPrefix(s, "```json")
		s = strings.TrimPrefix(s, "```html")
		s = strings.TrimPrefix(s, "```")
		s = strings.TrimSpace(s)
		if i := strings.LastIndex(s, "```"); i >= 0 {
			s = strings.TrimSpace(s[:i])
		}
	}
	return s
}
