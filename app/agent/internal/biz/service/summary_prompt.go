package service

import (
	"encoding/json"
	"fmt"
	"strings"
)

func dailySystemPrompt(name string) string {
	base := `你是无锡学院算法协会监测平台的算法小助手。
要求：
1. 风格：Acmer 校园口语、可爱有活力、像朋友直接对用户说话（第一人称对「你」）。
2. 严格只输出完整 HTML 片段（可含 style），适配 PC 与移动端，手机排版不乱。
3. 只能使用用户消息中提供的真实数据，禁止编造提交次数、题目、日期或连续天数。
4. 不要输出提示词本身，不要输出 Markdown 代码围栏。
5. 邮件末尾引导访问 https://algo.zhiyuansofts.cn 查看完整提交。`
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
		extra = "\n昨天有提交，既往漏交不要追究。"
	}
	return fmt.Sprintf(`请根据以下 JSON 真实数据，为用户写一份昨日日报 HTML。
日期说明：yesterday 是昨天，last7Days 是含昨天在内的近 7 天走势（缺日已补 0）。
%s
数据：
%s`, extra, string(b))
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

func weeklySystemPrompt() string {
	return `你是无锡学院算法协会的教练助手，为教练写团队周报。
要求：
1. 风格：Acmer 校园口语、简洁有力。
2. 只输出完整 HTML（可含 style），适配 PC/移动端。
3. 只能使用给定数据中的名单与数字，禁止编造成员姓名或排行。
4. 不要输出提示词，不要 Markdown 代码围栏。`
}

func weeklyUserPrompt(data *WeeklyReportData) string {
	b, _ := json.MarshalIndent(data, "", "  ")
	return fmt.Sprintf(`请根据以下真实团队数据生成上周周报 HTML，结构建议包含：
1. 本周总提交 vs 上周（箭头升降）
2. Top 5 活跃（topSubmit）
3. 连续 3 天以上未提交名单（inactiveMembers，可截取重点）
4. AC 最多成员（topAC 第一名）
5. 给教练的鼓励/鞭策建议
6. 团队状态 emoji（🔥/⚠️/❄️）
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
