package platform

import (
	"strconv"
	"strings"
)

// nowcoderProblemLabel 生成 parseNowCoder 可识别的 "id 标题"。
//
// 身份优先级（与 AC 站 practice-coding 对齐，避免同一题双计）：
//  1. 数字题库 id（training API 的 problem.id == ac.nowcoder.com/acm/problem/{id}）
//  2. 主站 questionUuid（仅当没有数字 id 时）
//  3. 纯数字 questionNum（ACM413 等展示号不可用，调用方应已过滤）
//
// 历史 bug：优先写 UUID，导致与 HTML 源的数字 id 分裂成两个 external_id / problem_id，
// 生涯过题数接近「数字题 + UUID 题」之和（例：官方 612 vs 站内 1110）。
func nowcoderProblemLabel(numericID int64, questionUUID, questionNum, title string) string {
	title = strings.TrimSpace(title)
	if numericID > 0 {
		id := strconv.FormatInt(numericID, 10)
		if title != "" {
			return id + " " + title
		}
		return id
	}
	if uuid := normalizeNowCoderUUID(questionUUID); uuid != "" {
		if title != "" {
			return uuid + " " + title
		}
		return uuid
	}
	qn := strings.TrimSpace(questionNum)
	if qn != "" && isDigits(qn) {
		return strings.TrimSpace(qn + " " + title)
	}
	return title
}
