package platform

import "strings"

// NormalizeCodeforcesVerdict 将 CF API verdict 归一化为站内状态字。
// CF 文档：评测中可能省略 verdict 字段 → 空串；终态为 OK / WRONG_ANSWER 等。
func NormalizeCodeforcesVerdict(verdict string) string {
	v := strings.TrimSpace(verdict)
	if v == "" {
		return "TESTING"
	}
	switch strings.ToUpper(v) {
	case "OK":
		return "OK" // IsAcceptedStatus 认 OK
	case "WRONG_ANSWER":
		return "WA"
	case "TIME_LIMIT_EXCEEDED":
		return "TLE"
	case "MEMORY_LIMIT_EXCEEDED":
		return "MLE"
	case "RUNTIME_ERROR":
		return "RE"
	case "COMPILATION_ERROR":
		return "CE"
	case "PRESENTATION_ERROR":
		return "PE"
	case "IDLENESS_LIMIT_EXCEEDED":
		return "ILE"
	case "SECURITY_VIOLATED":
		return "SV"
	case "CRASHED", "INPUT_PREPARATION_CRASHED":
		return "CRASHED"
	case "FAILED":
		return "FAILED"
	case "REJECTED":
		return "REJECTED"
	case "PARTIAL":
		return "PARTIAL"
	case "SKIPPED":
		return "SKIPPED"
	case "CHALLENGED":
		return "CHALLENGED"
	case "TESTING", "IN_QUEUE", "PENDING", "JUDGING":
		return "TESTING"
	default:
		// 未知字面量原样保留（大写），避免丢信息
		return strings.ToUpper(v)
	}
}

