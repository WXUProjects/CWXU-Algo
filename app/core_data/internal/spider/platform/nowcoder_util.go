package platform

import "strings"

// isDigits 是否为非空纯数字串。
func isDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return true
}

// normalizeNowCoderUUID 主站 questionUuid：32 位 hex（可带连字符）→ 去连字符小写；非法返回 ""。
func normalizeNowCoderUUID(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return ""
	}
	for _, r := range s {
		if !((r >= '0' && r <= '9') || (r >= 'a' && r <= 'f')) {
			return ""
		}
	}
	return s
}
