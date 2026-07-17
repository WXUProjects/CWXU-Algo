package notify

import (
	"strings"
	"unicode"
)

// ExtractMentions 从文本中提取 @username（去重、保留首次出现大小写）
// 规则：@ 后 2–32 位字母数字下划线，且不被更长的同类字符续接。
func ExtractMentions(text string) []string {
	if text == "" {
		return nil
	}
	seen := map[string]struct{}{}
	var out []string
	runes := []rune(text)
	for i := 0; i < len(runes); i++ {
		if runes[i] != '@' {
			continue
		}
		// 避免邮箱中间的 @：前一字符是字母数字时跳过
		if i > 0 && isUserChar(runes[i-1]) {
			continue
		}
		j := i + 1
		for j < len(runes) && isUserChar(runes[j]) {
			j++
		}
		// 长度必须在 2–32；更长则整段丢弃（不当截断）
		n := j - (i + 1)
		if n < 2 || n > 32 {
			i = j - 1
			continue
		}
		u := string(runes[i+1 : j])
		key := strings.ToLower(u)
		if _, ok := seen[key]; !ok {
			seen[key] = struct{}{}
			out = append(out, u)
		}
		i = j - 1
	}
	return out
}

func isUserChar(r rune) bool {
	return unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_'
}
