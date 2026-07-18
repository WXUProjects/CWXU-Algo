// Package blogtext holds pure blog/solution text helpers (summary, surface rules)
// shared by user service and blogsync without import cycles.
package blogtext

import "strings"

// VisibilityPublic is the only visibility that auto-surfaces.
const VisibilityPublic = "public"

// AutoSurface reports whether a public non-password article should auto-appear
// on discovery recommend, plaza, and author-org surfaces.
func AutoSurface(visibility string) bool {
	v := strings.ToLower(strings.TrimSpace(visibility))
	if v == "" {
		v = VisibilityPublic
	}
	return v == VisibilityPublic
}

// Default summary generation (shared by blog + solution mirrors).
const (
	// DefaultSummaryMaxRunes is the max length of a system-generated 简述.
	DefaultSummaryMaxRunes = 280
	defaultSummaryEllipsis = "…"
)

// DefaultSummary builds a content-derived brief for list cards.
func DefaultSummary(content string) string {
	s := strings.ReplaceAll(content, "\r\n", "\n")
	s = stripFencedCodeBlocks(s)
	s = stripMarkdownNoise(s)
	fields := strings.Fields(s)
	s = strings.Join(fields, " ")
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	runes := []rune(s)
	if len(runes) <= DefaultSummaryMaxRunes {
		return s
	}
	cut := DefaultSummaryMaxRunes
	window := runes[:cut]
	for i := len(window) - 1; i >= cut-40 && i >= 0; i-- {
		switch window[i] {
		case '。', '！', '？', '；', '.', '!', '?', ';', '，', ',':
			return string(window[:i+1]) + defaultSummaryEllipsis
		}
	}
	return string(window) + defaultSummaryEllipsis
}

// IsDefaultSummary reports whether summary matches the system default for content.
func IsDefaultSummary(summary, content string) bool {
	sum := strings.TrimSpace(summary)
	if sum == "" {
		return true
	}
	return sum == DefaultSummary(content)
}

// ResolveSummaryForSave: empty → regenerate default; non-empty custom kept.
func ResolveSummaryForSave(userSummary, content string) string {
	if strings.TrimSpace(userSummary) == "" {
		return DefaultSummary(content)
	}
	return strings.TrimSpace(userSummary)
}

func stripFencedCodeBlocks(s string) string {
	var b strings.Builder
	lines := strings.Split(s, "\n")
	inFence := false
	for _, line := range lines {
		trim := strings.TrimSpace(line)
		if strings.HasPrefix(trim, "```") {
			inFence = !inFence
			continue
		}
		if inFence {
			continue
		}
		b.WriteString(line)
		b.WriteByte('\n')
	}
	return b.String()
}

func stripMarkdownNoise(s string) string {
	out := s
	for {
		i := strings.Index(out, "![")
		if i < 0 {
			break
		}
		j := strings.Index(out[i:], "](")
		if j < 0 {
			break
		}
		j += i
		k := strings.Index(out[j:], ")")
		if k < 0 {
			break
		}
		k += j
		alt := out[i+2 : j]
		out = out[:i] + alt + out[k+1:]
	}
	for {
		i := strings.Index(out, "[")
		if i < 0 {
			break
		}
		j := strings.Index(out[i:], "](")
		if j < 0 {
			break
		}
		j += i
		k := strings.Index(out[j:], ")")
		if k < 0 {
			break
		}
		k += j
		label := out[i+1 : j]
		out = out[:i] + label + out[k+1:]
	}
	lines := strings.Split(out, "\n")
	for i, line := range lines {
		t := strings.TrimSpace(line)
		t = strings.TrimLeft(t, "#")
		t = strings.TrimSpace(t)
		if strings.HasPrefix(t, "- ") || strings.HasPrefix(t, "* ") {
			t = strings.TrimSpace(t[2:])
		}
		lines[i] = t
	}
	return strings.Join(lines, "\n")
}
