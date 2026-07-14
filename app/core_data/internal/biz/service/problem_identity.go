package service

import (
	"cwxu-algo/app/core_data/internal/spider"
	"fmt"
	"regexp"
	"strings"
)

// ParsedProblem 从提交记录解析出的题目身份
type ParsedProblem struct {
	Platform   string
	ExternalID string
	Title      string
	URL        string
	SkipFetch  bool // 不可爬取（若仍入库则 SKIPPED）
	SkipBank   bool // 不进入题库（如 LeetCode）
}

var (
	reCFProblem           = regexp.MustCompile(`^([A-Za-z0-9]+)\s*-\s*(.+)$`)
	reLuoGuPID            = regexp.MustCompile(`^([A-Z]\d+)\s+(.+)$`)
	reAtCoderTask         = regexp.MustCompile(`^[a-z0-9_]+$`)
	reQOJNum              = regexp.MustCompile(`#?\s*(\d+)`)
	reDigits                 = regexp.MustCompile(`^\d+$`)
	reNowCoderProblemURL     = regexp.MustCompile(`(?i)(?:https?://[^/\s]+)?/acm/problem/(\d+)`)
	reNowCoderPracticeURL    = regexp.MustCompile(`(?i)(?:https?://[^/\s]+)?/practice/([0-9a-fA-F-]{32,36})`)
	reNowCoderUUID           = regexp.MustCompile(`(?i)^[0-9a-f]{8}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{4}-?[0-9a-f]{12}$|^[0-9a-f]{32}$`)
)

// ParseProblemIdentity 从 SubmitLog 字段解析 (platform, external_id)
func ParseProblemIdentity(platform, contest, problem string) (*ParsedProblem, error) {
	platform = strings.TrimSpace(platform)
	contest = strings.TrimSpace(contest)
	problem = strings.TrimSpace(problem)
	if platform == "" || problem == "" {
		return nil, fmt.Errorf("empty platform or problem")
	}

	switch platform {
	case spider.CodeForces:
		return parseCodeforces(contest, problem)
	case spider.AtCoder:
		return parseAtCoder(contest, problem)
	case spider.LuoGu:
		return parseLuoGu(problem)
	case spider.NowCoder:
		return parseNowCoder(contest, problem)
	case spider.QOJ:
		return parseQOJ(problem)
	case spider.LeetCode:
		// 力扣不可爬，不入库题库
		return &ParsedProblem{
			Platform:  platform,
			Title:     problem,
			SkipFetch: true,
			SkipBank:  true,
		}, nil
	default:
		// 兜底：用 problem 文本 hash 级 id
		ext := sanitizeExternalID(problem)
		if ext == "" {
			return nil, fmt.Errorf("unsupported platform %s", platform)
		}
		return &ParsedProblem{
			Platform:   platform,
			ExternalID: ext,
			Title:      problem,
		}, nil
	}
}

func parseCodeforces(contest, problem string) (*ParsedProblem, error) {
	// Problem 形如 "C-Name" 或 "C1-Name"
	m := reCFProblem.FindStringSubmatch(problem)
	if m == nil {
		// 尝试仅 index
		parts := strings.SplitN(problem, " ", 2)
		index := strings.TrimSpace(parts[0])
		title := problem
		if len(parts) > 1 {
			title = strings.TrimSpace(parts[1])
		}
		if index == "" || contest == "" {
			return nil, fmt.Errorf("cf parse fail: %s %s", contest, problem)
		}
		return &ParsedProblem{
			Platform:   spider.CodeForces,
			ExternalID: contest + index,
			Title:      title,
			URL:        fmt.Sprintf("https://codeforces.com/contest/%s/problem/%s", contest, index),
		}, nil
	}
	index, name := m[1], strings.TrimSpace(m[2])
	if contest == "" {
		return nil, fmt.Errorf("cf missing contest")
	}
	return &ParsedProblem{
		Platform:   spider.CodeForces,
		ExternalID: contest + index,
		Title:      name,
		URL:        fmt.Sprintf("https://codeforces.com/contest/%s/problem/%s", contest, index),
	}, nil
}

func parseAtCoder(contest, problem string) (*ParsedProblem, error) {
	// problem 多为 problem_id 如 abc123_a
	pid := strings.TrimSpace(problem)
	if !reAtCoderTask.MatchString(pid) {
		pid = sanitizeExternalID(pid)
	}
	if pid == "" {
		return nil, fmt.Errorf("atcoder empty problem")
	}
	url := ""
	if contest != "" {
		url = fmt.Sprintf("https://atcoder.jp/contests/%s/tasks/%s", contest, pid)
	}
	return &ParsedProblem{
		Platform:   spider.AtCoder,
		ExternalID: pid,
		Title:      pid,
		URL:        url,
	}, nil
}

func parseLuoGu(problem string) (*ParsedProblem, error) {
	// "P1001 标题" 或 "P1001"
	m := reLuoGuPID.FindStringSubmatch(problem)
	var pid, title string
	if m != nil {
		pid, title = m[1], strings.TrimSpace(m[2])
	} else {
		parts := strings.Fields(problem)
		if len(parts) == 0 {
			return nil, fmt.Errorf("luogu empty")
		}
		pid = parts[0]
		if len(parts) > 1 {
			title = strings.Join(parts[1:], " ")
		} else {
			title = pid
		}
	}
	return &ParsedProblem{
		Platform:   spider.LuoGu,
		ExternalID: pid,
		Title:      title,
		URL:        "https://www.luogu.com.cn/problem/" + pid,
	}, nil
}

func parseNowCoder(contest, problem string) (*ParsedProblem, error) {
	// AC 站 external_id = 数字 id → https://ac.nowcoder.com/acm/problem/{id}
	// 主站 external_id = questionUuid（32 hex）→ https://www.nowcoder.com/practice/{uuid}
	// 禁止用 questionNum(ACM413) 或 main|uid 当 id
	title := strings.TrimSpace(problem)
	ext := ""
	isMainSite := false

	// 1) 主站 practice URL
	if m := reNowCoderPracticeURL.FindStringSubmatch(problem); m != nil {
		if u := normalizeNowCoderUUID(m[1]); u != "" {
			ext = u
			isMainSite = true
			title = strings.TrimSpace(reNowCoderPracticeURL.ReplaceAllString(problem, ""))
		}
	}

	// 2) AC 站 /acm/problem/123
	if ext == "" {
		if m := reNowCoderProblemURL.FindStringSubmatch(problem); m != nil {
			ext = m[1]
			title = strings.TrimSpace(reNowCoderProblemURL.ReplaceAllString(problem, ""))
		}
	}

	// 3) 字段首 token：UUID 或纯数字
	if ext == "" {
		fields := strings.Fields(problem)
		if len(fields) > 0 {
			if u := normalizeNowCoderUUID(fields[0]); u != "" {
				ext = u
				isMainSite = true
				if len(fields) > 1 {
					title = strings.Join(fields[1:], " ")
				} else {
					title = ext
				}
			} else if reDigits.MatchString(fields[0]) {
				ext = fields[0]
				if len(fields) > 1 {
					title = strings.Join(fields[1:], " ")
				} else {
					title = ext
				}
			}
		}
	}

	// 4) 整段 UUID / 纯数字
	if ext == "" {
		raw := strings.TrimSpace(problem)
		if u := normalizeNowCoderUUID(raw); u != "" {
			ext = u
			isMainSite = true
			title = ext
		} else if reDigits.MatchString(raw) {
			ext = raw
			title = ext
		}
	}

	// 5) contest 若是纯数字比赛 id 且 problem 无稳定 id：竞赛题无法去重 → 跳过
	if ext == "" {
		c := contest
		if i := strings.Index(c, "|"); i >= 0 {
			c = ""
		}
		if reDigits.MatchString(c) {
			return nil, fmt.Errorf("nowcoder contest problem without stable id")
		}
		return nil, fmt.Errorf("nowcoder missing problem id: %q", problem)
	}

	if title == "" {
		title = ext
	}
	problemURL := "https://ac.nowcoder.com/acm/problem/" + ext
	if isMainSite {
		problemURL = "https://www.nowcoder.com/practice/" + ext
	}
	return &ParsedProblem{
		Platform:   spider.NowCoder,
		ExternalID: ext,
		Title:      title,
		URL:        problemURL,
	}, nil
}

// normalizeNowCoderUUID 主站 questionUuid：32 位 hex（可带连字符），统一去掉连字符入库
func normalizeNowCoderUUID(s string) string {
	s = strings.TrimSpace(strings.ToLower(s))
	if !reNowCoderUUID.MatchString(s) {
		return ""
	}
	s = strings.ReplaceAll(s, "-", "")
	if len(s) != 32 {
		return ""
	}
	return s
}

func parseQOJ(problem string) (*ParsedProblem, error) {
	title := problem
	ext := sanitizeExternalID(problem)
	if m := reQOJNum.FindStringSubmatch(problem); m != nil {
		ext = m[1]
	}
	if ext == "" {
		return nil, fmt.Errorf("qoj parse fail")
	}
	return &ParsedProblem{
		Platform:   spider.QOJ,
		ExternalID: ext,
		Title:      title,
		URL:        "https://qoj.ac/problem/" + ext,
	}, nil
}

func sanitizeExternalID(s string) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, " ", "_")
	s = strings.ReplaceAll(s, "/", "_")
	s = strings.ReplaceAll(s, "\\", "_")
	if len(s) > 120 {
		s = s[:120]
	}
	return s
}
