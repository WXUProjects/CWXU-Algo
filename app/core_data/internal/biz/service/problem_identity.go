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
	reDigits              = regexp.MustCompile(`^\d+$`)
	reNowCoderProblemURL  = regexp.MustCompile(`(?i)(?:https?://[^/\s]+)?/acm/problem/(\d+)`)
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
	// 正确 external_id = 牛客题库数字 id（/acm/problem/{id}）
	// Problem 期望形如 "309177 【模板】高精度加法"；禁止用 questionNum(ACM3227) 或 main|uid 当 id
	title := strings.TrimSpace(problem)
	ext := ""

	// 1) 文本里的 /acm/problem/123 或 /problem/123
	if m := reNowCoderProblemURL.FindStringSubmatch(problem); m != nil {
		ext = m[1]
		// 标题尽量去掉 URL 残留
		title = strings.TrimSpace(reNowCoderProblemURL.ReplaceAllString(problem, ""))
	}

	// 2) 以纯数字开头："309177 标题"
	if ext == "" {
		fields := strings.Fields(problem)
		if len(fields) > 0 && reDigits.MatchString(fields[0]) {
			ext = fields[0]
			if len(fields) > 1 {
				title = strings.Join(fields[1:], " ")
			} else {
				title = ext
			}
		}
	}

	// 3) 整段纯数字
	if ext == "" && reDigits.MatchString(strings.TrimSpace(problem)) {
		ext = strings.TrimSpace(problem)
		title = ext
	}

	// 4) contest 若是纯数字比赛 id 且 problem 无数字 id：竞赛题无稳定练习题号 → 跳过爬取
	// 绝不使用 main|username / 标题 sanitize 当 external_id（会全站串题）
	if ext == "" {
		// 尝试从 contest 提取数字（真实 contest id，不是 main|uid）
		c := contest
		if i := strings.Index(c, "|"); i >= 0 {
			// main|uid 不是比赛 id，忽略
			c = ""
		}
		if reDigits.MatchString(c) {
			// 竞赛提交但无题目数字 id：无法稳定去重，跳过题库
			return nil, fmt.Errorf("nowcoder contest problem without numeric id")
		}
		return nil, fmt.Errorf("nowcoder missing numeric problem id: %q", problem)
	}

	if title == "" {
		title = ext
	}
	return &ParsedProblem{
		Platform:   spider.NowCoder,
		ExternalID: ext,
		Title:      title,
		URL:        "https://ac.nowcoder.com/acm/problem/" + ext,
	}, nil
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
