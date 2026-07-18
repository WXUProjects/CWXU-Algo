package service

import (
	"fmt"
	"net/url"
	"regexp"
	"strings"

	"cwxu-algo/app/core_data/internal/spider"
)

var (
	reURLCFContest    = regexp.MustCompile(`(?i)/contest/(\d+)/problem/([A-Za-z0-9]+)`)
	reURLCFProblemset = regexp.MustCompile(`(?i)/problemset/problem/(\d+)/([A-Za-z0-9]+)`)
	reURLCFGym        = regexp.MustCompile(`(?i)/gym/(\d+)/problem/([A-Za-z0-9]+)`)
	// group/{hash}/contest/{id}/problem/{index}
	reURLCFGroup = regexp.MustCompile(`(?i)/group/[A-Za-z0-9]+/contest/(\d+)/problem/([A-Za-z0-9]+)`)
	reURLAtCoder = regexp.MustCompile(`(?i)/contests/([a-z0-9_]+)/tasks/([a-z0-9_]+)`)
	// 短链 /tasks/{task_id}（无 contest 段）
	reURLAtCoderTask = regexp.MustCompile(`(?i)/tasks/([a-z0-9_]+)`)
	// 洛谷：P1001 / B2001 / CF1A / AT_abc123_a / SP1 / UVA100 / T1000…
	reURLLuoGu        = regexp.MustCompile(`(?i)/problem/([A-Za-z][A-Za-z0-9_]*)`)
	reURLLeetCode     = regexp.MustCompile(`(?i)/problems/([a-z0-9]+(?:-[a-z0-9]+)*)`)
	reURLNowCoderACM  = regexp.MustCompile(`(?i)/acm/problem/(\d+)`)
	reURLNowCoderPrac = regexp.MustCompile(`(?i)/practice/([0-9a-fA-F-]{32,36})`)
	reURLQOJ          = regexp.MustCompile(`(?i)/problem/(\d+)`)
)

// ParseProblemURL 从常见 OJ 链接解析题目身份；无法识别时返回 error（调用方忽略即可）
func ParseProblemURL(raw string) (*ParsedProblem, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("empty url")
	}
	// 允许用户只贴路径
	if !strings.Contains(raw, "://") {
		raw = "https://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	host := strings.ToLower(u.Host)
	path := u.Path
	full := host + path

	// Codeforces（含 gym / group / problemset）
	if strings.Contains(host, "codeforces.com") {
		if m := reURLCFContest.FindStringSubmatch(path); m != nil {
			return &ParsedProblem{
				Platform:   spider.CodeForces,
				ExternalID: m[1] + m[2],
				Title:      m[1] + m[2],
				URL:        fmt.Sprintf("https://codeforces.com/contest/%s/problem/%s", m[1], m[2]),
			}, nil
		}
		if m := reURLCFProblemset.FindStringSubmatch(path); m != nil {
			return &ParsedProblem{
				Platform:   spider.CodeForces,
				ExternalID: m[1] + m[2],
				Title:      m[1] + m[2],
				URL:        fmt.Sprintf("https://codeforces.com/problemset/problem/%s/%s", m[1], m[2]),
			}, nil
		}
		if m := reURLCFGym.FindStringSubmatch(path); m != nil {
			return &ParsedProblem{
				Platform:   spider.CodeForces,
				ExternalID: "gym" + m[1] + m[2],
				Title:      m[1] + m[2],
				URL:        fmt.Sprintf("https://codeforces.com/gym/%s/problem/%s", m[1], m[2]),
			}, nil
		}
		if m := reURLCFGroup.FindStringSubmatch(path); m != nil {
			// group 赛与正式 contest 共用 contestId+index 作为 external_id
			return &ParsedProblem{
				Platform:   spider.CodeForces,
				ExternalID: m[1] + m[2],
				Title:      m[1] + m[2],
				URL:        fmt.Sprintf("https://codeforces.com/contest/%s/problem/%s", m[1], m[2]),
			}, nil
		}
		return nil, fmt.Errorf("unrecognized codeforces url")
	}

	// AtCoder
	if strings.Contains(host, "atcoder.jp") {
		if m := reURLAtCoder.FindStringSubmatch(path); m != nil {
			return &ParsedProblem{
				Platform:   spider.AtCoder,
				ExternalID: m[2],
				Title:      m[2],
				URL:        fmt.Sprintf("https://atcoder.jp/contests/%s/tasks/%s", m[1], m[2]),
			}, nil
		}
		if m := reURLAtCoderTask.FindStringSubmatch(path); m != nil {
			task := m[1]
			return &ParsedProblem{
				Platform:   spider.AtCoder,
				ExternalID: task,
				Title:      task,
				URL:        atCoderTaskURL(task),
			}, nil
		}
		return nil, fmt.Errorf("unrecognized atcoder url")
	}

	// 洛谷
	if strings.Contains(host, "luogu.com") {
		if m := reURLLuoGu.FindStringSubmatch(path); m != nil {
			pid := m[1]
			// 规范大小写：前缀字母大写，AT_ 后段保留
			pid = normalizeLuoGuPID(pid)
			return &ParsedProblem{
				Platform:   spider.LuoGu,
				ExternalID: pid,
				Title:      pid,
				URL:        "https://www.luogu.com.cn/problem/" + pid,
			}, nil
		}
		return nil, fmt.Errorf("unrecognized luogu url")
	}

	// 力扣
	if strings.Contains(host, "leetcode.cn") || strings.Contains(host, "leetcode.com") {
		if m := reURLLeetCode.FindStringSubmatch(path); m != nil {
			slug := m[1]
			base := "https://leetcode.cn/problems/"
			if strings.Contains(host, "leetcode.com") && !strings.Contains(host, "leetcode.cn") {
				base = "https://leetcode.com/problems/"
			}
			return &ParsedProblem{
				Platform:   spider.LeetCode,
				ExternalID: slug,
				Title:      slug,
				URL:        base + slug + "/",
			}, nil
		}
		return nil, fmt.Errorf("unrecognized leetcode url")
	}

	// 牛客
	if strings.Contains(host, "nowcoder.com") {
		if m := reURLNowCoderACM.FindStringSubmatch(path); m != nil {
			return &ParsedProblem{
				Platform:   spider.NowCoder,
				ExternalID: m[1],
				Title:      m[1],
				URL:        "https://ac.nowcoder.com/acm/problem/" + m[1],
			}, nil
		}
		if m := reURLNowCoderPrac.FindStringSubmatch(path); m != nil {
			uuid := strings.ToLower(strings.ReplaceAll(m[1], "-", ""))
			return &ParsedProblem{
				Platform:   spider.NowCoder,
				ExternalID: uuid,
				Title:      uuid,
				URL:        "https://www.nowcoder.com/practice/" + uuid,
			}, nil
		}
		return nil, fmt.Errorf("unrecognized nowcoder url")
	}

	// QOJ
	if strings.Contains(host, "qoj.ac") || strings.HasPrefix(host, "qoj.") {
		if m := reURLQOJ.FindStringSubmatch(path); m != nil {
			return &ParsedProblem{
				Platform:   spider.QOJ,
				ExternalID: m[1],
				Title:      m[1],
				URL:        "https://qoj.ac/problem/" + m[1],
			}, nil
		}
		return nil, fmt.Errorf("unrecognized qoj url")
	}

	_ = full
	return nil, fmt.Errorf("unsupported problem url host=%s", host)
}
