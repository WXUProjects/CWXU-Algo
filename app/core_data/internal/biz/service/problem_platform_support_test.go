package service

import (
	"strings"
	"testing"
)

// 六大 OJ：加题链接识别 + 提交字段识别 必须都能出稳定 Platform/ExternalID/URL。

func TestParseProblemURL_AllPlatforms(t *testing.T) {
	cases := []struct {
		name   string
		raw    string
		plat   string
		ext    string
		urlSub string
	}{
		{"cf contest", "https://codeforces.com/contest/1791/problem/A", "CodeForces", "1791A", "contest/1791"},
		{"cf problemset", "https://codeforces.com/problemset/problem/1791/A", "CodeForces", "1791A", "problemset"},
		{"cf gym", "https://codeforces.com/gym/102861/problem/A", "CodeForces", "gym102861A", "gym/102861"},
		{"cf group", "https://codeforces.com/group/abcXYZ/contest/1234/problem/B", "CodeForces", "1234B", "contest/1234"},
		{"atcoder", "https://atcoder.jp/contests/abc123/tasks/abc123_a", "AtCoder", "abc123_a", "contests/abc123"},
		{"atcoder tasks short", "https://atcoder.jp/tasks/abc123_a", "AtCoder", "abc123_a", "abc123"},
		{"luogu P", "https://www.luogu.com.cn/problem/P1001", "LuoGu", "P1001", "P1001"},
		{"luogu CF mirror", "https://www.luogu.com.cn/problem/CF1A", "LuoGu", "CF1A", "CF1A"},
		{"luogu AT", "https://www.luogu.com.cn/problem/AT_abc123_a", "LuoGu", "AT_abc123_a", "AT_abc123_a"},
		{"luogu B", "https://www.luogu.com.cn/problem/B2001", "LuoGu", "B2001", "B2001"},
		{"leetcode cn", "https://leetcode.cn/problems/two-sum/", "LeetCode", "two-sum", "two-sum"},
		{"leetcode com", "https://leetcode.com/problems/two-sum/", "LeetCode", "two-sum", "two-sum"},
		{"nowcoder ac", "https://ac.nowcoder.com/acm/problem/12345", "NowCoder", "12345", "acm/problem/12345"},
		{"nowcoder practice", "https://www.nowcoder.com/practice/abcdef0123456789abcdef0123456789", "NowCoder", "abcdef0123456789abcdef0123456789", "practice/"},
		{"qoj", "https://qoj.ac/problem/1234", "QOJ", "1234", "problem/1234"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParseProblemURL(tc.raw)
			if err != nil {
				t.Fatalf("err=%v", err)
			}
			if p.Platform != tc.plat {
				t.Fatalf("platform=%s want %s", p.Platform, tc.plat)
			}
			if p.ExternalID != tc.ext {
				t.Fatalf("ext=%s want %s", p.ExternalID, tc.ext)
			}
			if p.URL == "" || !strings.Contains(p.URL, tc.urlSub) {
				t.Fatalf("url=%s want contain %s", p.URL, tc.urlSub)
			}
			if p.SkipFetch || p.SkipBank {
				t.Fatalf("should not skip: %+v", p)
			}
		})
	}
}

func TestParseProblemIdentity_AllPlatforms(t *testing.T) {
	cases := []struct {
		name     string
		platform string
		contest  string
		problem  string
		ext      string
		urlSub   string
	}{
		{"cf", "CodeForces", "1791", "A-Theatre Square", "1791A", "contest/1791"},
		{"cf gym negative", "CodeForces", "-102861", "A-Gym Task", "gym102861A", "gym/102861"},
		{"atcoder", "AtCoder", "abc123", "abc123_a", "abc123_a", "contests/abc123"},
		{"atcoder no contest", "AtCoder", "", "abc123_a", "abc123_a", "contests/abc123"},
		{"luogu P", "LuoGu", "", "P1001 A+B Problem", "P1001", "P1001"},
		{"luogu CF", "LuoGu", "", "CF1A Theatre Square", "CF1A", "CF1A"},
		{"luogu AT", "LuoGu", "", "AT_abc123_a Task A", "AT_abc123_a", "AT_abc123_a"},
		{"nowcoder ac", "NowCoder", "", "12345 题目标题", "12345", "acm/problem/12345"},
		{"nowcoder uuid", "NowCoder", "", "abcdef0123456789abcdef0123456789 练习题", "abcdef0123456789abcdef0123456789", "practice/"},
		{"qoj", "QOJ", "", "#1234 Some", "1234", "problem/1234"},
		{"leetcode", "LeetCode", "leetcode", "two-sum 两数之和", "two-sum", "two-sum"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			p, err := ParseProblemIdentity(tc.platform, tc.contest, tc.problem)
			if err != nil {
				t.Fatalf("err=%v", err)
			}
			if p.ExternalID != tc.ext {
				t.Fatalf("ext=%s want %s", p.ExternalID, tc.ext)
			}
			if p.URL == "" || !strings.Contains(p.URL, tc.urlSub) {
				t.Fatalf("url=%s want contain %s", p.URL, tc.urlSub)
			}
		})
	}
}

func TestAtCoderTaskURLInference(t *testing.T) {
	u := atCoderTaskURL("abc350_f")
	if !strings.Contains(u, "contests/abc350/tasks/abc350_f") {
		t.Fatalf("url=%s", u)
	}
}

func TestNormalizeLuoGuPID(t *testing.T) {
	if got := normalizeLuoGuPID("p1001"); got != "P1001" {
		t.Fatalf("got %s", got)
	}
	if got := normalizeLuoGuPID("cf1a"); got != "CF1A" {
		t.Fatalf("got %s", got)
	}
	if got := normalizeLuoGuPID("AT_abc123_a"); got != "AT_abc123_a" {
		t.Fatalf("got %s", got)
	}
}
