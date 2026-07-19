package problem_fetch

import (
	"os"
	"strings"
	"testing"
)

// 需外网；默认跳过。CWXU_LIVE_NC=1 时跑。
func TestLiveNowCoderContestFallback(t *testing.T) {
	if os.Getenv("CWXU_LIVE_NC") != "1" {
		t.Skip("set CWXU_LIVE_NC=1 to run live NowCoder fetch")
	}
	bank := "https://ac.nowcoder.com/acm/problem/319811"
	contest := "https://ac.nowcoder.com/acm/contest/137561/A"
	fc, err := FetchWithFallbacks("NowCoder", "319811", bank, []string{contest})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !strings.Contains(fc.Title, "小红") && !strings.Contains(fc.ContentMD, "字符串") {
		t.Fatalf("unexpected content title=%q md[:120]=%q", fc.Title, truncate(fc.ContentMD, 120))
	}
	md := fc.ContentMD
	if strings.Contains(md, "复制") {
		t.Fatalf("copy button leaked into md:\n%s", md)
	}
	if c := strings.Count(md, "## 输入描述"); c != 1 {
		t.Fatalf("## 输入描述 count=%d\n%s", c, md)
	}
	if c := strings.Count(md, "## 输出描述"); c != 1 {
		t.Fatalf("## 输出描述 count=%d\n%s", c, md)
	}
	// 输入描述里公式应在（长为 $n$ / $3$ 等）
	if !strings.Contains(md, "$") {
		t.Fatalf("expected formula markers in md:\n%s", md)
	}
	if !strings.Contains(md, "示例") && !strings.Contains(md, "```") {
		t.Fatalf("expected samples:\n%s", md)
	}
	t.Logf("title=%s\n%s", fc.Title, md)
}

// 主站 practice：无 ACM 式输入描述，不应从样例 h2 编造「输入描述」
func TestLiveNowCoderMainPractice(t *testing.T) {
	if os.Getenv("CWXU_LIVE_NC") != "1" {
		t.Skip("set CWXU_LIVE_NC=1")
	}
	u := "https://www.nowcoder.com/practice/75e878df47f24fdc9dc3e400ec6058ca"
	uuid := u[strings.LastIndex(u, "/")+1:]
	fc, err := Fetch("NowCoder", uuid, u)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !strings.Contains(fc.Title, "反转链表") {
		t.Fatalf("title=%q", fc.Title)
	}
	if strings.Contains(fc.ContentMD, "## 输入描述") {
		t.Fatalf("interview problem should not invent 输入描述:\n%s", fc.ContentMD)
	}
	if !strings.Contains(fc.ContentMD, "{1,2,3}") {
		t.Fatalf("sample missing:\n%s", fc.ContentMD)
	}
	t.Logf("title=%s md_len=%d", fc.Title, len(fc.ContentMD))
}
