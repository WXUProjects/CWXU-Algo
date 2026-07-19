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
	t.Logf("title=%s md_len=%d", fc.Title, len(fc.ContentMD))
}
