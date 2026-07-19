package problem_fetch

import (
	"os"
	"strings"
	"testing"
)

func TestLiveNowCoderParse319811(t *testing.T) {
	if os.Getenv("CWXU_LIVE_NC") != "1" {
		t.Skip("CWXU_LIVE_NC=1")
	}
	fc, err := FetchWithFallbacks("NowCoder", "319811",
		"https://ac.nowcoder.com/acm/problem/319811",
		[]string{"https://ac.nowcoder.com/acm/contest/137561/A"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("title=%s\n%s", fc.Title, fc.ContentMD)
	md := fc.ContentMD
	if !strings.Contains(md, "## 输入描述") || !strings.Contains(md, "## 输出描述") {
		t.Fatal("missing IO desc")
	}
	if strings.Contains(md, "复制") {
		t.Fatal("copy button leaked")
	}
	// 不应有把样例 h2「输入」当成二级描述的空段：## 输入\n\n## 输出
	if strings.Contains(md, "## 输入\n") && !strings.Contains(md, "## 输入描述") {
		t.Fatal("bare ## 输入 heading")
	}
	if !strings.Contains(md, "### 输入") || !strings.Contains(md, "R.I.P") {
		t.Fatal("sample missing")
	}
}
