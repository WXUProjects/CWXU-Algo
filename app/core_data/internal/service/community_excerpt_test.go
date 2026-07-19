package service

import (
	"strings"
	"testing"

	"cwxu-algo/app/common/blogtext"
)

// 社区 excerpt 必须与博客简述同源（blogtext），不能各写一套。
func TestExcerpt_sharesBlogtextDefaultSummary(t *testing.T) {
	src := `# [Digit Circus](https://vjudge.net/problem/AtCoder-abc465_e)
**涉及知识点：** [[数位DP]]、状态压缩、记忆化搜索
**题目描述**
请你计算满足以下三个条件中恰好一个的整数 $x$ 的个数，范围是 $1 \le x \le N$，结果对 998244353 取模。
条件一是判断 3 的倍数。
`
	got := excerpt(src, 0)
	want := blogtext.DefaultSummary(src)
	if got != want {
		t.Fatalf("excerpt must equal blogtext.DefaultSummary\ngot  %q\nwant %q", got, want)
	}
	if strings.Contains(got, "vjudge.net") || strings.Contains(got, "**") || strings.Contains(got, "[[") {
		t.Fatalf("markdown noise remains: %q", got)
	}
	if !strings.Contains(got, "请你计算") {
		t.Fatalf("expected body text: %q", got)
	}
}
