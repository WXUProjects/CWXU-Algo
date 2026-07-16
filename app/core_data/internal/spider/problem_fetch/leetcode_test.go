package problem_fetch

import (
	"strings"
	"testing"
)

func TestFetchLeetCodeLive(t *testing.T) {
	fc, err := Fetch("LeetCode", "two-sum", "https://leetcode.cn/problems/two-sum/")
	if err != nil {
		t.Fatalf("two-sum: %v", err)
	}
	if fc.Title == "" || len(fc.ContentMD) < 50 {
		t.Fatalf("empty content title=%q len=%d", fc.Title, len(fc.ContentMD))
	}
	if !strings.Contains(fc.ContentMD, "target") && !strings.Contains(fc.ContentMD, "目标") {
		t.Fatalf("unexpected content head: %q", fc.ContentMD[:min(120, len(fc.ContentMD))])
	}
	t.Logf("title=%s md_len=%d", fc.Title, len(fc.ContentMD))

	_, err = Fetch("LeetCode", "count-strictly-increasing-subarrays", "")
	if err == nil {
		t.Fatal("expected paid/no-content error")
	}
	if !strings.Contains(err.Error(), "付费") && !strings.Contains(err.Error(), "无公开") {
		t.Fatalf("want 付费/无公开题面 got %v", err)
	}
	t.Logf("paid err: %v", err)
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
