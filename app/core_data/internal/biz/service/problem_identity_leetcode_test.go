package service

import (
	"strings"
	"testing"
)

func TestParseLeetCodeIdentity(t *testing.T) {
	p, err := ParseProblemIdentity("LeetCode", "leetcode", "two-sum 两数之和")
	if err != nil {
		t.Fatal(err)
	}
	if p.ExternalID != "two-sum" || p.SkipBank || p.SkipFetch {
		t.Fatalf("got %+v", p)
	}
	if !strings.Contains(p.URL, "two-sum") {
		t.Fatalf("url=%s", p.URL)
	}
	if p.Title != "两数之和" {
		t.Fatalf("title=%s", p.Title)
	}

	_, err = ParseProblemIdentity("LeetCode", "leetcode", "lc-ac-problem-3")
	if err == nil {
		t.Fatal("synthetic should fail")
	}
	_, err = ParseProblemIdentity("LeetCode", "leetcode", "leetcode-submit")
	if err == nil {
		t.Fatal("calendar submit should fail")
	}
}
