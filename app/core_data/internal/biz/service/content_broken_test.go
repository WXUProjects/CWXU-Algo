package service

import "testing"

func TestContentLooksBroken(t *testing.T) {
	cases := []struct {
		md   string
		want bool
	}{
		{"", false},
		{"# Title\n\n### Problem Statement\n\nYou are given...", false},
		{"Problem StatementYou are given a tree", true},
		{"# E\n\t\t\tEditorial\n\nScore", true},
		{"Constraints1 \\leq N", true},
		{"InputThe input is given", true},
		{"OutputIf it is possible", true},
	}
	for _, c := range cases {
		if got := ContentLooksBroken(c.md); got != c.want {
			t.Fatalf("ContentLooksBroken(%q)=%v want %v", c.md, got, c.want)
		}
	}
}
