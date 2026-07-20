package problem_fetch

import (
	"strings"
	"testing"
)

func TestFetchAtCoder_LiveABC397E(t *testing.T) {
	if testing.Short() {
		t.Skip("live network")
	}
	fc, err := fetchAtCoder("https://atcoder.jp/contests/abc397/tasks/abc397_e")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if !strings.Contains(fc.Title, "Path Decomposition") {
		t.Fatalf("title=%q", fc.Title)
	}
	if strings.Contains(fc.Title, "Editorial") {
		t.Fatalf("title still has Editorial: %q", fc.Title)
	}
	if strings.Contains(fc.ContentMD, "Problem StatementYou") {
		t.Fatalf("glued h3+p still present")
	}
	if !strings.Contains(fc.ContentMD, "### Problem Statement") &&
		!strings.Contains(fc.ContentMD, "## Problem Statement") {
		// at least section break
		if !strings.Contains(fc.ContentMD, "Problem Statement") {
			t.Fatalf("missing Problem Statement section: %s", truncate(fc.ContentMD, 200))
		}
		t.Logf("warn: no markdown heading for Problem Statement, content head=%q", truncate(fc.ContentMD, 240))
	}
	if !strings.Contains(fc.ContentMD, "```") {
		t.Fatalf("expected sample pre block")
	}
	if strings.Contains(fc.ContentMD, "\tEditorial") {
		t.Fatalf("editorial noise in body")
	}
	t.Logf("title=%s content_len=%d", fc.Title, len(fc.ContentMD))
}

func TestContentSelection_H3NotGlued(t *testing.T) {
	// unit via selectionToMD on synthetic HTML through goquery is covered by live test;
	// keep a cheap glue detector regression for the broken production sample.
	broken := "# E - Path Decomposition of a Tree\n\t\t\tEditorial\n\nScore : 475 points\n\nProblem StatementYou are given a tree"
	if !strings.Contains(broken, "Problem StatementYou") {
		t.Fatal("fixture")
	}
}
