package service

import (
	"testing"

	"cwxu-algo/app/core_data/internal/data/model"
)

func TestNormalizeEditTags(t *testing.T) {
	got := normalizeEditTags([]string{" 动态规划 ", "前缀和", "动态规划", "", "  "})
	if len(got) != 2 || got[0] != "动态规划" || got[1] != "前缀和" {
		t.Fatalf("unexpected tags: %#v", got)
	}
}

func TestNonEmptyTags(t *testing.T) {
	if len(nonEmptyTags(model.StringArray{})) != 0 {
		t.Fatal("empty should be empty")
	}
	if len(nonEmptyTags(model.StringArray{"", "  "})) != 0 {
		t.Fatal("blank tags should be empty")
	}
	if len(nonEmptyTags(model.StringArray{"图论"})) != 1 {
		t.Fatal("expected one tag")
	}
}
