package service

import (
	"strings"
	"testing"

	"cwxu-algo/app/core_data/internal/data/model"
)

func TestNormalizeEditTags(t *testing.T) {
	got := normalizeEditTags([]string{" 动态规划 ", "前缀和", "动态规划", "", "  "})
	if len(got) != 2 || got[0] != "动态规划" || got[1] != "前缀和" {
		t.Fatalf("unexpected tags: %#v", got)
	}
}

func TestProblemEditPendingSummaryIncludesChangedFields(t *testing.T) {
	req := &model.ProblemEditRequest{
		HasTags:           true,
		HasContent:        true,
		ProposedTags:      model.StringArray{"动态规划", "前缀和"},
		ProposedContentMD: "这是一段新题面",
		ProposedTitle:     "新的题目标题",
		Note:              "修正样例与标签",
	}
	body := problemEditPendingSummary("原题目", req)
	for _, want := range []string{"原题目", "标题改为", "题面内容", "动态规划、前缀和", "修正样例与标签"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body=%q missing %q", body, want)
		}
	}
}

func TestProblemEditApprovalThankYouListsApprovedFields(t *testing.T) {
	req := &model.ProblemEditRequest{
		HasTags:       true,
		HasContent:    true,
		ProposedTitle: "新标题",
	}
	body := problemEditApprovalThankYou(nil, req)
	for _, want := range []string{"题目标题", "题面内容", "题目标签", "感谢你为 GoAlgo 作出贡献"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body=%q missing %q", body, want)
		}
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
