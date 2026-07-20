package service

import (
	"strings"
	"testing"
	"time"

	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
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

func TestHtmlEscapePlain(t *testing.T) {
	got := htmlEscapePlain(`a <b> & "x"`)
	if !strings.Contains(got, "&lt;") || !strings.Contains(got, "&amp;") {
		t.Fatalf("got %q", got)
	}
}

func TestListProblemContributorsDistinctByFirstApprove(t *testing.T) {
	dsn := "file:problem_contrib_" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&model.ProblemEditRequest{}); err != nil {
		t.Fatal(err)
	}
	t1 := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	t2 := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	t3 := time.Date(2026, 1, 3, 0, 0, 0, 0, time.UTC)
	rows := []model.ProblemEditRequest{
		{ProblemID: 9, UserID: 2, Status: model.ProblemEditApproved, UpdatedAt: t2},
		{ProblemID: 9, UserID: 1, Status: model.ProblemEditApproved, UpdatedAt: t1},
		// 同一用户再次通过：仍只出现一次
		{ProblemID: 9, UserID: 1, Status: model.ProblemEditApproved, UpdatedAt: t3},
		{ProblemID: 9, UserID: 3, Status: model.ProblemEditRejected, UpdatedAt: t1},
		{ProblemID: 8, UserID: 1, Status: model.ProblemEditApproved, UpdatedAt: t1},
	}
	for i := range rows {
		if err := db.Create(&rows[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	uc := &ProblemUseCase{data: &data.Data{DB: db}}
	ids, err := uc.ListProblemContributors(9)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
		t.Fatalf("ids=%v want [1 2] by first approve time", ids)
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


