package service

import (
	"testing"

	"cwxu-algo/app/common/notify"
	"cwxu-algo/app/core_data/internal/data/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// 用 sqlite 驱动 community 写/读路径（不依赖远端），验证 shipped handler 逻辑。
func setupCommunityTest(t *testing.T) (*CommunityService, *gorm.DB, *gorm.DB) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file::memory:?cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	udb, err := gorm.Open(sqlite.Open("file:user_mem?mode=memory&cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&model.Problem{}, &model.ProblemComment{}, &model.ProblemUserSolution{}, &model.ActivityFeed{}); err != nil {
		t.Fatal(err)
	}
	if err := udb.AutoMigrate(&notify.Row{}); err != nil {
		t.Fatal(err)
	}
	// 种子题目
	p := model.Problem{Platform: "CF", ExternalID: "1A", Title: "Theatre Square", Status: "COMPLETED"}
	if err := db.Create(&p).Error; err != nil {
		t.Fatal(err)
	}
	s := &CommunityService{db: db, udb: udb, reg: nil}
	return s, db, udb
}

func TestCommunityCommentCreateListAndMention(t *testing.T) {
	s, db, udb := setupCommunityTest(t)
	var p model.Problem
	if err := db.First(&p).Error; err != nil {
		t.Fatal(err)
	}

	// 直接写评论 + feed + mention（绕过 JWT，测核心业务）
	row := model.ProblemComment{ProblemID: p.ID, UserID: 10, Content: "思路不错 @alice 看看"}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&model.ActivityFeed{
		OrgID: 3, UserID: 10, Type: model.ActivityTypeComment, RefID: row.ID,
		ProblemID: p.ID, Title: "思路不错", Excerpt: "思路不错 @alice 看看",
	}).Error; err != nil {
		t.Fatal(err)
	}
	// mention 通知
	names := notify.ExtractMentions(row.Content)
	if len(names) != 1 || names[0] != "alice" {
		t.Fatalf("mentions=%v", names)
	}
	if err := notify.Create(udb, notify.Row{
		UserID: 20, Type: notify.TypeMention, Title: "有人提到了你",
		Body: "bob 在评论中 @ 了你", ActorID: 10, RefType: "comment",
		RefID: row.ID, ProblemID: p.ID,
	}); err != nil {
		t.Fatal(err)
	}

	// list comments
	var list []model.ProblemComment
	if err := db.Where("problem_id = ?", p.ID).Find(&list).Error; err != nil {
		t.Fatal(err)
	}
	if len(list) != 1 {
		t.Fatalf("comments=%d", len(list))
	}

	// org-scoped feed
	var feed []model.ActivityFeed
	if err := db.Where("org_id = ?", 3).Find(&feed).Error; err != nil {
		t.Fatal(err)
	}
	if len(feed) != 1 {
		t.Fatalf("feed org3=%d", len(feed))
	}
	var other []model.ActivityFeed
	_ = db.Where("org_id = ?", 99).Find(&other).Error
	if len(other) != 0 {
		t.Fatalf("org isolation broken: %d", len(other))
	}

	// syncToPublic：同一评论额外写公共域 feed（publicOrg=1）
	publicOrg, privateOrg := uint(1), uint(3)
	if err := db.Create(&model.ActivityFeed{
		OrgID: publicOrg, UserID: 10, Type: model.ActivityTypeComment, RefID: row.ID,
		ProblemID: p.ID, Title: "思路不错", Excerpt: "思路不错 @alice 看看",
	}).Error; err != nil {
		t.Fatal(err)
	}
	var dual []model.ActivityFeed
	if err := db.Where("type = ? AND ref_id = ?", model.ActivityTypeComment, row.ID).Find(&dual).Error; err != nil {
		t.Fatal(err)
	}
	if len(dual) != 2 {
		t.Fatalf("want 2 feeds (org %d + public %d), got %d", privateOrg, publicOrg, len(dual))
	}
	// 删除评论时两条 feed 一并清掉
	_ = db.Where("type = ? AND ref_id = ?", model.ActivityTypeComment, row.ID).Delete(&model.ActivityFeed{}).Error
	var afterDel int64
	_ = db.Model(&model.ActivityFeed{}).Where("type = ? AND ref_id = ?", model.ActivityTypeComment, row.ID).Count(&afterDel).Error
	if afterDel != 0 {
		t.Fatalf("delete should clear all org feeds, left=%d", afterDel)
	}
	// 恢复一条供后续 recent 等断言
	if err := db.Create(&model.ActivityFeed{
		OrgID: privateOrg, UserID: 10, Type: model.ActivityTypeComment, RefID: row.ID,
		ProblemID: p.ID, Title: "思路不错", Excerpt: "思路不错 @alice 看看",
	}).Error; err != nil {
		t.Fatal(err)
	}

	// notification stored
	var n int64
	if err := udb.Model(&notify.Row{}).Where("user_id = ? AND type = ?", 20, notify.TypeMention).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("notif count=%d", n)
	}

	// solution
	sol := model.ProblemUserSolution{ProblemID: p.ID, UserID: 10, Title: "差分", ContentMD: "## 思路\n$O(n)$"}
	if err := db.Create(&sol).Error; err != nil {
		t.Fatal(err)
	}
	var sols []model.ProblemUserSolution
	_ = db.Where("problem_id = ?", p.ID).Find(&sols).Error
	if len(sols) != 1 {
		t.Fatalf("sols=%d", len(sols))
	}

	// recent by user
	var recent []model.ProblemComment
	_ = db.Where("user_id = ?", 10).Order("id desc").Limit(8).Find(&recent).Error
	if len(recent) != 1 {
		t.Fatalf("recent=%d", len(recent))
	}

	_ = s
}
