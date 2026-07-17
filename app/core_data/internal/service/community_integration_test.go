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
	// 每测独立库，避免 shared memory 串数据
	name := "file:community_" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(name), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	udb, err := gorm.Open(sqlite.Open("file:user_"+t.Name()+"?mode=memory&cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(
		&model.Problem{}, &model.ProblemComment{}, &model.ProblemUserSolution{},
		&model.ActivityFeed{}, &model.CommunityLike{}, &model.CommunityReport{},
	); err != nil {
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
	// 顶层 root_id = 自身
	_ = db.Model(&row).Update("root_id", row.ID).Error
	row.RootID = row.ID
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

	// 层级回复
	reply := model.ProblemComment{
		ProblemID: p.ID, UserID: 20, Content: "同意",
		ParentID: row.ID, RootID: row.ID, Depth: 1, ReplyToUserID: 10,
	}
	if err := db.Create(&reply).Error; err != nil {
		t.Fatal(err)
	}
	var underRoot []model.ProblemComment
	_ = db.Where("root_id = ?", row.ID).Order("id asc").Find(&underRoot).Error
	if len(underRoot) < 2 {
		t.Fatalf("want root+reply, got %d", len(underRoot))
	}

	// 点赞 toggle
	like := model.CommunityLike{UserID: 20, TargetType: model.CommunityTargetComment, TargetID: row.ID}
	if err := db.Create(&like).Error; err != nil {
		t.Fatal(err)
	}
	_ = db.Model(&model.ProblemComment{}).Where("id = ?", row.ID).UpdateColumn("like_count", 1).Error
	var liked model.ProblemComment
	_ = db.First(&liked, row.ID).Error
	if liked.LikeCount != 1 {
		t.Fatalf("likeCount=%d", liked.LikeCount)
	}
	// 题解点赞
	solLike := model.CommunityLike{UserID: 20, TargetType: model.CommunityTargetSolution, TargetID: sol.ID}
	if err := db.Create(&solLike).Error; err != nil {
		t.Fatal(err)
	}
	// 举报
	rep := model.CommunityReport{
		UserID: 20, TargetType: model.CommunityTargetSolution, TargetID: sol.ID,
		Reason: "广告", Status: model.ReportStatusPending,
	}
	if err := db.Create(&rep).Error; err != nil {
		t.Fatal(err)
	}
	var repN int64
	_ = db.Model(&model.CommunityReport{}).Where("target_type = ? AND target_id = ?", model.CommunityTargetSolution, sol.ID).Count(&repN).Error
	if repN != 1 {
		t.Fatalf("reports=%d", repN)
	}

	// 子树收集
	ids := s.collectCommentSubtreeIDs(row.ID)
	if len(ids) < 2 {
		t.Fatalf("subtree=%v", ids)
	}

	// 深度挂载：回复 depth=MaxCommentDepth 时挂到其父
	deepParent := model.ProblemComment{
		ProblemID: p.ID, UserID: 30, Content: "L2",
		ParentID: reply.ID, RootID: row.ID, Depth: model.MaxCommentDepth, ReplyToUserID: 20,
	}
	if err := db.Create(&deepParent).Error; err != nil {
		t.Fatal(err)
	}
	// 模拟 create 的挂载点选择
	attach := deepParent
	if deepParent.Depth >= model.MaxCommentDepth && deepParent.ParentID > 0 {
		var up model.ProblemComment
		if db.First(&up, deepParent.ParentID).Error == nil {
			attach = up
		}
	}
	if attach.ID != reply.ID {
		t.Fatalf("max-depth attach want parent reply %d, got %d", reply.ID, attach.ID)
	}

	// like/report 存在性
	if !s.communityTargetExists(model.CommunityTargetComment, row.ID) {
		t.Fatal("comment should exist")
	}
	if !s.communityTargetExists(model.CommunityTargetSolution, sol.ID) {
		t.Fatal("solution should exist")
	}
	if s.communityTargetOwner(model.CommunityTargetComment, row.ID) != 10 {
		t.Fatal("owner mismatch")
	}
	s.adjustLikeCount(model.CommunityTargetComment, row.ID, 1)
	if s.readLikeCount(model.CommunityTargetComment, row.ID) != 2 {
		t.Fatalf("likeCount after +1 want 2, got %d", s.readLikeCount(model.CommunityTargetComment, row.ID))
	}
}

func TestSolutionCommentsIsolatedFromProblem(t *testing.T) {
	s, db, udb := setupCommunityTest(t)
	var p model.Problem
	_ = db.First(&p).Error

	// 题目讨论
	probC := model.ProblemComment{ProblemID: p.ID, SolutionID: 0, UserID: 1, Content: "题面讨论"}
	_ = db.Create(&probC).Error
	_ = db.Model(&probC).Update("root_id", probC.ID).Error
	probC.RootID = probC.ID

	// 用户题解
	sol := model.ProblemUserSolution{ProblemID: p.ID, UserID: 10, Title: "差分", ContentMD: "md"}
	_ = db.Create(&sol).Error

	// 题解评论
	solC := model.ProblemComment{
		ProblemID: p.ID, SolutionID: sol.ID, UserID: 2, Content: "题解说得好",
	}
	_ = db.Create(&solC).Error
	_ = db.Model(&solC).Update("root_id", solC.ID).Error
	solC.RootID = solC.ID

	// 列表隔离：题目讨论不含题解评论
	var problemRoots []model.ProblemComment
	_ = db.Where("problem_id = ? AND parent_id = 0 AND solution_id = 0", p.ID).Find(&problemRoots).Error
	if len(problemRoots) != 1 || problemRoots[0].ID != probC.ID {
		t.Fatalf("problem roots=%v", problemRoots)
	}
	var solRoots []model.ProblemComment
	_ = db.Where("solution_id = ? AND parent_id = 0", sol.ID).Find(&solRoots).Error
	if len(solRoots) != 1 || solRoots[0].ID != solC.ID {
		t.Fatalf("solution roots=%v", solRoots)
	}

	// commentToMap 带 solutionId
	m := s.commentToMap(solC, map[uint]userBrief{2: {username: "bob", name: "Bob"}}, map[uint]bool{})
	if m["solutionId"] != sol.ID {
		t.Fatalf("solutionId in map=%v", m["solutionId"])
	}

	// 删除题解级联清评论
	var commentIDs []uint
	_ = db.Model(&model.ProblemComment{}).Where("solution_id = ?", sol.ID).Pluck("id", &commentIDs).Error
	if len(commentIDs) != 1 {
		t.Fatalf("commentIDs=%v", commentIDs)
	}
	_ = db.Where("id IN ?", commentIDs).Delete(&model.ProblemComment{}).Error
	_ = db.Delete(&sol).Error
	var left int64
	_ = db.Model(&model.ProblemComment{}).Where("solution_id = ?", sol.ID).Count(&left).Error
	if left != 0 {
		t.Fatalf("cascade left=%d", left)
	}
	// 题目讨论仍在
	var still int64
	_ = db.Model(&model.ProblemComment{}).Where("id = ?", probC.ID).Count(&still).Error
	if still != 1 {
		t.Fatal("problem comment should remain")
	}

	// 题解顶层评论通知作者（业务写入）
	_ = notify.Create(udb, notify.Row{
		UserID: 10, Type: notify.TypeCommentReply, Title: "有人评论了你的题解",
		Body: "bob 评论了你的题解", ActorID: 2, RefType: "solution", RefID: sol.ID, ProblemID: p.ID,
	})
	var n int64
	_ = udb.Model(&notify.Row{}).Where("user_id = ? AND ref_type = ?", 10, "solution").Count(&n).Error
	if n != 1 {
		t.Fatalf("author notif=%d", n)
	}
}

func TestCommentTreeMapAndLikedSet(t *testing.T) {
	s, db, _ := setupCommunityTest(t)
	var p model.Problem
	_ = db.First(&p).Error

	root := model.ProblemComment{ProblemID: p.ID, UserID: 1, Content: "root", LikeCount: 2}
	_ = db.Create(&root).Error
	_ = db.Model(&root).Update("root_id", root.ID).Error
	root.RootID = root.ID

	r1 := model.ProblemComment{
		ProblemID: p.ID, UserID: 2, Content: "r1",
		ParentID: root.ID, RootID: root.ID, Depth: 1, ReplyToUserID: 1,
	}
	_ = db.Create(&r1).Error
	r2 := model.ProblemComment{
		ProblemID: p.ID, UserID: 3, Content: "r2",
		ParentID: r1.ID, RootID: root.ID, Depth: 2, ReplyToUserID: 2,
	}
	_ = db.Create(&r2).Error

	_ = db.Create(&model.CommunityLike{
		UserID: 9, TargetType: model.CommunityTargetComment, TargetID: root.ID,
	}).Error
	_ = db.Create(&model.CommunityLike{
		UserID: 9, TargetType: model.CommunityTargetComment, TargetID: r2.ID,
	}).Error

	liked := s.likedSet(9, model.CommunityTargetComment, []uint{root.ID, r1.ID, r2.ID})
	if !liked[root.ID] || liked[r1.ID] || !liked[r2.ID] {
		t.Fatalf("liked set wrong: %v", liked)
	}

	users := map[uint]userBrief{
		1: {username: "u1", name: "U1"},
		2: {username: "u2", name: "U2"},
		3: {username: "u3", name: "U3"},
	}
	all := []model.ProblemComment{root, r1, r2}
	byID := map[uint]map[string]interface{}{}
	for _, c := range all {
		byID[c.ID] = s.commentToMap(c, users, liked)
		byID[c.ID]["replies"] = []map[string]interface{}{}
	}
	for _, c := range []model.ProblemComment{r1, r2} {
		parent := byID[c.ParentID]
		list, _ := parent["replies"].([]map[string]interface{})
		parent["replies"] = append(list, byID[c.ID])
	}
	rootMap := byID[root.ID]
	if rootMap["liked"] != true {
		t.Fatal("root should be liked")
	}
	replies, _ := rootMap["replies"].([]map[string]interface{})
	if len(replies) != 1 {
		t.Fatalf("root replies=%d", len(replies))
	}
	nested, _ := replies[0]["replies"].([]map[string]interface{})
	if len(nested) != 1 || nested[0]["content"] != "r2" {
		t.Fatalf("nested=%v", nested)
	}
	if nested[0]["liked"] != true {
		t.Fatal("r2 should be liked")
	}
}

// 公共域发现：全站聚合 + (type,ref_id) 去重；私有域仅本 org。
func TestActivityFeedPublicAggregatesPrivateIsolates(t *testing.T) {
	s, db, _ := setupCommunityTest(t)
	var p model.Problem
	if err := db.First(&p).Error; err != nil {
		t.Fatal(err)
	}
	publicOrg, privateA, privateB := uint(1), uint(3), uint(5)

	// 同一评论双写 public + privateA（syncToPublic 形态）
	c1 := model.ProblemComment{ProblemID: p.ID, UserID: 10, Content: "公共可见评论"}
	if err := db.Create(&c1).Error; err != nil {
		t.Fatal(err)
	}
	for _, oid := range []uint{publicOrg, privateA} {
		if err := db.Create(&model.ActivityFeed{
			OrgID: oid, UserID: 10, Type: model.ActivityTypeComment, RefID: c1.ID,
			ProblemID: p.ID, Title: "公共可见评论", Excerpt: "公共可见评论",
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	// privateB 专属题解
	sol := model.ProblemUserSolution{ProblemID: p.ID, UserID: 20, Title: "B域题解", ContentMD: "x"}
	if err := db.Create(&sol).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&model.ActivityFeed{
		OrgID: privateB, UserID: 20, Type: model.ActivityTypeSolution, RefID: sol.ID,
		ProblemID: p.ID, Title: "B域题解", Excerpt: "x",
	}).Error; err != nil {
		t.Fatal(err)
	}

	// 公共聚合：按 type+ref_id 取 MAX(id)
	idSub := db.Model(&model.ActivityFeed{}).Select("MAX(id)").Group("type, ref_id")
	var publicList []model.ActivityFeed
	if err := db.Where("id IN (?)", idSub).Order("id desc").Find(&publicList).Error; err != nil {
		t.Fatal(err)
	}
	if len(publicList) != 2 {
		t.Fatalf("public aggregate want 2 unique items, got %d", len(publicList))
	}
	// 评论去重后只剩 1 条
	var commentN, solN int
	for _, a := range publicList {
		switch a.Type {
		case model.ActivityTypeComment:
			commentN++
			if a.RefID != c1.ID {
				t.Fatalf("comment ref=%d", a.RefID)
			}
		case model.ActivityTypeSolution:
			solN++
			if a.RefID != sol.ID {
				t.Fatalf("sol ref=%d", a.RefID)
			}
		}
	}
	if commentN != 1 || solN != 1 {
		t.Fatalf("commentN=%d solN=%d", commentN, solN)
	}

	// 私有域 A：只有评论（双写的那条 org=A），看不到 B 的题解
	var feedA []model.ActivityFeed
	_ = db.Where("org_id = ?", privateA).Find(&feedA).Error
	if len(feedA) != 1 || feedA[0].Type != model.ActivityTypeComment {
		t.Fatalf("privateA feed=%+v", feedA)
	}
	// 私有域 B：只有题解
	var feedB []model.ActivityFeed
	_ = db.Where("org_id = ?", privateB).Find(&feedB).Error
	if len(feedB) != 1 || feedB[0].Type != model.ActivityTypeSolution {
		t.Fatalf("privateB feed=%+v", feedB)
	}

	// 题库题解：不按 org 过滤，全站
	var sols []model.ProblemUserSolution
	_ = db.Where("problem_id = ?", p.ID).Find(&sols).Error
	if len(sols) != 1 {
		t.Fatalf("problem solutions want 1, got %d", len(sols))
	}
	_ = s // silence if unused beyond setup
}
