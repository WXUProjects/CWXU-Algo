package service

import (
	"testing"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func testInferDB(t *testing.T) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:contest_infer?mode=memory&cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	if err := db.AutoMigrate(
		&model.SubmitLog{},
		&model.ContestLog{},
		&model.ContestProblem{},
		&model.ContestUserProblem{},
		&model.ContestCalendar{},
	); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

func TestResolveContestWindow_DefaultDuration(t *testing.T) {
	db := testInferDB(t)
	startHint := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	start, end := ResolveContestWindow(db, spider.LeetCode, "weekly-contest-400", startHint)
	if !start.Equal(startHint) {
		t.Fatalf("start=%v want %v", start, startHint)
	}
	// 90min + 15min buffer
	wantEnd := startHint.Add(90*time.Minute + contestInferEndBuffer)
	if !end.Equal(wantEnd) {
		t.Fatalf("end=%v want %v", end, wantEnd)
	}
}

func TestResolveContestWindow_FromCalendar(t *testing.T) {
	db := testInferDB(t)
	_ = db.Create(&model.ContestCalendar{
		Platform:   spider.NowCoder,
		ExternalID: "12345",
		Name:       "test",
		URL:        "https://ac.nowcoder.com/acm/contest/12345",
		StartTime:  1700000000,
		EndTime:    1700007200,
		Source:     "cpolar",
	}).Error
	start, end := ResolveContestWindow(db, spider.NowCoder, "12345", time.Time{})
	if start.Unix() != 1700000000 {
		t.Fatalf("start unix=%d", start.Unix())
	}
	// end + buffer
	if end.Unix() != 1700007200+int64(contestInferEndBuffer.Seconds()) {
		t.Fatalf("end unix=%d", end.Unix())
	}
}

func TestInferContestUserProblems_NowCoderByProblemSetAndWindow(t *testing.T) {
	db := testInferDB(t)
	start := time.Date(2026, 6, 1, 10, 0, 0, 0, time.UTC)
	// 题目目录
	_ = db.Create(&model.ContestProblem{
		Platform: spider.NowCoder, ContestID: "999", Label: "A", ExternalID: "316899", Title: "A题",
	}).Error
	_ = db.Create(&model.ContestProblem{
		Platform: spider.NowCoder, ContestID: "999", Label: "B", ExternalID: "316900", Title: "B题",
	}).Error
	// 窗内：A WA + AC，B 仅 WA；窗外同题不计入
	logs := []model.SubmitLog{
		{Platform: spider.NowCoder, UserID: 1, SubmitID: "s1", Problem: "316899 题A", Status: "答案错误", Time: start.Add(10 * time.Minute)},
		{Platform: spider.NowCoder, UserID: 1, SubmitID: "s2", Problem: "316899 题A", Status: "答案正确", Time: start.Add(20 * time.Minute)},
		{Platform: spider.NowCoder, UserID: 1, SubmitID: "s3", Problem: "316900 题B", Status: "答案错误", Time: start.Add(30 * time.Minute)},
		// 窗外
		{Platform: spider.NowCoder, UserID: 1, SubmitID: "s4", Problem: "316899 题A", Status: "答案正确", Time: start.Add(5 * time.Hour)},
		// 他场题号
		{Platform: spider.NowCoder, UserID: 1, SubmitID: "s5", Problem: "111111 其他", Status: "答案正确", Time: start.Add(15 * time.Minute)},
	}
	for i := range logs {
		logs[i].FillIsAC()
	}
	if err := db.Create(&logs).Error; err != nil {
		t.Fatal(err)
	}

	n, err := InferContestUserProblems(db, spider.NowCoder, "999", []int64{1}, start)
	if err != nil {
		t.Fatal(err)
	}
	if n < 2 {
		t.Fatalf("upsert n=%d want >=2", n)
	}
	var cells []model.ContestUserProblem
	_ = db.Where("platform = ? AND contest_id = ? AND user_id = ?", spider.NowCoder, "999", 1).Find(&cells).Error
	byExt := map[string]model.ContestUserProblem{}
	for _, c := range cells {
		byExt[c.ExternalID] = c
	}
	a := byExt["316899"]
	if a.Status != model.ContestCellAC || a.Attempts != 1 {
		t.Fatalf("A cell=%+v want AC attempts=1", a)
	}
	if a.RelativeSec == nil || *a.RelativeSec != 20*60 {
		t.Fatalf("A relativeSec=%v want 1200", a.RelativeSec)
	}
	b := byExt["316900"]
	if b.Status != model.ContestCellTried || b.Attempts != 1 {
		t.Fatalf("B cell=%+v want TRIED attempts=1", b)
	}
	if _, ok := byExt["111111"]; ok {
		t.Fatal("other problem should not be inferred")
	}
}

func TestInferContestUserProblems_LeetCodeLcProbInWindow(t *testing.T) {
	db := testInferDB(t)
	start := time.Date(2026, 3, 1, 2, 30, 0, 0, time.UTC)
	_ = db.Create(&model.ContestProblem{
		Platform: spider.LeetCode, ContestID: "weekly-contest-400", Label: "A",
		ExternalID: "minimum-number-of-chairs-in-a-waiting-room", Title: "椅子",
	}).Error
	// lc-prob 在窗内；lc-cal 应忽略
	_ = db.Create(&model.SubmitLog{
		Platform: spider.LeetCode, UserID: 7, SubmitID: "lc-prob-100",
		Problem: "minimum-number-of-chairs-in-a-waiting-room 椅子", ExternalID: "minimum-number-of-chairs-in-a-waiting-room",
		Status: "AC", Time: start.Add(12 * time.Minute),
	}).Error
	_ = db.Create(&model.SubmitLog{
		Platform: spider.LeetCode, UserID: 7, SubmitID: "lc-cal-7-20260301-0",
		Problem: "leetcode-submit", Status: "SUBMIT", Time: start.Add(5 * time.Minute),
	}).Error

	n, err := InferContestUserProblems(db, spider.LeetCode, "weekly-contest-400", []int64{7}, start)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("n=%d want 1", n)
	}
	var cell model.ContestUserProblem
	if err := db.Where("user_id = 7").First(&cell).Error; err != nil {
		t.Fatal(err)
	}
	if cell.Status != model.ContestCellAC || cell.ExternalID != "minimum-number-of-chairs-in-a-waiting-room" {
		t.Fatalf("cell=%+v", cell)
	}
}

func TestInferContestUserProblems_NoProblemSetRequiresContestField(t *testing.T) {
	db := testInferDB(t)
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// 无 contest_problems：仅 contest 字段可归属
	_ = db.Create(&model.SubmitLog{
		Platform: spider.CodeForces, UserID: 2, SubmitID: "1",
		Contest: "2000", Problem: "A-Hello", Status: "OK", Time: start.Add(time.Minute),
	}).Error
	_ = db.Create(&model.SubmitLog{
		Platform: spider.CodeForces, UserID: 2, SubmitID: "2",
		Contest: "", Problem: "A-Hello", Status: "OK", Time: start.Add(2 * time.Minute),
	}).Error

	n, err := InferContestUserProblems(db, spider.CodeForces, "2000", []int64{2}, start)
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Fatalf("n=%d want 1 (only contest-tagged)", n)
	}
}
