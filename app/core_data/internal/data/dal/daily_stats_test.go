package dal

import (
	"testing"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"
)

func TestAggregateSubmitDeltas(t *testing.T) {
	day := time.Date(2026, 7, 16, 10, 0, 0, 0, time.Local)
	logs := []model.SubmitLog{
		{UserID: 1, Time: day, Platform: "CF", SubmitID: "a", IsAC: true},
		{UserID: 1, Time: day, Platform: "CF", SubmitID: "b", IsAC: false},
		{UserID: 1, Time: day, Platform: "LeetCode", SubmitID: "lc-ac-1", IsAC: true},
		{UserID: 1, Time: day, Platform: "LeetCode", SubmitID: "lc-prob-99", IsAC: true},
		{UserID: 1, Time: day, Platform: "LeetCode", SubmitID: "lc-cal-1-20260716-0", IsAC: false},
	}
	d := AggregateSubmitDeltas(logs)
	if len(d) != 1 {
		t.Fatalf("want 1 delta got %d", len(d))
	}
	if d[0].SubmitCnt != 3 { // CF×2 + lc-cal；排除 lc-ac / lc-prob
		t.Fatalf("submit_cnt=%d want 3", d[0].SubmitCnt)
	}
	if d[0].AcCnt != 3 { // CF AC + lc-ac + lc-prob
		t.Fatalf("ac_cnt=%d want 3", d[0].AcCnt)
	}
}

func TestDedupeSubmitLogsBySubmitID(t *testing.T) {
	logs := []model.SubmitLog{
		{SubmitID: "a", Problem: "1"},
		{SubmitID: "a", Problem: "2"},
		{SubmitID: "b", Problem: "3"},
		{SubmitID: "", Problem: "skip"},
	}
	out := dedupeSubmitLogsBySubmitID(logs)
	if len(out) != 2 {
		t.Fatalf("len=%d want 2", len(out))
	}
	if out[0].Problem != "1" || out[1].SubmitID != "b" {
		t.Fatalf("unexpected %+v", out)
	}
}
