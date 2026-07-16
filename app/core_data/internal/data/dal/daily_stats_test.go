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
	}
	d := AggregateSubmitDeltas(logs)
	if len(d) != 1 {
		t.Fatalf("want 1 delta got %d", len(d))
	}
	if d[0].SubmitCnt != 2 { // excludes lc-ac
		t.Fatalf("submit_cnt=%d want 2", d[0].SubmitCnt)
	}
	if d[0].AcCnt != 2 {
		t.Fatalf("ac_cnt=%d want 2", d[0].AcCnt)
	}
}
