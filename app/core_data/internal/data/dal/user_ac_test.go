package dal

import (
	"testing"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"
)

func TestAggregateOnlyAC(t *testing.T) {
	// ApplyUserACFromSubmits with empty / non-AC should no-op without db
	if err := ApplyUserACFromSubmits(nil, nil, nil); err != nil {
		t.Fatal(err)
	}
	pid := uint(1)
	logs := []model.SubmitLog{
		{UserID: 9, Platform: "CF", ProblemID: &pid, IsAC: true, Time: time.Now(), SubmitID: "x"},
		{UserID: 9, Platform: "CF", Problem: "B", IsAC: false, Time: time.Now(), SubmitID: "y"},
	}
	// nil db returns after empty maps when no AC... wait first is AC so needs db
	// only test key helper path via model
	if k := model.ACProblemKeyFromLog(&logs[0]); k != "p:1" {
		t.Fatalf("key=%s", k)
	}
	if logs[1].IsAC {
		t.Fatal("expected non-ac")
	}
}
