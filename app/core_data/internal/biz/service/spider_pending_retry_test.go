package service

import (
	"testing"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"
)

func TestCountRecentPendingSubmits(t *testing.T) {
	now := time.Now()
	logs := []model.SubmitLog{
		{Status: "Judging", Time: now.Add(-1 * time.Minute)},
		{Status: "TESTING", Time: now.Add(-10 * time.Minute)},
		{Status: "OK", Time: now.Add(-1 * time.Minute)},
		{Status: "WA", Time: now.Add(-2 * time.Minute)},
		// 过旧 pending 不计
		{Status: "Judging", Time: now.Add(-48 * time.Hour)},
		// 无时间戳的 pending 保守计入
		{Status: "正在评测"},
	}
	n := countRecentPendingSubmits(logs, 24*time.Hour)
	if n != 3 {
		t.Fatalf("want 3 recent pending, got %d", n)
	}
	if countRecentPendingSubmits(logs, 0) != 0 {
		t.Fatal("maxAge<=0 should yield 0")
	}
	if countRecentPendingSubmits(nil, 24*time.Hour) != 0 {
		t.Fatal("nil logs")
	}
}

func TestCountRecentPendingSubmits_AllFinal(t *testing.T) {
	logs := []model.SubmitLog{
		{Status: "OK", Time: time.Now()},
		{Status: "答案正确", Time: time.Now()},
	}
	if countRecentPendingSubmits(logs, 24*time.Hour) != 0 {
		t.Fatal("no pending expected")
	}
}
