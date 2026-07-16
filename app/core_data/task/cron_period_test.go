package task

import (
	"testing"
	"time"
)

func TestIntervalPeriodStartAlignsToWallClock(t *testing.T) {
	loc := cronTZ()

	// 10:07 → 60min 周期起点应为 10:00
	now := time.Date(2026, 7, 16, 10, 7, 30, 0, loc)
	got := intervalPeriodStart(now, 60)
	want := time.Date(2026, 7, 16, 10, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("60min: got %v want %v", got, want)
	}

	// 10:07 → 5min 周期起点 10:05
	got = intervalPeriodStart(now, 5)
	want = time.Date(2026, 7, 16, 10, 5, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("5min: got %v want %v", got, want)
	}

	// 10:07 → 180min 周期：00:00 / 03:00 / 06:00 / 09:00 / 12:00… → 09:00
	got = intervalPeriodStart(now, 180)
	want = time.Date(2026, 7, 16, 9, 0, 0, 0, loc)
	if !got.Equal(want) {
		t.Fatalf("180min: got %v want %v", got, want)
	}

	// 整点本身落在槽起点
	exact := time.Date(2026, 7, 16, 11, 0, 0, 0, loc)
	got = intervalPeriodStart(exact, 60)
	if !got.Equal(exact) {
		t.Fatalf("exact hour: got %v want %v", got, exact)
	}
}

func TestIntervalPeriodStartIndependentOfProcessStart(t *testing.T) {
	loc := cronTZ()
	// 同一 60min 槽内任意时刻，周期起点相同（与“启动于 10:07”无关）
	a := intervalPeriodStart(time.Date(2026, 7, 16, 10, 1, 0, 0, loc), 60)
	b := intervalPeriodStart(time.Date(2026, 7, 16, 10, 59, 0, 0, loc), 60)
	if !a.Equal(b) {
		t.Fatalf("same slot must share period start: %v vs %v", a, b)
	}
	next := intervalPeriodStart(time.Date(2026, 7, 16, 11, 0, 0, 0, loc), 60)
	if !next.After(a) {
		t.Fatalf("next hour must advance period: %v then %v", a, next)
	}
}
