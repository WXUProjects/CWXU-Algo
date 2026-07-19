package service

import (
	"testing"
	"time"

	"cwxu-algo/app/core_data/internal/data/dal"
)

func TestClampHeatmapRangePersonal(t *testing.T) {
	start, end, err := clampHeatmapRange("20100101", "20260719", heatmapMaxDaysPersonal)
	if err != nil {
		t.Fatal(err)
	}
	if start != "2010-01-01" || end != "2026-07-19" {
		t.Fatalf("want full range, got %s..%s", start, end)
	}
}

func TestClampHeatmapRangeAggregate(t *testing.T) {
	start, end, err := clampHeatmapRange("20100101", "20260719", heatmapMaxDaysAggregate)
	if err != nil {
		t.Fatal(err)
	}
	// ~400 days before end
	if end != "2026-07-19" {
		t.Fatalf("end %s", end)
	}
	if start >= "2025-06-01" && start <= "2025-07-01" {
		// ok around 400d
	} else {
		t.Fatalf("aggregate start unexpected: %s", start)
	}
}

func TestFilterDailyCountsInRange(t *testing.T) {
	rows := []dal.DailyCount{
		{Day: time.Date(2023, 1, 1, 0, 0, 0, 0, time.Local), Cnt: 1},
		{Day: time.Date(2024, 6, 1, 0, 0, 0, 0, time.Local), Cnt: 2},
		{Day: time.Date(2025, 12, 31, 0, 0, 0, 0, time.Local), Cnt: 3},
	}
	got := filterDailyCountsInRange(rows, "2024-01-01", "2025-01-01")
	if len(got) != 1 || got[0].Cnt != 2 {
		t.Fatalf("got %+v", got)
	}
	// 全量
	if n := len(filterDailyCountsInRange(rows, "2020-01-01", "2030-01-01")); n != 3 {
		t.Fatalf("full filter n=%d", n)
	}
}

func TestPersonalHeatmapCareerRangeWidth(t *testing.T) {
	start, end := personalHeatmapCareerRange()
	st, err1 := time.ParseInLocation("2006-01-02", start, time.Local)
	en, err2 := time.ParseInLocation("2006-01-02", end, time.Local)
	if err1 != nil || err2 != nil {
		t.Fatal(err1, err2)
	}
	days := int(en.Sub(st).Hours() / 24)
	if days < heatmapMaxDaysPersonal-1 || days > heatmapMaxDaysPersonal+1 {
		t.Fatalf("career width days=%d want ~%d", days, heatmapMaxDaysPersonal)
	}
}
