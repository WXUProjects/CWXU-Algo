package platform

import (
	"strings"
	"testing"
	"time"
)

func TestLeetCodeFetchLive(t *testing.T) {
	p := NewLeetCode{}
	full, err := p.FetchSubmitLog(999001, "sanenchen-o", true)
	if err != nil {
		t.Fatalf("full fetch: %v", err)
	}
	if len(full) == 0 {
		t.Fatal("full fetch empty")
	}

	cal, ac := 0, 0
	for _, l := range full {
		if strings.HasPrefix(l.SubmitID, "lc-cal-") {
			cal++
			if l.Status != "SUBMIT" {
				t.Fatalf("cal status want SUBMIT got %s", l.Status)
			}
		}
		if strings.HasPrefix(l.SubmitID, "lc-ac-") {
			ac++
			if l.Status != "AC" {
				t.Fatalf("ac status want AC got %s", l.Status)
			}
			// 全量初始化 AC 不应落在今天，避免污染今日做题数
			now := time.Now()
			if l.Time.Year() == now.Year() && l.Time.YearDay() == now.YearDay() {
				t.Fatalf("full AC should not be today: %v", l.Time)
			}
		}
	}
	if cal == 0 {
		t.Fatal("expected calendar submits")
	}
	if ac == 0 {
		t.Fatal("expected AC total")
	}
	t.Logf("full total=%d cal=%d ac=%d", len(full), cal, ac)

	incr, err := p.FetchSubmitLog(999001, "sanenchen-o", false)
	if err != nil {
		t.Fatalf("incr fetch: %v", err)
	}
	cal2, ac2 := 0, 0
	for _, l := range incr {
		if strings.HasPrefix(l.SubmitID, "lc-cal-") {
			cal2++
		}
		if strings.HasPrefix(l.SubmitID, "lc-ac-") {
			ac2++
		}
	}
	if ac2 != ac {
		t.Fatalf("incr ac count should equal full ac total for stable ids: full=%d incr=%d", ac, ac2)
	}
	if cal2 > cal {
		t.Fatalf("incr cal should be subset: full=%d incr=%d", cal, cal2)
	}
	t.Logf("incr total=%d cal=%d ac=%d", len(incr), cal2, ac2)
}
