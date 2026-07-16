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

	cal, pad, ac, prob := 0, 0, 0, 0
	probSlugs := map[string]struct{}{}
	for _, l := range full {
		switch {
		case strings.HasPrefix(l.SubmitID, "lc-cal-"):
			cal++
			if l.Status != "SUBMIT" {
				t.Fatalf("cal status want SUBMIT got %s", l.Status)
			}
		case strings.HasPrefix(l.SubmitID, "lc-pad-"):
			pad++
			if l.Status != "SUBMIT" {
				t.Fatalf("pad status want SUBMIT got %s", l.Status)
			}
		case strings.HasPrefix(l.SubmitID, "lc-ac-"):
			ac++
			if l.Status != "AC" {
				t.Fatalf("ac status want AC got %s", l.Status)
			}
			// 全量初始化 AC 不应落在今天，避免污染今日做题数
			now := time.Now()
			if l.Time.Year() == now.Year() && l.Time.YearDay() == now.YearDay() {
				t.Fatalf("full AC should not be today: %v", l.Time)
			}
		case strings.HasPrefix(l.SubmitID, "lc-prob-"):
			prob++
			if l.Status != "AC" {
				t.Fatalf("prob status want AC got %s", l.Status)
			}
			if l.ExternalID == "" {
				t.Fatal("prob missing external_id (titleSlug)")
			}
			if !strings.HasPrefix(l.Problem, l.ExternalID) {
				t.Fatalf("prob problem should start with slug: %q", l.Problem)
			}
			probSlugs[l.ExternalID] = struct{}{}
		}
	}
	if cal == 0 {
		t.Fatal("expected calendar submits")
	}
	if ac == 0 && len(probSlugs) == 0 {
		t.Fatal("expected AC total or recent problems")
	}
	if prob == 0 {
		t.Fatal("expected recent AC (lc-prob-*) from public profile")
	}
	// 生涯提交：cal + pad 应接近 totalSubmissions（允许接口略有偏差）
	prog, err := fetchLeetCodeProgress("sanenchen-o")
	if err != nil {
		t.Fatalf("progress: %v", err)
	}
	if cal+pad < prog.TotalSubmissions-5 || cal+pad > prog.TotalSubmissions+5 {
		t.Fatalf("cal+pad=%d want ~totalSubmissions=%d", cal+pad, prog.TotalSubmissions)
	}
	// 合成 AC 条数 = acTotal（全量稳定 ID）
	if ac != prog.AcTotal {
		t.Fatalf("syn ac=%d want acTotal=%d", ac, prog.AcTotal)
	}
	t.Logf("full total=%d cal=%d pad=%d ac=%d prob=%d uniqueSlug=%d acTotal=%d totalSub=%d",
		len(full), cal, pad, ac, prob, len(probSlugs), prog.AcTotal, prog.TotalSubmissions)

	incr, err := p.FetchSubmitLog(999001, "sanenchen-o", false)
	if err != nil {
		t.Fatalf("incr fetch: %v", err)
	}
	cal2, pad2, ac2, prob2 := 0, 0, 0, 0
	for _, l := range incr {
		switch {
		case strings.HasPrefix(l.SubmitID, "lc-cal-"):
			cal2++
		case strings.HasPrefix(l.SubmitID, "lc-pad-"):
			pad2++
		case strings.HasPrefix(l.SubmitID, "lc-ac-"):
			ac2++
		case strings.HasPrefix(l.SubmitID, "lc-prob-"):
			prob2++
		}
	}
	if ac2 != ac {
		t.Fatalf("incr ac count should equal full ac for stable ids: full=%d incr=%d", ac, ac2)
	}
	if pad2 != pad {
		t.Fatalf("incr pad should equal full pad: full=%d incr=%d", pad, pad2)
	}
	if cal2 > cal {
		t.Fatalf("incr cal should be subset: full=%d incr=%d", cal, cal2)
	}
	if prob2 == 0 {
		t.Fatal("incr expected recent AC")
	}
	t.Logf("incr total=%d cal=%d pad=%d ac=%d prob=%d", len(incr), cal2, pad2, ac2, prob2)
}
