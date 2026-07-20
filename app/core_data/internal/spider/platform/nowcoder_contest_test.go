package platform

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"cwxu-algo/app/core_data/internal/spider"
)

func TestParseNowCoderProfileUID(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"156260548", "156260548"},
		{"https://ac.nowcoder.com/acm/contest/profile/156260548", "156260548"},
		{"https://ac.nowcoder.com/acm/contest/profile/156260548?from=xx", "156260548"},
		{"http://ac.nowcoder.com/acm/contest/profile/156260548/", "156260548"},
		{"", ""},
		{"not-a-uid", ""},
	}
	for _, c := range cases {
		if g := ParseNowCoderProfileUID(c.in); g != c.want {
			t.Errorf("ParseNowCoderProfileUID(%q)=%q want %q", c.in, g, c.want)
		}
	}
}

func TestParseNowCoderContestLogsFromHistory_Fixture156260548(t *testing.T) {
	// 真实抓取：https://ac.nowcoder.com/acm/contest/profile/156260548 参赛历史第 1 页
	path := filepath.Join("testdata", "nowcoder_contest_history_156260548_p1.json")
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	logs, page, err := ParseNowCoderContestLogsFromHistory(42, body)
	if err != nil {
		t.Fatal(err)
	}
	if page == nil || page.TotalCount < 5 {
		t.Fatalf("pageInfo=%+v", page)
	}
	if len(logs) < 5 {
		t.Fatalf("logs=%d", len(logs))
	}

	loc := time.FixedZone("CST", 8*3600)
	byID := map[string]time.Duration{}
	for _, l := range logs {
		if l.Platform != spider.NowCoder || l.UserID != 42 {
			t.Fatalf("bad meta %+v", l)
		}
		if l.Time.IsZero() || !l.EndTime.After(l.Time) {
			t.Fatalf("missing real window contest=%s start=%v end=%v", l.ContestId, l.Time, l.EndTime)
		}
		byID[l.ContestId] = l.EndTime.Sub(l.Time)
		t.Logf("id=%s name=%s %s–%s dur=%v ac=%d/%d rank=%d",
			l.ContestId, l.ContestName,
			l.Time.In(loc).Format("01-02 15:04"),
			l.EndTime.In(loc).Format("15:04"),
			l.EndTime.Sub(l.Time), l.AcCount, l.TotalCount, l.Rank)
	}

	// 关键：赛长不固定，必须识别出 4h / 5h / 2.5h / 2h
	want := map[string]time.Duration{
		"137658": 4 * time.Hour,                    // 河南萌新 13:00–17:00
		"137478": 5 * time.Hour,                    // 中南民大重现 13:00–18:00
		"132781": 2*time.Hour + 30*time.Minute,     // 华德 17:30–20:00
		"131416": 2 * time.Hour,                    // 小白月赛
		"129231": 2 * time.Hour,                    // 周赛
	}
	for id, dur := range want {
		got, ok := byID[id]
		if !ok {
			t.Errorf("missing contest %s", id)
			continue
		}
		if got != dur {
			t.Errorf("contest %s dur=%v want %v", id, got, dur)
		}
	}

	all3h := true
	for _, g := range byID {
		if g != 3*time.Hour {
			all3h = false
			break
		}
	}
	if all3h {
		t.Fatal("all contests collapsed to 3h — real durations not recognized")
	}
}

func TestNowcoderContestWindowFromItem_DurationFallback(t *testing.T) {
	item := ContestHistoryItem{
		StartTime:       json.Number("1784523600000"),
		EndTime:         json.Number("0"),
		ContestDuration: json.Number("14400000"), // 4h ms
	}
	s, e, ok := nowcoderContestWindowFromItem(item)
	if !ok || e-s != 4*3600 {
		t.Fatalf("start=%d end=%d ok=%v", s, e, ok)
	}
}

func TestFetchContestLog_Live156260548(t *testing.T) {
	if testing.Short() {
		t.Skip("live")
	}
	// 主页：https://ac.nowcoder.com/acm/contest/profile/156260548
	logs, err := NewNowCoder{}.FetchContestLog(1, "https://ac.nowcoder.com/acm/contest/profile/156260548", false)
	if err != nil {
		t.Fatalf("FetchContestLog: %v", err)
	}
	if len(logs) == 0 {
		t.Fatal("empty contest history")
	}
	var saw4h, sawNon3h bool
	durs := map[time.Duration]int{}
	for _, l := range logs {
		if !l.EndTime.After(l.Time) {
			t.Errorf("contest %s missing EndTime (must use API real duration)", l.ContestId)
			continue
		}
		d := l.EndTime.Sub(l.Time)
		durs[d]++
		if d == 4*time.Hour {
			saw4h = true
		}
		if d != 3*time.Hour {
			sawNon3h = true
		}
		t.Logf("%s %s %s–%s dur=%v",
			l.ContestId, l.ContestName,
			l.Time.In(time.FixedZone("CST", 8*3600)).Format("2006-01-02 15:04"),
			l.EndTime.In(time.FixedZone("CST", 8*3600)).Format("15:04"),
			d)
	}
	if !sawNon3h {
		t.Fatal("all durations 3h — API endTime/contestDuration not applied")
	}
	// 该用户近期有河南萌新 4h
	if !saw4h {
		t.Log("warn: no 4h contest on first page (history order may change)")
	}
	t.Logf("duration histogram: %v", durs)
}

// 回归：重构后参赛历史解析字段与赛长不变。
func TestContestHistoryItemToLog_MutationGuard(t *testing.T) {
	item := ContestHistoryItem{
		ContestId:       json.Number("137658"),
		ContestName:     "河南萌新联赛",
		Rank:            65,
		TotalCount:      13,
		AcCount:         10,
		StartTime:       json.Number("1784523600000"),
		EndTime:         json.Number("1784538000000"),
		ContestDuration: json.Number("14400000"),
	}
	log, ok := ContestHistoryItemToLog(99, item)
	if !ok {
		t.Fatal("expected ok")
	}
	if log.ContestId != "137658" || log.UserID != 99 || log.Platform != spider.NowCoder {
		t.Fatalf("meta %+v", log)
	}
	if log.ContestName != "河南萌新联赛" || log.Rank != 65 || log.AcCount != 10 || log.TotalCount != 13 {
		t.Fatalf("fields %+v", log)
	}
	if log.EndTime.Sub(log.Time) != 4*time.Hour {
		t.Fatalf("dur=%v", log.EndTime.Sub(log.Time))
	}
	if !strings.HasSuffix(log.ContestUrl, "/137658") {
		t.Fatalf("url=%s", log.ContestUrl)
	}
}
