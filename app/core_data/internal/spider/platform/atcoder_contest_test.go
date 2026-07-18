package platform

import (
	"cwxu-algo/app/core_data/internal/spider"
	"strings"
	"testing"
	"time"
)

// Compile-time: registered AtCoder provider satisfies SubmitContestFetcher.
var _ spider.SubmitContestFetcher = NewAtCoder{}

func TestNormalizeAtCoderContestID(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"agc004.contest.atcoder.jp", "agc004"},
		{"abc382.contest.atcoder.jp", "abc382"},
		{"abc382", "abc382"},
		{"  arc061.contest.atcoder.jp ", "arc061"},
		{"", ""},
	}
	for _, c := range cases {
		if got := normalizeAtCoderContestID(c.in); got != c.want {
			t.Errorf("normalizeAtCoderContestID(%q)=%q want %q", c.in, got, c.want)
		}
	}
}

func TestContestLogsFromAtCoderHistory_Fixture(t *testing.T) {
	raw := []byte(`[
		{
			"IsRated": true,
			"Place": 2,
			"OldRating": 0,
			"NewRating": 2720,
			"ContestScreenName": "agc004.contest.atcoder.jp",
			"ContestName": "AtCoder Grand Contest 004",
			"ContestNameEn": "",
			"EndTime": "2016-09-04T22:50:00+09:00"
		},
		{
			"IsRated": true,
			"Place": 1,
			"OldRating": 2720,
			"NewRating": 3000,
			"ContestScreenName": "abc100.contest.atcoder.jp",
			"ContestName": "AtCoder Beginner Contest 100",
			"ContestNameEn": "",
			"EndTime": "2018-06-16T22:40:00+09:00"
		}
	]`)
	hist, err := parseAtCoderHistoryJSON(raw)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	all := contestLogsFromAtCoderHistory(42, hist, true)
	if len(all) != 2 {
		t.Fatalf("needAll=true got %d logs", len(all))
	}
	first := all[0]
	if first.Platform != spider.AtCoder {
		t.Errorf("platform=%q", first.Platform)
	}
	if first.UserID != 42 {
		t.Errorf("userId=%d", first.UserID)
	}
	if first.ContestId != "agc004" {
		t.Errorf("contestId=%q", first.ContestId)
	}
	if first.ContestName != "AtCoder Grand Contest 004" {
		t.Errorf("name=%q", first.ContestName)
	}
	if first.ContestUrl != "https://atcoder.jp/contests/agc004" {
		t.Errorf("url=%q", first.ContestUrl)
	}
	if first.Rank != 2 {
		t.Errorf("rank=%d", first.Rank)
	}
	if first.Time.IsZero() {
		t.Error("time empty")
	}
	// EndTime +09:00 → check parsed wall clock
	wantYear, wantMonth, wantDay := 2016, time.September, 4
	y, m, d := first.Time.Date()
	if y != wantYear || m != wantMonth || d != wantDay {
		t.Errorf("time date=%v-%v-%v", y, m, d)
	}

	latestOnly := contestLogsFromAtCoderHistory(42, hist, false)
	if len(latestOnly) != 1 {
		t.Fatalf("needAll=false got %d", len(latestOnly))
	}
	if latestOnly[0].ContestId != "abc100" || latestOnly[0].Rank != 1 {
		t.Errorf("latest=%+v", latestOnly[0])
	}
}

func TestFetchContestLog_EmptyUsername(t *testing.T) {
	_, err := NewAtCoder{}.FetchContestLog(1, "", true)
	if err == nil {
		t.Fatal("expected error for empty username")
	}
	if !strings.Contains(err.Error(), "username") {
		t.Errorf("error should mention username: %v", err)
	}
}

func TestFetchContestLog_LiveTourist(t *testing.T) {
	logs, err := NewAtCoder{}.FetchContestLog(1, "tourist", true)
	if err != nil {
		t.Skipf("network/API: %v", err)
	}
	if len(logs) == 0 {
		t.Fatal("tourist should have non-empty contest history")
	}
	var ok int
	for _, l := range logs {
		if l.Platform != spider.AtCoder {
			t.Fatalf("platform=%q", l.Platform)
		}
		if l.ContestId == "" || l.ContestName == "" {
			t.Fatalf("empty id/name: %+v", l)
		}
		if !strings.HasPrefix(l.ContestUrl, "https://atcoder.jp/contests/") {
			t.Fatalf("bad url: %q", l.ContestUrl)
		}
		if strings.Contains(l.ContestId, ".contest.atcoder.jp") {
			t.Fatalf("contest id not normalized: %q", l.ContestId)
		}
		if l.Rank < 1 {
			t.Fatalf("rank should be >=1 for tourist: %+v", l)
		}
		if l.Time.IsZero() {
			t.Fatalf("time empty: %+v", l)
		}
		ok++
	}
	t.Logf("AtCoder tourist contests=%d first=%s rank=%d last=%s rank=%d",
		len(logs), logs[0].ContestId, logs[0].Rank, logs[len(logs)-1].ContestId, logs[len(logs)-1].Rank)
	if ok < 1 {
		t.Fatal("no valid contests")
	}

	// needAll=false → single latest
	one, err := NewAtCoder{}.FetchContestLog(1, "tourist", false)
	if err != nil {
		t.Skipf("network/API: %v", err)
	}
	if len(one) != 1 {
		t.Fatalf("needAll=false want 1 got %d", len(one))
	}
	if one[0].ContestId != logs[len(logs)-1].ContestId {
		t.Errorf("latest id=%q want %q", one[0].ContestId, logs[len(logs)-1].ContestId)
	}
}
