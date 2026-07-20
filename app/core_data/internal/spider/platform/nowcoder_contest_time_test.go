package platform

import (
	"strings"
	"testing"
	"time"
)

// 精简自 ac.nowcoder.com/acm/contest/137658 页内嵌 JSON（河南萌新联赛 13:00–17:00）
const sampleNowCoderContestHTML = `
<html><head><title>河南萌新联赛2026第（一）场：河南工业大学_ACM/NOI_牛客竞赛OJ</title></head>
<body>
<script>
var pageData = {"isManager":false,"startTime":1784523600000,"signUpStartTime":1784073600000,
"name":"河南萌新联赛2026第（一）场：河南工业大学",
"isFinished":true,"endTime":1784538000000,"signUpId":0};
</script>
</body></html>
`

func TestParseNowCoderContestTimesHTML_FourHour(t *testing.T) {
	start, end, name, err := ParseNowCoderContestTimesHTML(sampleNowCoderContestHTML)
	if err != nil {
		t.Fatal(err)
	}
	loc := time.FixedZone("CST", 8*3600)
	wantStart := time.Date(2026, 7, 20, 13, 0, 0, 0, loc).Unix()
	wantEnd := time.Date(2026, 7, 20, 17, 0, 0, 0, loc).Unix()
	if start != wantStart {
		t.Fatalf("start=%d want %d (%v)", start, wantStart, time.Unix(start, 0).In(loc))
	}
	if end != wantEnd {
		t.Fatalf("end=%d want %d (%v)", end, wantEnd, time.Unix(end, 0).In(loc))
	}
	if end-start != 4*3600 {
		t.Fatalf("duration=%dh want 4h", (end-start)/3600)
	}
	if !strings.Contains(name, "河南萌新") {
		t.Fatalf("name=%q", name)
	}
}

func TestParseNowCoderContestTimesHTML_Seconds(t *testing.T) {
	html := `{"startTime":1700000000,"endTime":1700014400,"name":"sec-test"}`
	start, end, name, err := ParseNowCoderContestTimesHTML(html)
	if err != nil {
		t.Fatal(err)
	}
	if start != 1700000000 || end != 1700014400 {
		t.Fatalf("start=%d end=%d", start, end)
	}
	if end-start != 4*3600 {
		t.Fatalf("dur=%d", end-start)
	}
	if name != "sec-test" {
		t.Fatalf("name=%q", name)
	}
}

func TestNowcoderMsToUnixSec(t *testing.T) {
	if g := nowcoderMsToUnixSec(1784523600000); g != 1784523600 {
		t.Fatalf("ms got %d", g)
	}
	if g := nowcoderMsToUnixSec(1784523600); g != 1784523600 {
		t.Fatalf("sec got %d", g)
	}
	if g := nowcoderMsToUnixSec(0); g != 0 {
		t.Fatalf("zero got %d", g)
	}
}

func TestParseNowCoderContestTimesHTML_Missing(t *testing.T) {
	if _, _, _, err := ParseNowCoderContestTimesHTML(`<html>no times</html>`); err == nil {
		t.Fatal("expected error")
	}
}

// 爬多少用多少：>12h 仍应解析成功。
func TestParseNowCoderContestTimesHTML_Long24h(t *testing.T) {
	html := `{"startTime":1700000000000,"endTime":1700086400000,"name":"long-24h"}`
	start, end, name, err := ParseNowCoderContestTimesHTML(html)
	if err != nil {
		t.Fatal(err)
	}
	if end-start != 24*3600 {
		t.Fatalf("dur=%ds want 24h", end-start)
	}
	if name != "long-24h" {
		t.Fatalf("name=%q", name)
	}
}

func TestFetchNowCoderContestTimes_Live137658(t *testing.T) {
	if testing.Short() {
		t.Skip("live")
	}
	start, end, name, err := FetchNowCoderContestTimes("137658")
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	loc := time.FixedZone("CST", 8*3600)
	t.Logf("name=%q start=%v end=%v dur=%v", name, time.Unix(start, 0).In(loc), time.Unix(end, 0).In(loc), time.Duration(end-start)*time.Second)
	if end-start != 4*3600 {
		t.Fatalf("duration=%ds want 4h", end-start)
	}
	wantStart := time.Date(2026, 7, 20, 13, 0, 0, 0, loc).Unix()
	wantEnd := time.Date(2026, 7, 20, 17, 0, 0, 0, loc).Unix()
	if start != wantStart || end != wantEnd {
		t.Fatalf("got %d–%d want %d–%d", start, end, wantStart, wantEnd)
	}
}
