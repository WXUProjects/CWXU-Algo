package problem_fetch

import "testing"

func TestNowCoderContestProblemURL(t *testing.T) {
	got := NowCoderContestProblemURL("137561", "A")
	want := "https://ac.nowcoder.com/acm/contest/137561/A"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if NowCoderContestProblemURL("", "A") != "" {
		t.Fatal("empty contest should yield empty url")
	}
}

func TestNowCoderBankProblemURL(t *testing.T) {
	got := NowCoderBankProblemURL("319811")
	want := "https://ac.nowcoder.com/acm/problem/319811"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
	if NowCoderBankProblemURL("abc") != "" {
		t.Fatal("non-digit id should be empty")
	}
}

func TestIsNowCoderContestURL(t *testing.T) {
	if !IsNowCoderContestURL("https://ac.nowcoder.com/acm/contest/137561/A") {
		t.Fatal("expected contest url")
	}
	if IsNowCoderContestURL("https://ac.nowcoder.com/acm/problem/319811") {
		t.Fatal("bank url is not contest")
	}
}

func TestExtractNowCoderPageInfoProblemID(t *testing.T) {
	html := `
<script type="text/javascript">
    window.pageInfo = {
        contestId: '137561',
        problemId: '319811',
        questionId: '11728760',
    };
</script>`
	if got := extractNowCoderPageInfoProblemID(html); got != "319811" {
		t.Fatalf("got %q", got)
	}
}

func TestCleanNowCoderTitleContest(t *testing.T) {
	got := cleanNowCoderTitle("A-小红的字符串处理_牛客周赛 Round 153")
	if got != "小红的字符串处理" {
		t.Fatalf("got %q", got)
	}
}

func TestNowcoderShouldTryNextURL(t *testing.T) {
	if !nowcoderShouldTryNextURL(fmtErr("NowCoder 题面暂无访问权限，请稍后重试")) {
		t.Fatal("permission should try next")
	}
	if !nowcoderShouldTryNextURL(fmtErr("NowCoder 未找到题面 DOM，请稍后重试")) {
		t.Fatal("dom miss should try next")
	}
}

// tiny helper to avoid importing fmt only for Errorf in test of error messages
type strErr string

func (e strErr) Error() string { return string(e) }
func fmtErr(s string) error    { return strErr(s) }
