package platform

import "testing"

func TestNowcoderProblemLabel_PreferNumericOverUUID(t *testing.T) {
	// training API 同时给 id + uuid 时，必须落到数字 id（与 AC 站一致）
	got := nowcoderProblemLabel(284799, "376488f44cac4787923be95c3d0a535b", "ACM408", "小R排数字")
	want := "284799 小R排数字"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestNowcoderProblemLabel_UUIDWhenNoNumeric(t *testing.T) {
	got := nowcoderProblemLabel(0, "376488f44cac4787923be95c3d0a535b", "ACM408", "小R排数字")
	want := "376488f44cac4787923be95c3d0a535b 小R排数字"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestNowcoderProblemLabel_DigitsQuestionNumFallback(t *testing.T) {
	got := nowcoderProblemLabel(0, "", "309177", "某题")
	want := "309177 某题"
	if got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestNowcoderProblemLabel_SkipNonDigitsQuestionNum(t *testing.T) {
	// ACM413 不可当 external_id
	got := nowcoderProblemLabel(0, "", "ACM413", "某题")
	if got != "某题" {
		t.Fatalf("got %q want title-only", got)
	}
}
