package model

import "testing"

func TestIsAcceptedStatus(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		{"AC", true},
		{" ac ", true},
		{"OK", true},
		{"Accepted", true},
		{"ACCEPTED", true},
		{"正确", true},
		{"答案正确", true},
		{"WA", false},
		{"Wrong Answer", false},
		{"", false},
	}
	for _, c := range cases {
		if got := IsAcceptedStatus(c.in); got != c.want {
			t.Errorf("IsAcceptedStatus(%q)=%v want %v", c.in, got, c.want)
		}
	}
}

func TestFillIsAC(t *testing.T) {
	logs := []SubmitLog{
		{Status: "AC"},
		{Status: "WA"},
		{Status: "  accepted "},
	}
	FillIsACBatch(logs)
	if !logs[0].IsAC || logs[1].IsAC || !logs[2].IsAC {
		t.Fatalf("FillIsACBatch unexpected: %+v", logs)
	}
}

func TestIsLeetCodeSyntheticSubmit(t *testing.T) {
	if IsLeetCodeSyntheticSubmit("LeetCode", "lc-prob-123") {
		t.Fatal("lc-prob should show in activity")
	}
	if !IsLeetCodeSyntheticSubmit("LeetCode", "lc-cal-1-20260101-0") {
		t.Fatal("lc-cal should be hidden")
	}
	if !IsLeetCodeSyntheticSubmit("LeetCode", "lc-pad-1-0") {
		t.Fatal("lc-pad should be hidden")
	}
	if !IsLeetCodeSyntheticSubmit("LeetCode", "lc-ac-1-0") {
		t.Fatal("lc-ac should be hidden")
	}
	if IsLeetCodeSyntheticSubmit("CodeForces", "123") {
		t.Fatal("non-LC never synthetic")
	}
}
