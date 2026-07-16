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
