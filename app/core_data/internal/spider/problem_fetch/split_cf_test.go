package problem_fetch

import "testing"

func TestSplitCF(t *testing.T) {
	cases := []struct {
		id      string
		contest string
		index   string
		gym     bool
	}{
		{"1791A", "1791", "A", false},
		{"1791A1", "1791", "A1", false},
		{"gym102861A", "102861", "A", true},
		{"gym102861A1", "102861", "A1", true},
		{"-102861A", "102861", "A", true},
		{"GYM102861B", "102861", "B", true},
	}
	for _, tc := range cases {
		c, idx, gym := splitCF(tc.id)
		if c != tc.contest || idx != tc.index || gym != tc.gym {
			t.Fatalf("%s → contest=%s index=%s gym=%v want %s %s %v",
				tc.id, c, idx, gym, tc.contest, tc.index, tc.gym)
		}
	}
}

func TestAtCoderURLFromExternalID(t *testing.T) {
	u := atCoderURLFromExternalID("abc123_a")
	want := "https://atcoder.jp/contests/abc123/tasks/abc123_a"
	if u != want {
		t.Fatalf("got %s want %s", u, want)
	}
}
