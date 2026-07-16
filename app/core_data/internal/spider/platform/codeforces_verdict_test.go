package platform

import "testing"

func TestNormalizeCodeforcesVerdict(t *testing.T) {
	cases := map[string]string{
		"":                      "TESTING",
		"  ":                    "TESTING",
		"OK":                    "OK",
		"WRONG_ANSWER":          "WA",
		"TIME_LIMIT_EXCEEDED":   "TLE",
		"MEMORY_LIMIT_EXCEEDED": "MLE",
		"RUNTIME_ERROR":         "RE",
		"COMPILATION_ERROR":     "CE",
		"PARTIAL":               "PARTIAL",
		"SKIPPED":               "SKIPPED",
		"CHALLENGED":            "CHALLENGED",
		"TESTING":               "TESTING",
		"IDLENESS_LIMIT_EXCEEDED": "ILE",
	}
	for in, want := range cases {
		if got := NormalizeCodeforcesVerdict(in); got != want {
			t.Errorf("NormalizeCodeforcesVerdict(%q)=%q want %q", in, got, want)
		}
	}
}

