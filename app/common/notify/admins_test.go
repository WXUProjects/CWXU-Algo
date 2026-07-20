package notify

import "testing"

func TestLookupUserEmailEmpty(t *testing.T) {
	if LookupUserEmail(nil, 1) != "" {
		t.Fatal("nil db")
	}
	if LookupUserEmail(nil, 0) != "" {
		t.Fatal("zero user")
	}
	if EmailUser(nil, 1, "s", "<p>x</p>") {
		t.Fatal("nil db EmailUser")
	}
}

func TestParseEmailList(t *testing.T) {
	raw := "a@x.com, b@y.com; c@z.com\nA@x.com  invalid  d@ok.com"
	got := ParseEmailList(raw)
	want := []string{"a@x.com", "b@y.com", "c@z.com", "d@ok.com"}
	if len(got) != len(want) {
		t.Fatalf("len=%d want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("i=%d got %q want %q", i, got[i], want[i])
		}
	}
	if ParseEmailList("") != nil && len(ParseEmailList("")) != 0 {
		t.Fatal("empty should be empty")
	}
}
