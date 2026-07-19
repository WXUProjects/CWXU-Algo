package model

import "testing"

func TestACProblemKey(t *testing.T) {
	pid := uint(42)
	if got := ACProblemKey("CF", "123", "A", &pid); got != "p:42" {
		t.Fatalf("got %q", got)
	}
	if got := ACProblemKey("CF", " 99 ", "A", nil); got != "e:CF:99" {
		t.Fatalf("got %q", got)
	}
	if got := ACProblemKey("AtCoder", "", "abc001_a", nil); got != "n:AtCoder:abc001_a" {
		t.Fatalf("got %q", got)
	}
}

func TestIsLeetCodeOfficialACKey(t *testing.T) {
	if !IsLeetCodeOfficialACKey("e:LeetCode:ac-0") {
		t.Fatal("ac-0 should be official")
	}
	if !IsLeetCodeOfficialACKey("e:LeetCode:ac-42") {
		t.Fatal("ac-42 should be official")
	}
	if IsLeetCodeOfficialACKey("e:LeetCode:two-sum") {
		t.Fatal("slug should not be official synthetic")
	}
	if IsLeetCodeOfficialACKey("e:CodeForces:123A") {
		t.Fatal("other platform")
	}
}
