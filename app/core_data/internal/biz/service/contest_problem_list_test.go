package service

import (
	"testing"
)

func TestLabelSortKey(t *testing.T) {
	if labelSortKey("A") >= labelSortKey("B") {
		t.Fatal("A should < B")
	}
	if labelSortKey("Z") >= labelSortKey("1") {
		t.Fatal("letter before numeric")
	}
}

func TestAtCoderTaskLabel(t *testing.T) {
	if got := atCoderTaskLabel("abc123_a"); got != "A" {
		t.Fatalf("got %s", got)
	}
	if got := atCoderTaskLabel("abc123_f"); got != "F" {
		t.Fatalf("got %s", got)
	}
}

func TestEscapeJSON(t *testing.T) {
	if escapeJSON(`a"b`) != `a\"b` {
		t.Fatalf("escape failed: %q", escapeJSON(`a"b`))
	}
}
