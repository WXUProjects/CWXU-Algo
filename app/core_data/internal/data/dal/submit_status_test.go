package dal

import (
	"testing"

	"cwxu-algo/app/core_data/internal/data/model"
)

func TestShouldRewriteFinalStatus(t *testing.T) {
	if !shouldRewriteFinalStatus("WRONG_ANSWER", "WA") {
		t.Fatal("long→short should rewrite")
	}
	if shouldRewriteFinalStatus("WA", "OK") {
		t.Fatal("must not rewrite final→other final")
	}
	if shouldRewriteFinalStatus("OK", "OK") {
		t.Fatal("same status no rewrite")
	}
}

func TestIsPendingSubmitStatus(t *testing.T) {
	if !model.IsPendingSubmitStatus("") || !model.IsPendingSubmitStatus("TESTING") {
		t.Fatal("empty/TESTING pending")
	}
	if model.IsPendingSubmitStatus("OK") || model.IsPendingSubmitStatus("WA") {
		t.Fatal("final not pending")
	}
}
