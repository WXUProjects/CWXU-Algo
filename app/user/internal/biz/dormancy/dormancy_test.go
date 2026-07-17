package dormancy

import (
	"testing"
	"time"
)

func TestClampInactiveDays(t *testing.T) {
	if ClampInactiveDays(0) != DefaultInactiveDays {
		t.Fatalf("default")
	}
	if ClampInactiveDays(14) != 14 {
		t.Fatalf("14")
	}
	if ClampInactiveDays(999) != MaxInactiveDays {
		t.Fatalf("max")
	}
}

func TestIsDormant(t *testing.T) {
	now := time.Date(2026, 7, 17, 12, 0, 0, 0, time.UTC)
	old := now.Add(-20 * 24 * time.Hour)
	recent := now.Add(-2 * 24 * time.Hour)

	if !IsDormant(&old, 14, ExemptFlags{}, now) {
		t.Fatal("old should dormant")
	}
	if IsDormant(&recent, 14, ExemptFlags{}, now) {
		t.Fatal("recent should active")
	}
	if IsDormant(&old, 14, ExemptFlags{IsSiteAdmin: true}, now) {
		t.Fatal("site admin exempt")
	}
	if IsDormant(&old, 14, ExemptFlags{ForceSync: true}, now) {
		t.Fatal("force sync exempt")
	}
	if IsDormant(&old, 14, ExemptFlags{PaidPlan: true}, now) {
		t.Fatal("paid plan exempt")
	}
	if !IsDormant(nil, 14, ExemptFlags{}, now) {
		t.Fatal("nil last login dormant")
	}
}

func TestIsPaidPlan(t *testing.T) {
	if !IsPaidPlan("team") || !IsPaidPlan("pro") || IsPaidPlan("free") {
		t.Fatal("plan")
	}
}
