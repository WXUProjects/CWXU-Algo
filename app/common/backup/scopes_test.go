package backup

import (
	"os"
	"path/filepath"
	"testing"
)

func TestNormalizeScopes(t *testing.T) {
	all, err := NormalizeScopes(nil)
	if err != nil || len(all) != 1 || all[0] != ScopeAll {
		t.Fatalf("empty → all: %v %v", all, err)
	}
	s, err := NormalizeScopes([]string{"users", "USERS", "problems"})
	if err != nil {
		t.Fatal(err)
	}
	if len(s) != 2 {
		t.Fatalf("dedupe: %v", s)
	}
	if _, err := NormalizeScopes([]string{"nope"}); err == nil {
		t.Fatal("expected error")
	}
}

func TestExpandedAndNeedsCore(t *testing.T) {
	ex := ExpandedScopes([]string{ScopeAll})
	if !HasScope(ex, ScopeSubmits) || !HasScope(ex, ScopeFiles) {
		t.Fatalf("all should expand: %v", ex)
	}
	if !NeedsCoreDB(ExpandedScopes([]string{ScopeProblems})) {
		t.Fatal("problems needs core")
	}
	if NeedsCoreDB(ExpandedScopes([]string{ScopeUsers})) {
		t.Fatal("users does not need core")
	}
}

func TestZipRoundTrip(t *testing.T) {
	dir := t.TempDir()
	src := filepath.Join(dir, "src")
	_ = os.MkdirAll(filepath.Join(src, "data"), 0o755)
	if err := os.WriteFile(filepath.Join(src, "manifest.json"), []byte(`{"version":"goalgo-backup-v1"}`), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "data", "users.ndjson"), []byte("{\"id\":1}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	zipPath := filepath.Join(dir, "b.zip")
	if err := ZipDir(src, zipPath); err != nil {
		t.Fatal(err)
	}
	out := filepath.Join(dir, "out")
	if err := UnzipTo(zipPath, out); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(out, "manifest.json"))
	if err != nil || len(raw) == 0 {
		t.Fatalf("manifest missing: %v", err)
	}
}
