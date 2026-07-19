package dal

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// GORM Raw 会把 SQL 字符串里的 '?' 也当成绑定占位符，曾导致 ListUserPlatformAC 恒失败、platforms 空。
func TestListUserPlatformACSourceHasNoQuestionMarkLiteral(t *testing.T) {
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("runtime.Caller failed")
	}
	srcPath := filepath.Join(filepath.Dir(file), "user_ac.go")
	b, err := os.ReadFile(srcPath)
	if err != nil {
		t.Fatal(err)
	}
	src := string(b)
	// 截取 ListUserPlatformAC 函数体
	start := strings.Index(src, "func ListUserPlatformAC")
	if start < 0 {
		t.Fatal("ListUserPlatformAC not found")
	}
	rest := src[start:]
	end := strings.Index(rest, "\nfunc ")
	if end < 0 {
		end = len(rest)
	}
	body := rest[:end]
	if strings.Contains(body, `''?'`) || strings.Contains(body, `', '?')`) || strings.Contains(body, `''?'`) {
		t.Fatal("ListUserPlatformAC must not use SQL string literal '?' (GORM binds it)")
	}
	if strings.Contains(body, "', '?')") || strings.Contains(body, `", '?')`) {
		t.Fatal("ListUserPlatformAC must not use SQL string literal '?' (GORM binds it)")
	}
	// 明确禁止 COALESCE 空平台回落为 '?'
	if strings.Contains(body, "'?'") {
		t.Fatalf("ListUserPlatformAC still contains '?' string literal:\n%s", body)
	}
	if !strings.Contains(body, "unknown") {
		t.Fatal("expected empty-platform label 'unknown'")
	}
	// 牛客统一 NowCoder，不再拆 Tracker/竞赛站
	if strings.Contains(body, "PlatformACNowCoderTracker") || strings.Contains(body, "PlatformACNowCoderContest") {
		t.Fatal("NowCoder pie must not split tracker/contest labels")
	}
	if strings.Contains(body, "牛客Tracker") || strings.Contains(body, "牛客竞赛站") {
		t.Fatal("NowCoder pie must not use split Chinese labels")
	}
	if !strings.Contains(body, "firstErr") {
		t.Fatal("expected resilient multi-query path (firstErr)")
	}
}
