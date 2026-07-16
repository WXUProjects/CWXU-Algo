package dal

import (
	"strings"
	"testing"
)

func TestRetentionShouldRebuildPreagg(t *testing.T) {
	if retentionShouldRebuildPreagg(0) {
		t.Fatal("cold=0 must NOT rebuild from hot-only submit_logs")
	}
	if !retentionShouldRebuildPreagg(1) {
		t.Fatal("cold>0 should rebuild once before prune")
	}
	if !retentionShouldRebuildPreagg(100) {
		t.Fatal("cold>0 should rebuild")
	}
}

func TestRetentionAllowMarkDoneWithoutRebuild(t *testing.T) {
	// 新库 / 刚 Purge：两边都空，允许 mark
	if err := retentionAllowMarkDoneWithoutRebuild(0, 0); err != nil {
		t.Fatalf("empty hot+ledger should allow: %v", err)
	}
	// 健康热表-only：ledger 含冷 id，可大于 hot
	if err := retentionAllowMarkDoneWithoutRebuild(100, 500); err != nil {
		t.Fatalf("ledger>=hot should allow: %v", err)
	}
	if err := retentionAllowMarkDoneWithoutRebuild(100, 100); err != nil {
		t.Fatalf("ledger==hot should allow: %v", err)
	}
	// 账本残缺：禁止 quiet 用热表重建
	err := retentionAllowMarkDoneWithoutRebuild(50, 0)
	if err == nil {
		t.Fatal("hot>0 ledger=0 must refuse")
	}
	if !strings.Contains(err.Error(), "PurgeSubmitsAndRecrawl") {
		t.Fatalf("error should point to purge path, got: %v", err)
	}
}
