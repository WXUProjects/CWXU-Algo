package notify

import (
	"testing"
)

func TestCreateNilDB(t *testing.T) {
	// 无 DB 时静默成功，避免 core 未配 user DSN 时崩溃
	if err := Create(nil, Row{UserID: 1, Title: "t"}); err != nil {
		t.Fatalf("Create(nil) err=%v", err)
	}
	if err := CreateMany(nil, []Row{{UserID: 1}}); err != nil {
		t.Fatalf("CreateMany(nil) err=%v", err)
	}
}

func TestCreateManyDedup(t *testing.T) {
	// 仅校验不 panic；真实写库在集成环境测
	rows := []Row{
		{UserID: 1, Type: TypeMention, Title: "a"},
		{UserID: 1, Type: TypeMention, Title: "b"}, // 同接收者去重
		{UserID: 0, Type: TypeMention, Title: "skip"},
	}
	if err := CreateMany(nil, rows); err != nil {
		t.Fatal(err)
	}
}
