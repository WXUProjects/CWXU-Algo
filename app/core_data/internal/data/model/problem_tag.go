package model

// ProblemTag 题 ↔ 标签倒排（空间换时间：避免 List/ListTags 扫 jsonb）
type ProblemTag struct {
	ProblemID uint   `gorm:"primaryKey;comment:题目ID"`
	Tag       string `gorm:"primaryKey;size:64;index:idx_problem_tag_tag,priority:1;not null;comment:算法标签"`
}

func (ProblemTag) TableName() string { return "problem_tags" }
