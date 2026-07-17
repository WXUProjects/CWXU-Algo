package dal

import (
	"context"
	"strings"

	"cwxu-algo/app/core_data/internal/data/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// NormalizeTags 去空白去重
func NormalizeTags(tags []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if len(t) > 64 {
			t = t[:64]
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
	}
	return out
}

// SyncProblemTags 用新标签集合替换某题倒排行；返回 (old, new) 供 user_tag_ac 差分
func SyncProblemTags(ctx context.Context, db *gorm.DB, problemID uint, tags []string) (oldTags, newTags []string, err error) {
	if db == nil || problemID == 0 {
		return nil, nil, nil
	}
	newTags = NormalizeTags(tags)

	var oldRows []model.ProblemTag
	if err = db.WithContext(ctx).Where("problem_id = ?", problemID).Find(&oldRows).Error; err != nil {
		return nil, nil, err
	}
	oldTags = make([]string, 0, len(oldRows))
	for _, r := range oldRows {
		oldTags = append(oldTags, r.Tag)
	}

	// 相同集合则跳过
	if sameStringSet(oldTags, newTags) {
		return oldTags, newTags, nil
	}

	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if e := tx.Where("problem_id = ?", problemID).Delete(&model.ProblemTag{}).Error; e != nil {
			return e
		}
		if len(newTags) == 0 {
			return nil
		}
		rows := make([]model.ProblemTag, 0, len(newTags))
		for _, t := range newTags {
			rows = append(rows, model.ProblemTag{ProblemID: problemID, Tag: t})
		}
		return tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&rows).Error
	})
	return oldTags, newTags, err
}

func sameStringSet(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	m := map[string]struct{}{}
	for _, x := range a {
		m[x] = struct{}{}
	}
	for _, x := range b {
		if _, ok := m[x]; !ok {
			return false
		}
	}
	return true
}

// ListTagCounts 从倒排表聚合
func ListTagCounts(ctx context.Context, db *gorm.DB, limit int) ([]struct {
	Tag   string
	Count int64
}, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 300 {
		limit = 300
	}
	type row struct {
		Tag   string
		Count int64
	}
	var rows []row
	err := db.WithContext(ctx).Raw(`
		SELECT tag, COUNT(*)::bigint AS count
		FROM problem_tags
		GROUP BY tag
		ORDER BY count DESC, tag ASC
		LIMIT ?
	`, limit).Scan(&rows).Error
	out := make([]struct {
		Tag   string
		Count int64
	}, 0, len(rows))
	for _, r := range rows {
		out = append(out, struct {
			Tag   string
			Count int64
		}{r.Tag, r.Count})
	}
	return out, err
}


