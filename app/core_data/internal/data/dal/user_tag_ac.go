package dal

import (
	"context"
	"fmt"

	"cwxu-algo/app/core_data/internal/data/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// IncUserTagAC 用户首次 AC 某题后，对该题标签各 +1
func IncUserTagAC(ctx context.Context, db *gorm.DB, userID int64, tags []string) error {
	if db == nil || userID <= 0 {
		return nil
	}
	tags = NormalizeTags(tags)
	for _, tag := range tags {
		row := model.UserTagAC{UserID: userID, Tag: tag, Count: 1}
		if err := db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "user_id"}, {Name: "tag"}},
				DoUpdates: clause.Assignments(map[string]interface{}{
					"count": gorm.Expr("user_tag_ac.count + 1"),
				}),
			}).
			Create(&row).Error; err != nil {
			return err
		}
	}
	return nil
}

// AdjustUserTagACForProblemTagsChange 题标签从 old→new 时，所有 AC 过该题的用户差分
func AdjustUserTagACForProblemTagsChange(ctx context.Context, db *gorm.DB, problemID uint, oldTags, newTags []string) error {
	if db == nil || problemID == 0 {
		return nil
	}
	oldTags = NormalizeTags(oldTags)
	newTags = NormalizeTags(newTags)
	if sameStringSet(oldTags, newTags) {
		return nil
	}
	removed, added := diffStringSets(oldTags, newTags)
	if len(removed) == 0 && len(added) == 0 {
		return nil
	}

	// AC 过该题的用户
	var userIDs []int64
	if err := db.WithContext(ctx).Model(&model.UserProblemStatus{}).
		Where("problem_id = ? AND status = ?", problemID, model.UserProblemStatusAC).
		Pluck("user_id", &userIDs).Error; err != nil {
		return err
	}
	if len(userIDs) == 0 {
		// 回退：从 user_ac_problems p:id
		key := fmt.Sprintf("p:%d", problemID)
		_ = db.WithContext(ctx).Model(&model.UserACProblem{}).
			Where("problem_key = ?", key).
			Pluck("user_id", &userIDs).Error
	}
	if len(userIDs) == 0 {
		return nil
	}

	for _, uid := range userIDs {
		for _, tag := range removed {
			if err := db.WithContext(ctx).Exec(`
				UPDATE user_tag_ac SET count = count - 1
				WHERE user_id = ? AND tag = ? AND count > 0
			`, uid, tag).Error; err != nil {
				return err
			}
			_ = db.WithContext(ctx).Exec(`
				DELETE FROM user_tag_ac WHERE user_id = ? AND tag = ? AND count <= 0
			`, uid, tag).Error
		}
		if err := IncUserTagAC(ctx, db, uid, added); err != nil {
			return err
		}
	}
	return nil
}

func diffStringSets(old, neu []string) (removed, added []string) {
	om := map[string]struct{}{}
	nm := map[string]struct{}{}
	for _, t := range old {
		om[t] = struct{}{}
	}
	for _, t := range neu {
		nm[t] = struct{}{}
	}
	for t := range om {
		if _, ok := nm[t]; !ok {
			removed = append(removed, t)
		}
	}
	for t := range nm {
		if _, ok := om[t]; !ok {
			added = append(added, t)
		}
	}
	return
}

// IncUserTagACForFirstProblemAC 用户首次绑定 AC 某 problem_id 时按题当前 tags +1
func IncUserTagACForFirstProblemAC(ctx context.Context, db *gorm.DB, userID int64, problemID uint) error {
	if db == nil || userID <= 0 || problemID == 0 {
		return nil
	}
	var p model.Problem
	if err := db.WithContext(ctx).Select("id", "tags", "status").First(&p, problemID).Error; err != nil {
		return nil // 题不存在则跳过
	}
	tags := NormalizeTags([]string(p.Tags))
	if len(tags) == 0 {
		return nil
	}
	return IncUserTagAC(ctx, db, userID, tags)
}

// ListUserTagAC 画像雷达
func ListUserTagAC(ctx context.Context, db *gorm.DB, userID int64, limit int) ([]struct {
	Tag   string
	Count int64
}, error) {
	if limit <= 0 {
		limit = 20
	}
	type row struct {
		Tag   string
		Count int64
	}
	var rows []row
	err := db.WithContext(ctx).
		Model(&model.UserTagAC{}).
		Select("tag, count").
		Where("user_id = ? AND count > 0", userID).
		Order("count DESC, tag ASC").
		Limit(limit).
		Find(&rows).Error
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


