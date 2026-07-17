package dal

import (
	"context"

	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

// EnsureSystemProblemsets 幂等创建用户的「我的收藏」「待做题单」
func EnsureSystemProblemsets(ctx context.Context, db *gorm.DB, ownerID uint) error {
	if db == nil || ownerID == 0 {
		return nil
	}
	specs := []struct {
		kind  string
		title string
	}{
		{model.ProblemsetKindFavorites, "我的收藏"},
		{model.ProblemsetKindTodo, "待做题单"},
	}
	for _, sp := range specs {
		var n int64
		if err := db.WithContext(ctx).Model(&model.Problemset{}).
			Where("owner_id = ? AND kind = ?", ownerID, sp.kind).
			Count(&n).Error; err != nil {
			return err
		}
		if n > 0 {
			continue
		}
		row := model.Problemset{
			OwnerID:     ownerID,
			Title:       sp.title,
			Description: "",
			Kind:        sp.kind,
			Visibility:  model.ProblemsetVisPrivate,
		}
		if err := db.WithContext(ctx).Create(&row).Error; err != nil {
			// 并发唯一冲突：忽略（无 DB 唯一约束时靠再查）
			var again int64
			_ = db.WithContext(ctx).Model(&model.Problemset{}).
				Where("owner_id = ? AND kind = ?", ownerID, sp.kind).
				Count(&again).Error
			if again == 0 {
				return err
			}
		}
	}
	return nil
}

// RemoveFromTodoOnAC 用户 AC 某题后，从该用户全部「待做」系统题单剔除
func RemoveFromTodoOnAC(ctx context.Context, db *gorm.DB, userID int64, problemID uint) error {
	if db == nil || userID <= 0 || problemID == 0 {
		return nil
	}
	if !db.Migrator().HasTable(&model.Problemset{}) || !db.Migrator().HasTable(&model.ProblemsetItem{}) {
		return nil
	}
	var todos []model.Problemset
	if err := db.WithContext(ctx).
		Select("id").
		Where("owner_id = ? AND kind = ?", uint(userID), model.ProblemsetKindTodo).
		Find(&todos).Error; err != nil {
		return err
	}
	if len(todos) == 0 {
		return nil
	}
	ids := make([]uint, 0, len(todos))
	for _, t := range todos {
		ids = append(ids, t.ID)
	}
	res := db.WithContext(ctx).
		Where("problemset_id IN ? AND problem_id = ?", ids, problemID).
		Delete(&model.ProblemsetItem{})
	if res.Error != nil {
		return res.Error
	}
	if res.RowsAffected > 0 {
		// 修正 item_count
		for _, id := range ids {
			var cnt int64
			_ = db.WithContext(ctx).Model(&model.ProblemsetItem{}).
				Where("problemset_id = ?", id).Count(&cnt).Error
			_ = db.WithContext(ctx).Model(&model.Problemset{}).
				Where("id = ?", id).Update("item_count", cnt).Error
		}
		log.Debugf("RemoveFromTodoOnAC user=%d problem=%d removed=%d", userID, problemID, res.RowsAffected)
	}
	return nil
}

// RemoveFromTodoOnACBatch 批量 AC 剔除（提交管线）
func RemoveFromTodoOnACBatch(ctx context.Context, db *gorm.DB, logs []model.SubmitLog) error {
	if db == nil || len(logs) == 0 {
		return nil
	}
	type pair struct {
		uid int64
		pid uint
	}
	seen := map[pair]struct{}{}
	for i := range logs {
		l := &logs[i]
		if !l.IsAC || l.UserID <= 0 || l.ProblemID == nil || *l.ProblemID == 0 {
			continue
		}
		p := pair{uid: l.UserID, pid: *l.ProblemID}
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		if err := RemoveFromTodoOnAC(ctx, db, p.uid, p.pid); err != nil {
			log.Warnf("RemoveFromTodoOnAC user=%d problem=%d: %v", p.uid, p.pid, err)
		}
	}
	return nil
}
