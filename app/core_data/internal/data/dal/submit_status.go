package dal

import (
	"context"
	"strings"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

// RefreshPendingSubmitVerdicts 回写已入库但状态仍为评测中/空的提交。
// 场景：CF 首次爬到时 verdict 为空或 TESTING，已进账本后 FilterUncounted 会跳过，
// 若不回写则 UI 永久空白。不重计 submit_cnt；若 is_ac 0→1 则补 daily.ac 与 user_ac。
// 另：允许把历史长字面量（WRONG_ANSWER）归一为短码（WA），不改 is_ac 统计。
func RefreshPendingSubmitVerdicts(ctx context.Context, db *gorm.DB, fetched []model.SubmitLog) (int64, error) {
	if db == nil || len(fetched) == 0 {
		return 0, nil
	}
	want := make(map[string]model.SubmitLog, len(fetched))
	ids := make([]string, 0, len(fetched))
	for i := range fetched {
		l := fetched[i]
		if l.SubmitID == "" {
			continue
		}
		if model.IsPendingSubmitStatus(l.Status) {
			continue
		}
		if _, ok := want[l.SubmitID]; !ok {
			ids = append(ids, l.SubmitID)
		}
		want[l.SubmitID] = l
	}
	if len(ids) == 0 {
		return 0, nil
	}

	var updated int64
	const chunk = 300
	for i := 0; i < len(ids); i += chunk {
		j := i + chunk
		if j > len(ids) {
			j = len(ids)
		}
		part := ids[i:j]
		var existing []model.SubmitLog
		if err := db.WithContext(ctx).
			Where("submit_id IN ?", part).
			Find(&existing).Error; err != nil {
			return updated, err
		}
		for _, old := range existing {
			neu, ok := want[old.SubmitID]
			if !ok {
				continue
			}
			newStatus := strings.TrimSpace(neu.Status)
			if newStatus == "" {
				continue
			}
			oldStatus := strings.TrimSpace(old.Status)
			if oldStatus == newStatus {
				continue
			}
			// 仅：旧 pending，或长名→短码归一
			if !model.IsPendingSubmitStatus(oldStatus) && !shouldRewriteFinalStatus(oldStatus, newStatus) {
				continue
			}

			newIsAC := model.IsAcceptedStatus(newStatus)
			oldIsAC := old.IsAC || model.IsAcceptedStatus(oldStatus)

			res := db.WithContext(ctx).Model(&model.SubmitLog{}).
				Where("submit_id = ? AND status = ?", old.SubmitID, old.Status).
				Updates(map[string]interface{}{
					"status": newStatus,
					"is_ac":  newIsAC,
				})
			if res.Error != nil {
				return updated, res.Error
			}
			if res.RowsAffected == 0 {
				continue
			}
			updated += res.RowsAffected

			if !oldIsAC && newIsAC {
				row := old
				row.Status = newStatus
				row.IsAC = true
				if err := ApplyUserACFromSubmits(ctx, db, []model.SubmitLog{row}); err != nil {
					log.Warnf("RefreshPending: ApplyUserAC submit=%s: %v", old.SubmitID, err)
				}
				day := time.Date(row.Time.Year(), row.Time.Month(), row.Time.Day(), 0, 0, 0, 0, row.Time.Location())
				plat := strings.TrimSpace(row.Platform)
				if plat == "" {
					plat = "?"
				}
				if err := ApplyDailyDeltas(ctx, db, []DailyDelta{{
					UserID:   row.UserID,
					Day:      day,
					Platform: plat,
					AcCnt:    1,
				}}); err != nil {
					log.Warnf("RefreshPending: ApplyDailyAC submit=%s: %v", old.SubmitID, err)
				}
			}
		}
	}
	return updated, nil
}

// shouldRewriteFinalStatus 允许把 CF 原始长 verdict 归一成短码（不重计）
func shouldRewriteFinalStatus(oldStatus, newStatus string) bool {
	o := strings.ToUpper(strings.TrimSpace(oldStatus))
	n := strings.ToUpper(strings.TrimSpace(newStatus))
	if o == n || n == "" {
		return false
	}
	longToShort := map[string]string{
		"WRONG_ANSWER":            "WA",
		"TIME_LIMIT_EXCEEDED":     "TLE",
		"MEMORY_LIMIT_EXCEEDED":   "MLE",
		"RUNTIME_ERROR":           "RE",
		"COMPILATION_ERROR":       "CE",
		"PRESENTATION_ERROR":      "PE",
		"IDLENESS_LIMIT_EXCEEDED": "ILE",
		"SECURITY_VIOLATED":       "SV",
	}
	if short, ok := longToShort[o]; ok && short == n {
		return true
	}
	return false
}
