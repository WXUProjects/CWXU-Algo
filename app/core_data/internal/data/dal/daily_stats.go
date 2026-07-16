package dal

import (
	"context"
	"strings"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// DailyDelta 某用户某日的增量
type DailyDelta struct {
	UserID    int64
	Day       time.Time // 截断到日 00:00 local
	SubmitCnt int64
	AcCnt     int64
}

// AggregateSubmitDeltas 将新插入的提交聚合成日增量（仅新行，可重复调用不会用旧行）
func AggregateSubmitDeltas(logs []model.SubmitLog) []DailyDelta {
	if len(logs) == 0 {
		return nil
	}
	type key struct {
		uid int64
		day string
	}
	m := make(map[key]*DailyDelta, len(logs)/4+1)
	for i := range logs {
		l := &logs[i]
		day := time.Date(l.Time.Year(), l.Time.Month(), l.Time.Day(), 0, 0, 0, 0, l.Time.Location())
		k := key{uid: l.UserID, day: day.Format("2006-01-02")}
		d, ok := m[k]
		if !ok {
			d = &DailyDelta{UserID: l.UserID, Day: day}
			m[k] = d
		}
		// 力扣合成 AC / 最近通过明细不计提交（避免与日历双计）
		if model.CountsTowardSubmitStat(l.Platform, l.SubmitID) {
			d.SubmitCnt++
		}
		if l.IsAC {
			d.AcCnt++
		}
	}
	out := make([]DailyDelta, 0, len(m))
	for _, d := range m {
		if d.SubmitCnt == 0 && d.AcCnt == 0 {
			continue
		}
		out = append(out, *d)
	}
	return out
}

// ApplyDailyDeltas 原子累加日汇总
func ApplyDailyDeltas(ctx context.Context, db *gorm.DB, deltas []DailyDelta) error {
	if len(deltas) == 0 || db == nil {
		return nil
	}
	for _, d := range deltas {
		row := model.DailyUserStat{
			UserID:    d.UserID,
			Day:       d.Day,
			SubmitCnt: d.SubmitCnt,
			AcCnt:     d.AcCnt,
		}
		err := db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "user_id"}, {Name: "day"}},
				DoUpdates: clause.Assignments(map[string]interface{}{
					"submit_cnt": gorm.Expr("daily_user_stats.submit_cnt + EXCLUDED.submit_cnt"),
					"ac_cnt":     gorm.Expr("daily_user_stats.ac_cnt + EXCLUDED.ac_cnt"),
				}),
			}).
			Create(&row).Error
		if err != nil {
			return err
		}
	}
	return nil
}

// BackfillDailyUserStatsIfEmpty 表为空时从 submit_logs 全量聚合一次（启动幂等）
func BackfillDailyUserStatsIfEmpty(db *gorm.DB) {
	if db == nil || !db.Migrator().HasTable(&model.DailyUserStat{}) {
		return
	}
	var n int64
	if err := db.Model(&model.DailyUserStat{}).Count(&n).Error; err != nil {
		log.Warnf("daily_user_stats count failed: %v", err)
		return
	}
	if n > 0 {
		return
	}
	log.Infof("daily_user_stats empty, backfill from submit_logs…")
	// 与热力图一致：date_trunc('day', time) 使用库 session 时区
	res := db.Exec(`
		INSERT INTO daily_user_stats (user_id, day, submit_cnt, ac_cnt)
		SELECT
			user_id,
			date_trunc('day', time)::date AS day,
			COUNT(*) FILTER (
				WHERE ` + model.SQLExcludeLeetCodeNonSubmit + `
			) AS submit_cnt,
			COUNT(*) FILTER (WHERE is_ac = true) AS ac_cnt
		FROM submit_logs
		GROUP BY user_id, date_trunc('day', time)::date
		HAVING
			COUNT(*) FILTER (WHERE ` + model.SQLExcludeLeetCodeNonSubmit + `) > 0
			OR COUNT(*) FILTER (WHERE is_ac = true) > 0
		ON CONFLICT (user_id, day) DO NOTHING
	`)
	if res.Error != nil {
		log.Warnf("daily_user_stats backfill failed: %v", res.Error)
		return
	}
	log.Infof("daily_user_stats backfill done rows=%d", res.RowsAffected)
}

// PruneLeetCodeProbDuplicates 清理某用户已入库的重复 lc-prob（同 external_id 只留最新一条）
// 用于历史脏数据；新写入由 FilterNewSubmitLogs 拦截。
func PruneLeetCodeProbDuplicates(ctx context.Context, db *gorm.DB, userID int64) (int64, error) {
	if db == nil || userID == 0 {
		return 0, nil
	}
	// 保留同 user+external_id 下 time 最大、id 最大的一条，删其余
	res := db.WithContext(ctx).Exec(`
		DELETE FROM submit_logs a
		USING submit_logs b
		WHERE a.user_id = ?
		  AND a.platform = 'LeetCode'
		  AND a.submit_id LIKE 'lc-prob-%'
		  AND b.user_id = a.user_id
		  AND b.platform = a.platform
		  AND b.submit_id LIKE 'lc-prob-%'
		  AND a.external_id IS NOT NULL AND a.external_id <> ''
		  AND a.external_id = b.external_id
		  AND (a.time < b.time OR (a.time = b.time AND a.id < b.id))
	`, userID)
	return res.RowsAffected, res.Error
}

// FilterNewSubmitLogs 入库前去重：
//  1) 本批内按 submit_id 去重
//  2) 去掉库中已有 submit_id
//  3) 力扣 lc-prob：同一用户同一 titleSlug（external_id）只保留一条（防近期动态重复）
func FilterNewSubmitLogs(ctx context.Context, db *gorm.DB, logs []model.SubmitLog) ([]model.SubmitLog, error) {
	if len(logs) == 0 {
		return nil, nil
	}

	// 1) 本批 submit_id 去重（保留先出现）
	logs = dedupeSubmitLogsBySubmitID(logs)
	if len(logs) == 0 {
		return nil, nil
	}

	ids := make([]string, len(logs))
	for i := range logs {
		ids[i] = logs[i].SubmitID
	}
	// 2) 库中已有 submit_id
	const chunk = 500
	exist := make(map[string]struct{}, len(ids)/2)
	for i := 0; i < len(ids); i += chunk {
		j := i + chunk
		if j > len(ids) {
			j = len(ids)
		}
		var found []string
		if err := db.WithContext(ctx).Model(&model.SubmitLog{}).
			Where("submit_id IN ?", ids[i:j]).
			Pluck("submit_id", &found).Error; err != nil {
			return nil, err
		}
		for _, id := range found {
			exist[id] = struct{}{}
		}
	}
	out := make([]model.SubmitLog, 0, len(logs))
	for i := range logs {
		if logs[i].SubmitID == "" {
			continue
		}
		if _, ok := exist[logs[i].SubmitID]; ok {
			continue
		}
		out = append(out, logs[i])
	}
	if len(out) == 0 {
		return nil, nil
	}

	// 3) 力扣最近通过：同用户同题已入库则跳过（API 会对一题返回多次 AC / 换 submissionId）
	out, err := filterLeetCodeProbAlreadyHaveSlug(ctx, db, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// dedupeSubmitLogsBySubmitID 本批内按 submit_id 去重，保留首次出现
func dedupeSubmitLogsBySubmitID(logs []model.SubmitLog) []model.SubmitLog {
	if len(logs) <= 1 {
		return logs
	}
	seen := make(map[string]struct{}, len(logs))
	out := make([]model.SubmitLog, 0, len(logs))
	for i := range logs {
		id := logs[i].SubmitID
		if id == "" {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		out = append(out, logs[i])
	}
	return out
}

// filterLeetCodeProbAlreadyHaveSlug 去掉「该用户该 titleSlug 已有 lc-prob-*」的候选
func filterLeetCodeProbAlreadyHaveSlug(ctx context.Context, db *gorm.DB, logs []model.SubmitLog) ([]model.SubmitLog, error) {
	type ukey struct {
		uid int64
		ext string
	}
	// 收集本批 lc-prob 的 (user, external_id)
	need := make(map[ukey]struct{})
	userSet := make(map[int64]struct{})
	for i := range logs {
		l := &logs[i]
		if l.Platform != "LeetCode" || !strings.HasPrefix(l.SubmitID, "lc-prob-") {
			continue
		}
		ext := strings.TrimSpace(l.ExternalID)
		if ext == "" {
			// 尝试从 problem 首 token 取 slug
			if f := strings.Fields(l.Problem); len(f) > 0 {
				ext = f[0]
			}
		}
		if ext == "" {
			continue
		}
		// 规范化 external_id 写回，便于后续绑定
		if l.ExternalID == "" {
			l.ExternalID = ext
		}
		k := ukey{uid: l.UserID, ext: ext}
		need[k] = struct{}{}
		userSet[l.UserID] = struct{}{}
	}
	if len(need) == 0 {
		return logs, nil
	}

	uids := make([]int64, 0, len(userSet))
	for u := range userSet {
		uids = append(uids, u)
	}
	exts := make([]string, 0, len(need))
	extSeen := make(map[string]struct{}, len(need))
	for k := range need {
		if _, ok := extSeen[k.ext]; ok {
			continue
		}
		extSeen[k.ext] = struct{}{}
		exts = append(exts, k.ext)
	}

	type row struct {
		UserID     int64  `gorm:"column:user_id"`
		ExternalID string `gorm:"column:external_id"`
	}
	var rows []row
	if err := db.WithContext(ctx).Model(&model.SubmitLog{}).
		Select("DISTINCT user_id, external_id").
		Where("platform = ? AND submit_id LIKE ? AND user_id IN ? AND external_id IN ?",
			"LeetCode", "lc-prob-%", uids, exts).
		Find(&rows).Error; err != nil {
		return nil, err
	}
	have := make(map[ukey]struct{}, len(rows))
	for _, r := range rows {
		have[ukey{uid: r.UserID, ext: r.ExternalID}] = struct{}{}
	}
	// 本批内同用户同 slug 也只留一条（保留先出现，通常更新）
	batchSeen := make(map[ukey]struct{}, len(need))
	out := make([]model.SubmitLog, 0, len(logs))
	for i := range logs {
		l := &logs[i]
		if l.Platform == "LeetCode" && strings.HasPrefix(l.SubmitID, "lc-prob-") {
			ext := strings.TrimSpace(l.ExternalID)
			if ext == "" {
				if f := strings.Fields(l.Problem); len(f) > 0 {
					ext = f[0]
				}
			}
			if ext != "" {
				k := ukey{uid: l.UserID, ext: ext}
				if _, ok := have[k]; ok {
					continue
				}
				if _, ok := batchSeen[k]; ok {
					continue
				}
				batchSeen[k] = struct{}{}
			}
		}
		out = append(out, *l)
	}
	return out, nil
}
