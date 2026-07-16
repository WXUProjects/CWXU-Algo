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

// DailyDelta 某用户某平台某日的增量
type DailyDelta struct {
	UserID    int64
	Day       time.Time // 截断到日 00:00 local
	Platform  string
	SubmitCnt int64
	AcCnt     int64
}

// AggregateSubmitDeltas 将新插入的提交聚合成日增量（仅新行，可重复调用不会用旧行）
func AggregateSubmitDeltas(logs []model.SubmitLog) []DailyDelta {
	if len(logs) == 0 {
		return nil
	}
	type key struct {
		uid  int64
		day  string
		plat string
	}
	m := make(map[key]*DailyDelta, len(logs)/4+1)
	for i := range logs {
		l := &logs[i]
		day := time.Date(l.Time.Year(), l.Time.Month(), l.Time.Day(), 0, 0, 0, 0, l.Time.Location())
		plat := strings.TrimSpace(l.Platform)
		k := key{uid: l.UserID, day: day.Format("2006-01-02"), plat: plat}
		d, ok := m[k]
		if !ok {
			d = &DailyDelta{UserID: l.UserID, Day: day, Platform: plat}
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

// ApplyDailyDeltas 原子累加日汇总（按 user+day+platform）
func ApplyDailyDeltas(ctx context.Context, db *gorm.DB, deltas []DailyDelta) error {
	if len(deltas) == 0 || db == nil {
		return nil
	}
	for _, d := range deltas {
		row := model.DailyUserStat{
			UserID:    d.UserID,
			Day:       d.Day,
			Platform:  d.Platform,
			SubmitCnt: d.SubmitCnt,
			AcCnt:     d.AcCnt,
		}
		err := db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "user_id"}, {Name: "day"}, {Name: "platform"}},
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

// InsertCountedSubmitIDs 写入已计入账本（ON CONFLICT DO NOTHING）
func InsertCountedSubmitIDs(ctx context.Context, db *gorm.DB, logs []model.SubmitLog) error {
	if db == nil || len(logs) == 0 {
		return nil
	}
	rows := make([]model.CountedSubmitID, 0, len(logs))
	for i := range logs {
		if logs[i].SubmitID == "" {
			continue
		}
		rows = append(rows, model.CountedSubmitID{
			SubmitID: logs[i].SubmitID,
			UserID:   logs[i].UserID,
			Platform: strings.TrimSpace(logs[i].Platform),
		})
	}
	if len(rows) == 0 {
		return nil
	}
	const batch = 500
	for i := 0; i < len(rows); i += batch {
		j := i + batch
		if j > len(rows) {
			j = len(rows)
		}
		chunk := rows[i:j]
		if err := db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "submit_id"}},
				DoNothing: true,
			}).
			Create(&chunk).Error; err != nil {
			return err
		}
	}
	return nil
}

// FilterHotSubmitLogs 仅保留热窗内明细（近 6 个月）
func FilterHotSubmitLogs(logs []model.SubmitLog, now time.Time) []model.SubmitLog {
	if len(logs) == 0 {
		return nil
	}
	cutoff := model.SubmitLogHotCutoff(now)
	out := make([]model.SubmitLog, 0, len(logs))
	for i := range logs {
		if !logs[i].Time.Before(cutoff) {
			out = append(out, logs[i])
		}
	}
	return out
}

// FilterUncountedSubmits 入库前去重（账本为真相）：
//  1) 本批内按 submit_id 去重
//  2) 去掉 counted_submit_ids 已有（冷明细剪掉后仍靠账本防双计）
//  3) 热表 submit_logs 兜底（账本未写完/迁移前）
//  4) 力扣 lc-prob：同一用户同一 titleSlug 已有则跳过
//
// 注意：daily_user_stats 是累加语义；若账本残缺，全量爬会把冷历史再次 ApplyDaily。
func FilterUncountedSubmits(ctx context.Context, db *gorm.DB, logs []model.SubmitLog) ([]model.SubmitLog, error) {
	if len(logs) == 0 {
		return nil, nil
	}

	logs = dedupeSubmitLogsBySubmitID(logs)
	if len(logs) == 0 {
		return nil, nil
	}

	ids := make([]string, len(logs))
	for i := range logs {
		ids[i] = logs[i].SubmitID
	}
	const chunk = 500
	exist := make(map[string]struct{}, len(ids)/2)
	hasLedger := db.Migrator().HasTable(&model.CountedSubmitID{})
	// 账本为唯一真相；热表仅兜底（冷提交不在热表）
	for i := 0; i < len(ids); i += chunk {
		j := i + chunk
		if j > len(ids) {
			j = len(ids)
		}
		part := ids[i:j]
		if hasLedger {
			var found []string
			if err := db.WithContext(ctx).Model(&model.CountedSubmitID{}).
				Where("submit_id IN ?", part).
				Pluck("submit_id", &found).Error; err != nil {
				return nil, err
			}
			for _, id := range found {
				exist[id] = struct{}{}
			}
		}
		// 热表兜底：防止账本短暂落后时热窗口双计
		var foundHot []string
		if err := db.WithContext(ctx).Model(&model.SubmitLog{}).
			Where("submit_id IN ?", part).
			Pluck("submit_id", &foundHot).Error; err != nil {
			return nil, err
		}
		for _, id := range foundHot {
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

	out, err := filterLeetCodeProbAlreadyHaveSlug(ctx, db, out)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// FilterNewSubmitLogs 兼容旧名：等价 FilterUncountedSubmits
func FilterNewSubmitLogs(ctx context.Context, db *gorm.DB, logs []model.SubmitLog) ([]model.SubmitLog, error) {
	return FilterUncountedSubmits(ctx, db, logs)
}

// BackfillDailyUserStatsIfEmpty 表为空时从 submit_logs 全量聚合一次（启动幂等）
// 含 platform 维度；迁移完整流程见 RunSubmitRetentionMigrate。
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
	res := db.Exec(`
		INSERT INTO daily_user_stats (user_id, day, platform, submit_cnt, ac_cnt)
		SELECT
			user_id,
			date_trunc('day', time)::date AS day,
			COALESCE(NULLIF(btrim(platform), ''), '?') AS platform,
			COUNT(*) FILTER (
				WHERE ` + model.SQLExcludeLeetCodeNonSubmit + `
			) AS submit_cnt,
			COUNT(*) FILTER (WHERE is_ac = true) AS ac_cnt
		FROM submit_logs
		GROUP BY user_id, date_trunc('day', time)::date, COALESCE(NULLIF(btrim(platform), ''), '?')
		HAVING
			COUNT(*) FILTER (WHERE ` + model.SQLExcludeLeetCodeNonSubmit + `) > 0
			OR COUNT(*) FILTER (WHERE is_ac = true) > 0
		ON CONFLICT (user_id, day, platform) DO NOTHING
	`)
	if res.Error != nil {
		log.Warnf("daily_user_stats backfill failed: %v", res.Error)
		return
	}
	log.Infof("daily_user_stats backfill done rows=%d", res.RowsAffected)
}

// PruneLeetCodeProbDuplicates 清理某用户已入库的重复 lc-prob（同 external_id 只留最新一条）
func PruneLeetCodeProbDuplicates(ctx context.Context, db *gorm.DB, userID int64) (int64, error) {
	if db == nil || userID == 0 {
		return 0, nil
	}
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
	need := make(map[ukey]struct{})
	userSet := make(map[int64]struct{})
	for i := range logs {
		l := &logs[i]
		if l.Platform != "LeetCode" || !strings.HasPrefix(l.SubmitID, "lc-prob-") {
			continue
		}
		ext := strings.TrimSpace(l.ExternalID)
		if ext == "" {
			if f := strings.Fields(l.Problem); len(f) > 0 {
				ext = f[0]
			}
		}
		if ext == "" {
			continue
		}
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
	// 账本时代：同题已在 user_ac_problems（e:LeetCode:slug）也视为已有
	if db.Migrator().HasTable(&model.UserACProblem{}) {
		keys := make([]string, 0, len(need))
		for k := range need {
			keys = append(keys, "e:LeetCode:"+k.ext)
		}
		type acRow struct {
			UserID     int64  `gorm:"column:user_id"`
			ProblemKey string `gorm:"column:problem_key"`
		}
		var acRows []acRow
		if err := db.WithContext(ctx).Model(&model.UserACProblem{}).
			Select("user_id, problem_key").
			Where("user_id IN ? AND problem_key IN ?", uids, keys).
			Find(&acRows).Error; err == nil {
			for _, r := range acRows {
				ext := strings.TrimPrefix(r.ProblemKey, "e:LeetCode:")
				if ext != r.ProblemKey {
					have[ukey{uid: r.UserID, ext: ext}] = struct{}{}
				}
			}
		}
	}

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

// DeletePlatformDailyStats 换绑：删某用户某平台日汇总
func DeletePlatformDailyStats(ctx context.Context, db *gorm.DB, userID int64, platform string) error {
	if db == nil || userID <= 0 || platform == "" {
		return nil
	}
	if !db.Migrator().HasTable(&model.DailyUserStat{}) {
		return nil
	}
	return db.WithContext(ctx).
		Where("user_id = ? AND platform = ?", userID, platform).
		Delete(&model.DailyUserStat{}).Error
}

// DeletePlatformCountedIDs 换绑：删某用户某平台账本
func DeletePlatformCountedIDs(ctx context.Context, db *gorm.DB, userID int64, platform string) error {
	if db == nil || userID <= 0 || platform == "" {
		return nil
	}
	if !db.Migrator().HasTable(&model.CountedSubmitID{}) {
		return nil
	}
	return db.WithContext(ctx).
		Where("user_id = ? AND platform = ?", userID, platform).
		Delete(&model.CountedSubmitID{}).Error
}

// DeleteUserCountedIDs 硬删用户账本
func DeleteUserCountedIDs(ctx context.Context, db *gorm.DB, userID int64) error {
	if db == nil || userID <= 0 {
		return nil
	}
	if !db.Migrator().HasTable(&model.CountedSubmitID{}) {
		return nil
	}
	return db.WithContext(ctx).Where("user_id = ?", userID).Delete(&model.CountedSubmitID{}).Error
}

// PruneColdSubmitLogs 分批删除热窗外明细；返回累计删除行数
func PruneColdSubmitLogs(ctx context.Context, db *gorm.DB, now time.Time, batchSize int) (int64, error) {
	if db == nil {
		return 0, nil
	}
	if batchSize <= 0 {
		batchSize = 5000
	}
	cutoff := model.SubmitLogHotCutoff(now)
	var total int64
	for {
		res := db.WithContext(ctx).Exec(`
			DELETE FROM submit_logs
			WHERE ctid IN (
				SELECT ctid FROM submit_logs
				WHERE time < ?
				LIMIT ?
			)
		`, cutoff, batchSize)
		if res.Error != nil {
			return total, res.Error
		}
		total += res.RowsAffected
		if res.RowsAffected == 0 {
			break
		}
		// 大批量时让出连接
		if res.RowsAffected < int64(batchSize) {
			break
		}
	}
	return total, nil
}
