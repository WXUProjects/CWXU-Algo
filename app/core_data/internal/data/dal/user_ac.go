package dal

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// ApplyUserACFromSubmits 对新插入的 AC 提交维护去重预聚合（写入时提前算）
// - user_ac_problems：生涯每题首次 AC（含 platform）
// - user_ac_problem_days：该自然日是否 AC 过该题
func ApplyUserACFromSubmits(ctx context.Context, db *gorm.DB, logs []model.SubmitLog) error {
	if db == nil || len(logs) == 0 {
		return nil
	}

	type firstRec struct {
		userID   int64
		key      string
		platform string
		at       time.Time
	}
	firstMap := make(map[string]*firstRec, 32)
	dayMap := make(map[string]model.UserACProblemDay, 32)

	for i := range logs {
		l := &logs[i]
		if !l.IsAC {
			continue
		}
		key := model.ACProblemKeyFromLog(l)
		plat := strings.TrimSpace(l.Platform)
		day := time.Date(l.Time.Year(), l.Time.Month(), l.Time.Day(), 0, 0, 0, 0, l.Time.Location())

		fk := fmt.Sprintf("%d\x00%s", l.UserID, key)
		if prev, ok := firstMap[fk]; !ok || l.Time.Before(prev.at) {
			firstMap[fk] = &firstRec{userID: l.UserID, key: key, platform: plat, at: l.Time}
		}

		dk := fmt.Sprintf("%d\x00%s\x00%s", l.UserID, day.Format("2006-01-02"), key)
		dayMap[dk] = model.UserACProblemDay{
			UserID:     l.UserID,
			Day:        day,
			ProblemKey: key,
			Platform:   plat,
		}
	}

	for _, f := range firstMap {
		row := model.UserACProblem{
			UserID:     f.userID,
			ProblemKey: f.key,
			Platform:   f.platform,
			FirstACAt:  f.at,
		}
		err := db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "user_id"}, {Name: "problem_key"}},
				DoUpdates: clause.Assignments(map[string]interface{}{
					"first_ac_at": gorm.Expr(
						"CASE WHEN EXCLUDED.first_ac_at < user_ac_problems.first_ac_at THEN EXCLUDED.first_ac_at ELSE user_ac_problems.first_ac_at END",
					),
					// platform 仅在仍空时补上
					"platform": gorm.Expr(
						"CASE WHEN user_ac_problems.platform = '' OR user_ac_problems.platform IS NULL THEN EXCLUDED.platform ELSE user_ac_problems.platform END",
					),
				}),
			}).
			Create(&row).Error
		if err != nil {
			return err
		}
	}

	for _, row := range dayMap {
		err := db.WithContext(ctx).
			Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "user_id"}, {Name: "day"}, {Name: "problem_key"}},
				DoNothing: true,
			}).
			Create(&row).Error
		if err != nil {
			return err
		}
	}
	return nil
}

// PeriodAcDistinctFromPreagg 个人 AC 去重时段统计（读预聚合，不扫 submit_logs）
func PeriodAcDistinctFromPreagg(db *gorm.DB, userId int64, now time.Time) (PeriodAcCount, error) {
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	thisWeekStart := getWeekStart(now)
	lastWeekStart := thisWeekStart.Add(-7 * 24 * time.Hour)
	thisMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	lastMonthStart := thisMonthStart.AddDate(0, -1, 0)
	thisYearStart := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
	lastYearStart := thisYearStart.AddDate(-1, 0, 0)

	todayDay := todayStart.Format("2006-01-02")
	weekDay := thisWeekStart.Format("2006-01-02")
	lastWeekDay := lastWeekStart.Format("2006-01-02")
	monthDay := thisMonthStart.Format("2006-01-02")
	lastMonthDay := lastMonthStart.Format("2006-01-02")
	yearDay := thisYearStart.Format("2006-01-02")
	lastYearDay := lastYearStart.Format("2006-01-02")

	var ac PeriodAcCount
	err := db.Table("user_ac_problem_days").
		Where("user_id = ?", userId).
		Select(`
			COUNT(DISTINCT problem_key) FILTER (WHERE day = ?::date) AS today,
			COUNT(DISTINCT problem_key) FILTER (WHERE day >= ?::date AND day <= ?::date) AS this_week,
			COUNT(DISTINCT problem_key) FILTER (WHERE day >= ?::date AND day < ?::date) AS last_week,
			COUNT(DISTINCT problem_key) FILTER (WHERE day >= ?::date AND day <= ?::date) AS this_month,
			COUNT(DISTINCT problem_key) FILTER (WHERE day >= ?::date AND day < ?::date) AS last_month,
			COUNT(DISTINCT problem_key) FILTER (WHERE day >= ?::date AND day <= ?::date) AS this_year,
			COUNT(DISTINCT problem_key) FILTER (WHERE day >= ?::date AND day < ?::date) AS last_year
		`,
			todayDay,
			weekDay, todayDay,
			lastWeekDay, weekDay,
			monthDay, todayDay,
			lastMonthDay, monthDay,
			yearDay, todayDay,
			lastYearDay, yearDay,
		).Scan(&ac).Error
	if err != nil {
		return ac, err
	}

	_ = db.Table("user_ac_problems").
		Where("user_id = ?", userId).
		Count(&ac.Total).Error

	var raw struct{ Total int64 }
	_ = db.Table("daily_user_stats").
		Select("COALESCE(SUM(ac_cnt),0) AS total").
		Where("user_id = ?", userId).
		Scan(&raw).Error
	ac.TotalRaw = raw.Total
	// 不变量：累计 AC 次数 ≥ 去重题数（每题至少 1 次 AC）
	// 日汇总与 user_ac 短暂不一致（清洗/重爬中）时以题数为下界，避免前端出现「557 次 / 1339 题」
	if ac.TotalRaw < ac.Total {
		ac.TotalRaw = ac.Total
	}
	return ac, nil
}

// RebuildUserPreaggFromSubmits 按该用户当前 submit_logs 全量重建预聚合（运维/修复用）。
// SetSpider 主路径已改为按平台剪枝，不再调用全量 Rebuild。
// Deprecated: 明细不全时勿用于生产换绑。
func RebuildUserPreaggFromSubmits(ctx context.Context, db *gorm.DB, userId int64) error {
	if db == nil || userId <= 0 {
		return nil
	}
	tx := db.WithContext(ctx)

	if tx.Migrator().HasTable(&model.DailyUserStat{}) {
		if err := tx.Where("user_id = ?", userId).Delete(&model.DailyUserStat{}).Error; err != nil {
			return err
		}
		if err := tx.Exec(`
			INSERT INTO daily_user_stats (user_id, day, platform, submit_cnt, ac_cnt)
			SELECT
				user_id,
				date_trunc('day', time)::date AS day,
				COALESCE(NULLIF(btrim(platform), ''), '?') AS platform,
				COUNT(*) FILTER (
					WHERE `+model.SQLExcludeLeetCodeNonSubmit+`
				) AS submit_cnt,
				COUNT(*) FILTER (WHERE is_ac = true) AS ac_cnt
			FROM submit_logs
			WHERE user_id = ?
			GROUP BY user_id, date_trunc('day', time)::date, COALESCE(NULLIF(btrim(platform), ''), '?')
			HAVING
				COUNT(*) FILTER (WHERE `+model.SQLExcludeLeetCodeNonSubmit+`) > 0
				OR COUNT(*) FILTER (WHERE is_ac = true) > 0
		`, userId).Error; err != nil {
			return err
		}
	}

	if tx.Migrator().HasTable(&model.UserACProblem{}) {
		if err := tx.Where("user_id = ?", userId).Delete(&model.UserACProblem{}).Error; err != nil {
			return err
		}
		if err := tx.Exec(`
			INSERT INTO user_ac_problems (user_id, problem_key, platform, first_ac_at)
			SELECT user_id, problem_key, platform, MIN(time) AS first_ac_at
			FROM (
				SELECT
					user_id,
					time,
					COALESCE(NULLIF(btrim(platform), ''), '?') AS platform,
					COALESCE(
						CASE WHEN problem_id IS NOT NULL AND problem_id <> 0 THEN 'p:' || problem_id::text END,
						CASE WHEN external_id IS NOT NULL AND btrim(external_id) <> '' THEN 'e:' || platform || ':' || external_id END,
						'n:' || platform || ':' || COALESCE(problem, '')
					) AS problem_key
				FROM submit_logs
				WHERE user_id = ? AND is_ac = true
			) t
			GROUP BY user_id, problem_key, platform
		`, userId).Error; err != nil {
			return err
		}
	}

	if tx.Migrator().HasTable(&model.UserACProblemDay{}) {
		if err := tx.Where("user_id = ?", userId).Delete(&model.UserACProblemDay{}).Error; err != nil {
			return err
		}
		if err := tx.Exec(`
			INSERT INTO user_ac_problem_days (user_id, day, problem_key, platform)
			SELECT DISTINCT
				user_id,
				date_trunc('day', time)::date AS day,
				COALESCE(
					CASE WHEN problem_id IS NOT NULL AND problem_id <> 0 THEN 'p:' || problem_id::text END,
					CASE WHEN external_id IS NOT NULL AND btrim(external_id) <> '' THEN 'e:' || platform || ':' || external_id END,
					'n:' || platform || ':' || COALESCE(problem, '')
				) AS problem_key,
				COALESCE(NULLIF(btrim(platform), ''), '?') AS platform
			FROM submit_logs
			WHERE user_id = ? AND is_ac = true
		`, userId).Error; err != nil {
			return err
		}
	}
	return nil
}

// PromoteUserACFromBoundSubmits 根据已绑定 problem_id 的 AC 明细，把 e:/n: 预聚合键升为 p:{id}。
func PromoteUserACFromBoundSubmits(ctx context.Context, db *gorm.DB, userID int64) error {
	if db == nil || userID <= 0 {
		return nil
	}
	type bound struct {
		ProblemID  uint
		Platform   string
		ExternalID string
		Problem    string
	}
	var rows []bound
	if err := db.WithContext(ctx).Raw(`
		SELECT DISTINCT
			problem_id AS problem_id,
			COALESCE(platform, '') AS platform,
			COALESCE(external_id, '') AS external_id,
			COALESCE(problem, '') AS problem
		FROM submit_logs
		WHERE user_id = ?
		  AND is_ac = true
		  AND problem_id IS NOT NULL AND problem_id <> 0
	`, userID).Scan(&rows).Error; err != nil {
		return err
	}
	for _, r := range rows {
		oldKeys := []string{
			model.ACProblemKey(r.Platform, r.ExternalID, r.Problem, nil),
			model.ACProblemKey(r.Platform, "", r.Problem, nil),
		}
		if err := PromoteUserACKeysToProblemID(ctx, db, userID, oldKeys, r.ProblemID); err != nil {
			return err
		}
	}
	return nil
}

// PromoteUserACKeysToProblemID 绑题后把 e:/n: 预聚合键升级为 p:{id}。
// 若 p: 已存在则合并 earliest first_ac_at 并删除旧键。
func PromoteUserACKeysToProblemID(ctx context.Context, db *gorm.DB, userID int64, oldKeys []string, problemID uint) error {
	if db == nil || userID <= 0 || problemID == 0 {
		return nil
	}
	newKey := fmt.Sprintf("p:%d", problemID)
	// 去重且排除已是 p: 的键
	seen := map[string]struct{}{}
	keys := make([]string, 0, len(oldKeys))
	for _, k := range oldKeys {
		k = strings.TrimSpace(k)
		if k == "" || k == newKey {
			continue
		}
		if _, ok := seen[k]; ok {
			continue
		}
		seen[k] = struct{}{}
		keys = append(keys, k)
	}
	if len(keys) == 0 {
		return nil
	}

	if db.Migrator().HasTable(&model.UserACProblem{}) {
		var rows []model.UserACProblem
		if err := db.WithContext(ctx).
			Where("user_id = ? AND problem_key IN ?", userID, keys).
			Find(&rows).Error; err != nil {
			return err
		}
		if len(rows) > 0 {
			var existing model.UserACProblem
			hasNew := db.WithContext(ctx).
				Where("user_id = ? AND problem_key = ?", userID, newKey).
				First(&existing).Error == nil

			firstAt := rows[0].FirstACAt
			platform := rows[0].Platform
			for _, r := range rows {
				if r.FirstACAt.Before(firstAt) {
					firstAt = r.FirstACAt
				}
				if platform == "" && r.Platform != "" {
					platform = r.Platform
				}
			}
			if hasNew && existing.FirstACAt.Before(firstAt) {
				firstAt = existing.FirstACAt
			}
			if hasNew && existing.Platform != "" {
				platform = existing.Platform
			}

			row := model.UserACProblem{
				UserID:     userID,
				ProblemKey: newKey,
				Platform:   platform,
				FirstACAt:  firstAt,
			}
			if err := db.WithContext(ctx).
				Clauses(clause.OnConflict{
					Columns: []clause.Column{{Name: "user_id"}, {Name: "problem_key"}},
					DoUpdates: clause.Assignments(map[string]interface{}{
						"first_ac_at": gorm.Expr(
							"CASE WHEN EXCLUDED.first_ac_at < user_ac_problems.first_ac_at THEN EXCLUDED.first_ac_at ELSE user_ac_problems.first_ac_at END",
						),
						"platform": gorm.Expr(
							"CASE WHEN user_ac_problems.platform = '' OR user_ac_problems.platform IS NULL THEN EXCLUDED.platform ELSE user_ac_problems.platform END",
						),
					}),
				}).
				Create(&row).Error; err != nil {
				return err
			}
			if err := db.WithContext(ctx).
				Where("user_id = ? AND problem_key IN ?", userID, keys).
				Delete(&model.UserACProblem{}).Error; err != nil {
				return err
			}
		}
	}

	if db.Migrator().HasTable(&model.UserACProblemDay{}) {
		var dayRows []model.UserACProblemDay
		if err := db.WithContext(ctx).
			Where("user_id = ? AND problem_key IN ?", userID, keys).
			Find(&dayRows).Error; err != nil {
			return err
		}
		for _, r := range dayRows {
			promoted := model.UserACProblemDay{
				UserID:     r.UserID,
				Day:        r.Day,
				ProblemKey: newKey,
				Platform:   r.Platform,
			}
			if err := db.WithContext(ctx).
				Clauses(clause.OnConflict{
					Columns:   []clause.Column{{Name: "user_id"}, {Name: "day"}, {Name: "problem_key"}},
					DoNothing: true,
				}).
				Create(&promoted).Error; err != nil {
				return err
			}
		}
		if len(dayRows) > 0 {
			if err := db.WithContext(ctx).
				Where("user_id = ? AND problem_key IN ?", userID, keys).
				Delete(&model.UserACProblemDay{}).Error; err != nil {
				return err
			}
		}
	}
	return nil
}

// DeletePlatformUserAC 换绑：删某用户某平台 AC 预聚合
func DeletePlatformUserAC(ctx context.Context, db *gorm.DB, userID int64, platform string) error {
	if db == nil || userID <= 0 || platform == "" {
		return nil
	}
	if db.Migrator().HasTable(&model.UserACProblem{}) {
		if err := db.WithContext(ctx).
			Where("user_id = ? AND platform = ?", userID, platform).
			Delete(&model.UserACProblem{}).Error; err != nil {
			return err
		}
	}
	if db.Migrator().HasTable(&model.UserACProblemDay{}) {
		if err := db.WithContext(ctx).
			Where("user_id = ? AND platform = ?", userID, platform).
			Delete(&model.UserACProblemDay{}).Error; err != nil {
			return err
		}
	}
	return nil
}

// DeleteUserPreagg 删除用户全部预聚合 + 账本（硬删账号时用）
func DeleteUserPreagg(ctx context.Context, db *gorm.DB, userId int64) error {
	if db == nil || userId <= 0 {
		return nil
	}
	if db.Migrator().HasTable(&model.DailyUserStat{}) {
		if err := db.WithContext(ctx).Where("user_id = ?", userId).Delete(&model.DailyUserStat{}).Error; err != nil {
			return err
		}
	}
	if db.Migrator().HasTable(&model.UserACProblem{}) {
		if err := db.WithContext(ctx).Where("user_id = ?", userId).Delete(&model.UserACProblem{}).Error; err != nil {
			return err
		}
	}
	if db.Migrator().HasTable(&model.UserACProblemDay{}) {
		if err := db.WithContext(ctx).Where("user_id = ?", userId).Delete(&model.UserACProblemDay{}).Error; err != nil {
			return err
		}
	}
	return DeleteUserCountedIDs(ctx, db, userId)
}

// BackfillUserACIfEmpty 空表时从 submit_logs 回填（启动幂等）
func BackfillUserACIfEmpty(db *gorm.DB) {
	if db == nil || !db.Migrator().HasTable(&model.UserACProblem{}) {
		return
	}
	var n int64
	if err := db.Model(&model.UserACProblem{}).Count(&n).Error; err != nil {
		log.Warnf("user_ac_problems count failed: %v", err)
		return
	}
	if n > 0 {
		return
	}
	log.Infof("user_ac_problems empty, backfill from submit_logs…")

	res1 := db.Exec(`
		INSERT INTO user_ac_problems (user_id, problem_key, platform, first_ac_at)
		SELECT user_id, problem_key, platform, MIN(time) AS first_ac_at
		FROM (
			SELECT
				user_id,
				time,
				COALESCE(NULLIF(btrim(platform), ''), '?') AS platform,
				COALESCE(
					CASE WHEN problem_id IS NOT NULL AND problem_id <> 0 THEN 'p:' || problem_id::text END,
					CASE WHEN external_id IS NOT NULL AND btrim(external_id) <> '' THEN 'e:' || platform || ':' || external_id END,
					'n:' || platform || ':' || COALESCE(problem, '')
				) AS problem_key
			FROM submit_logs
			WHERE is_ac = true
		) t
		GROUP BY user_id, problem_key, platform
		ON CONFLICT (user_id, problem_key) DO NOTHING
	`)
	if res1.Error != nil {
		log.Warnf("user_ac_problems backfill failed: %v", res1.Error)
	} else {
		log.Infof("user_ac_problems backfill rows=%d", res1.RowsAffected)
	}

	if !db.Migrator().HasTable(&model.UserACProblemDay{}) {
		return
	}
	var nd int64
	_ = db.Model(&model.UserACProblemDay{}).Count(&nd).Error
	if nd > 0 {
		return
	}
	log.Infof("user_ac_problem_days empty, backfill from submit_logs…")
	res2 := db.Exec(`
		INSERT INTO user_ac_problem_days (user_id, day, problem_key, platform)
		SELECT DISTINCT
			user_id,
			date_trunc('day', time)::date AS day,
			COALESCE(
				CASE WHEN problem_id IS NOT NULL AND problem_id <> 0 THEN 'p:' || problem_id::text END,
				CASE WHEN external_id IS NOT NULL AND btrim(external_id) <> '' THEN 'e:' || platform || ':' || external_id END,
				'n:' || platform || ':' || COALESCE(problem, '')
			) AS problem_key,
			COALESCE(NULLIF(btrim(platform), ''), '?') AS platform
		FROM submit_logs
		WHERE is_ac = true
		ON CONFLICT (user_id, day, problem_key) DO NOTHING
	`)
	if res2.Error != nil {
		log.Warnf("user_ac_problem_days backfill failed: %v", res2.Error)
		return
	}
	log.Infof("user_ac_problem_days backfill rows=%d", res2.RowsAffected)
}
