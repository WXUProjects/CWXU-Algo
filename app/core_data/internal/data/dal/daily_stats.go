package dal

import (
	"context"
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

// FilterNewSubmitLogs 去掉已存在 submit_id，返回待插入子集（用于准确维护日汇总）
func FilterNewSubmitLogs(ctx context.Context, db *gorm.DB, logs []model.SubmitLog) ([]model.SubmitLog, error) {
	if len(logs) == 0 {
		return nil, nil
	}
	ids := make([]string, len(logs))
	for i := range logs {
		ids[i] = logs[i].SubmitID
	}
	// 分批 IN，避免超长参数
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
		if _, ok := exist[logs[i].SubmitID]; ok {
			continue
		}
		out = append(out, logs[i])
	}
	return out, nil
}
