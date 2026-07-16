package data

import (
	"cwxu-algo/app/common/conf"
	gorm2 "cwxu-algo/app/common/data/gorm"
	redis2 "cwxu-algo/app/common/data/redis"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spidermetrics"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData, NewDataDB, NewDataRDB)

// NewDataDB 从 Data 中提取 DB
func NewDataDB(data *Data) *gorm.DB {
	return data.DB
}

// NewDataRDB 从 Data 中提取 RDB
func NewDataRDB(data *Data) *redis.Client {
	return data.RDB
}

// Data .
type Data struct {
	DB  *gorm.DB
	RDB *redis.Client
}

// NewData .
func NewData(c *conf.Data) (*Data, func(), error) {
	data := &Data{DB: gorm2.InitGorm(c), RDB: redis2.InitRedis(c)}
	migrateModels(data.DB)
	spidermetrics.BindRedis(data.RDB)
	cleanup := func() {
		log.Info("closing the data resources")
		sql, _ := data.DB.DB()
		sql.Close()
	}
	return data, cleanup, nil
}

// migrateModels 合并
func migrateModels(db *gorm.DB) {
	reconcilePlatformDuplicates(db)
	// 旧 daily_user_stats 无 platform：先改名，再 AutoMigrate 建新表
	prepareDailyUserStatsPlatform(db)
	err := db.AutoMigrate(
		&model.SubmitLog{},
		&model.Platform{},
		&model.ContestLog{},
		&model.Bulletin{},
		&model.Problem{},
		&model.ProblemEditRequest{},
		&model.EmergencyNotice{},
		&model.DailyUserStat{},
		&model.UserACProblem{},
		&model.UserACProblemDay{},
		&model.CountedSubmitID{},
		&model.ContestCalendar{},
		&model.ContestCalendarSub{},
		&model.ContestCalendarNotifyLog{},
	)
	if err != nil {
		panic("数据库：数据库自动合并失败")
	}
	// 丢弃旧无 platform 日汇总（清洗任务会从 submit_logs 全量重建）
	_ = db.Exec(`DROP TABLE IF EXISTS daily_user_stats_pre_platform`).Error
	ensureSubmitLogPerf(db)
	// 空表兜底回填（清洗任务会覆盖重建；新环境无历史时有用）
	backfillDailyUserStatsIfEmpty(db)
	backfillUserACIfEmpty(db)
}

// prepareDailyUserStatsPlatform 旧 PK (user_id,day) → 新 PK (user_id,day,platform)
func prepareDailyUserStatsPlatform(db *gorm.DB) {
	if db == nil || !db.Migrator().HasTable("daily_user_stats") {
		return
	}
	var n int64
	_ = db.Raw(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = 'daily_user_stats' AND column_name = 'platform'
	`).Scan(&n).Error
	if n > 0 {
		return
	}
	log.Infof("database: renaming daily_user_stats → daily_user_stats_pre_platform (add platform PK)")
	if err := db.Exec(`ALTER TABLE daily_user_stats RENAME TO daily_user_stats_pre_platform`).Error; err != nil {
		log.Warnf("database: rename daily_user_stats failed: %v", err)
	}
}

// backfillDailyUserStatsIfEmpty 空表时从 submit_logs 全量聚合（含 platform）
func backfillDailyUserStatsIfEmpty(db *gorm.DB) {
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

// backfillUserACIfEmpty 生涯/按日 AC 去重表空则从明细回填
func backfillUserACIfEmpty(db *gorm.DB) {
	if db == nil || !db.Migrator().HasTable(&model.UserACProblem{}) {
		return
	}
	var n int64
	if err := db.Model(&model.UserACProblem{}).Count(&n).Error; err != nil {
		log.Warnf("user_ac_problems count failed: %v", err)
		return
	}
	if n == 0 {
		log.Infof("user_ac_problems empty, backfill from submit_logs…")
		res := db.Exec(`
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
		if res.Error != nil {
			log.Warnf("user_ac_problems backfill failed: %v", res.Error)
		} else {
			log.Infof("user_ac_problems backfill rows=%d", res.RowsAffected)
		}
	}

	if !db.Migrator().HasTable(&model.UserACProblemDay{}) {
		return
	}
	var nd int64
	if err := db.Model(&model.UserACProblemDay{}).Count(&nd).Error; err != nil {
		return
	}
	if nd > 0 {
		return
	}
	log.Infof("user_ac_problem_days empty, backfill from submit_logs…")
	res := db.Exec(`
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
	if res.Error != nil {
		log.Warnf("user_ac_problem_days backfill failed: %v", res.Error)
		return
	}
	log.Infof("user_ac_problem_days backfill rows=%d", res.RowsAffected)
}

// ensureSubmitLogPerf 回填 is_ac + 补性能索引（10w+ 提交 / 日增 2w 场景）
// 幂等：可重复执行；启动时同步建索引（数据量尚小时可接受）。
func ensureSubmitLogPerf(db *gorm.DB) {
	if db == nil || !db.Migrator().HasTable(&model.SubmitLog{}) {
		return
	}
	// 历史行回填 is_ac（仅 false → true，可重复）
	res := db.Exec(`
		UPDATE submit_logs
		SET is_ac = true
		WHERE is_ac = false
		  AND UPPER(BTRIM(status)) IN ('AC', 'OK', 'ACCEPTED', '正确', '答案正确')
	`)
	if res.Error != nil {
		log.Warnf("submit_logs is_ac backfill failed: %v", res.Error)
	} else if res.RowsAffected > 0 {
		log.Infof("submit_logs is_ac backfill rows=%d", res.RowsAffected)
	}

	indexSQLs := []string{
		`CREATE INDEX IF NOT EXISTS idx_submit_user_isac_time ON submit_logs (user_id, is_ac, time DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_submit_isac_time ON submit_logs (time DESC) WHERE is_ac = true`,
		`CREATE INDEX IF NOT EXISTS idx_submit_user_time_nonsynthetic ON submit_logs (user_id, time DESC)
			WHERE ` + model.SQLExcludeLeetCodeNonSubmit,
		`CREATE INDEX IF NOT EXISTS idx_submit_time_nonsynthetic ON submit_logs (time DESC)
			WHERE ` + model.SQLExcludeLeetCodeNonSubmit,
		`CREATE INDEX IF NOT EXISTS idx_daily_stats_day ON daily_user_stats (day)`,
		`CREATE INDEX IF NOT EXISTS idx_daily_stats_day_user ON daily_user_stats (day, user_id)`,
		`CREATE INDEX IF NOT EXISTS idx_daily_stats_user_plat ON daily_user_stats (user_id, platform)`,
		`CREATE INDEX IF NOT EXISTS idx_uac_day_user ON user_ac_problem_days (user_id, day)`,
		`CREATE INDEX IF NOT EXISTS idx_uac_user_first ON user_ac_problems (user_id, first_ac_at)`,
		`CREATE INDEX IF NOT EXISTS idx_uac_user_plat ON user_ac_problems (user_id, platform)`,
		`CREATE INDEX IF NOT EXISTS idx_uac_day_plat ON user_ac_problem_days (user_id, platform)`,
		`CREATE INDEX IF NOT EXISTS idx_counted_user_plat ON counted_submit_ids (user_id, platform)`,
	}
	for _, sql := range indexSQLs {
		if err := db.Exec(sql).Error; err != nil {
			log.Warnf("submit_logs index ensure failed: %v sql=%s", err, sql)
		}
	}
}

// reconcilePlatformDuplicates prepares historical bindings for the new
// (user_id, platform) unique index. Submission and contest rows reference that
// natural key rather than platforms.id, so retaining the newest binding is safe.
func reconcilePlatformDuplicates(db *gorm.DB) {
	if db == nil || !db.Migrator().HasTable(&model.Platform{}) {
		return
	}
	result := db.Exec(`
		DELETE FROM platforms
		WHERE id IN (
			SELECT id FROM (
				SELECT id,
					ROW_NUMBER() OVER (PARTITION BY user_id, platform ORDER BY id DESC) AS duplicate_rank
				FROM platforms
			) AS duplicate_rows
			WHERE duplicate_rank > 1
		)
	`)
	if result.Error != nil {
		panic("数据库：OJ 绑定历史重复数据归并失败")
	}
	if result.RowsAffected > 0 {
		log.Warnf("database migration removed %d duplicate platform bindings", result.RowsAffected)
	}
}
