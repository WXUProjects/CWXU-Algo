package dal

import (
	"context"
	"fmt"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	// SubmitRetentionMigrateVersion 线上清洗版本号（改逻辑时 bump）
	SubmitRetentionMigrateVersion = "v1"
	submitRetentionDoneKey        = "submit_retention:" + SubmitRetentionMigrateVersion + "_done"
	submitRetentionLockKey        = "submit_retention:" + SubmitRetentionMigrateVersion + "_lock"
)

// SubmitRetentionResult 清洗结果
type SubmitRetentionResult struct {
	Skipped      bool
	DailyRows    int64
	ACRows       int64
	ACDayRows    int64
	LedgerRows   int64
	DeletedLogs  int64
	Duration     time.Duration
}

// RunSubmitRetentionMigrate 幂等：回填写死层/账本 + 删除 6 个月外 submit_logs。
// dryRun=true 时只统计将删除行数，不写库不删。
// rdb 可为 nil（则仅用 DB 侧探测；多实例时建议传 Redis 锁）。
func RunSubmitRetentionMigrate(ctx context.Context, db *gorm.DB, rdb *redis.Client, dryRun bool) (*SubmitRetentionResult, error) {
	if db == nil {
		return nil, fmt.Errorf("db nil")
	}
	start := time.Now()
	res := &SubmitRetentionResult{}

	if !dryRun && rdb != nil {
		ok, err := rdb.SetNX(ctx, submitRetentionLockKey, "1", 2*time.Hour).Result()
		if err != nil {
			log.Warnf("submit_retention: redis lock error: %v", err)
		} else if !ok {
			res.Skipped = true
			log.Infof("submit_retention: another instance holds lock, skip")
			return res, nil
		} else {
			defer func() { _ = rdb.Del(context.Background(), submitRetentionLockKey).Err() }()
		}
		if v, err := rdb.Get(ctx, submitRetentionDoneKey).Result(); err == nil && v == "1" {
			res.Skipped = true
			log.Infof("submit_retention: already done (%s), skip", SubmitRetentionMigrateVersion)
			return res, nil
		}
	}

	// 已完成标记也可落在 DB（无 Redis 时）
	if !dryRun && hasRetentionMetaDone(db) {
		res.Skipped = true
		log.Infof("submit_retention: meta done, skip")
		return res, nil
	}

	log.Infof("submit_retention migrate start version=%s dryRun=%v", SubmitRetentionMigrateVersion, dryRun)

	if dryRun {
		cutoff := model.SubmitLogHotCutoff(time.Now())
		var n int64
		_ = db.WithContext(ctx).Model(&model.SubmitLog{}).Where("time < ?", cutoff).Count(&n).Error
		res.DeletedLogs = n
		res.Duration = time.Since(start)
		log.Infof("submit_retention dry-run: cold submit_logs=%d cutoff=%s", n, cutoff.Format(time.RFC3339))
		return res, nil
	}

	// 1) 确保 schema（platform 列 / 账本表）— 调用方应已 AutoMigrate
	if err := ensureDailyPlatformSchema(db); err != nil {
		return nil, fmt.Errorf("ensure daily schema: %w", err)
	}

	// 2) 从全量 submit_logs 重建日汇总（带 platform）
	if err := db.WithContext(ctx).Exec(`DELETE FROM daily_user_stats`).Error; err != nil {
		return nil, fmt.Errorf("truncate daily: %w", err)
	}
	r1 := db.WithContext(ctx).Exec(`
		INSERT INTO daily_user_stats (user_id, day, platform, submit_cnt, ac_cnt)
		SELECT
			user_id,
			date_trunc('day', time)::date AS day,
			COALESCE(NULLIF(btrim(platform), ''), '?') AS platform,
			COUNT(*) FILTER (WHERE ` + model.SQLExcludeLeetCodeNonSubmit + `) AS submit_cnt,
			COUNT(*) FILTER (WHERE is_ac = true) AS ac_cnt
		FROM submit_logs
		GROUP BY user_id, date_trunc('day', time)::date, COALESCE(NULLIF(btrim(platform), ''), '?')
		HAVING
			COUNT(*) FILTER (WHERE ` + model.SQLExcludeLeetCodeNonSubmit + `) > 0
			OR COUNT(*) FILTER (WHERE is_ac = true) > 0
	`)
	if r1.Error != nil {
		return nil, fmt.Errorf("rebuild daily: %w", r1.Error)
	}
	res.DailyRows = r1.RowsAffected
	log.Infof("submit_retention: daily_user_stats rows=%d", res.DailyRows)

	// 3) 重建 user_ac_*
	if db.Migrator().HasTable(&model.UserACProblem{}) {
		_ = db.WithContext(ctx).Exec(`DELETE FROM user_ac_problems`).Error
		r2 := db.WithContext(ctx).Exec(`
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
		`)
		if r2.Error != nil {
			return nil, fmt.Errorf("rebuild user_ac_problems: %w", r2.Error)
		}
		res.ACRows = r2.RowsAffected
		log.Infof("submit_retention: user_ac_problems rows=%d", res.ACRows)
	}
	if db.Migrator().HasTable(&model.UserACProblemDay{}) {
		_ = db.WithContext(ctx).Exec(`DELETE FROM user_ac_problem_days`).Error
		r3 := db.WithContext(ctx).Exec(`
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
		`)
		if r3.Error != nil {
			return nil, fmt.Errorf("rebuild user_ac_problem_days: %w", r3.Error)
		}
		res.ACDayRows = r3.RowsAffected
		log.Infof("submit_retention: user_ac_problem_days rows=%d", res.ACDayRows)
	}

	// 4) 账本全量
	if !db.Migrator().HasTable(&model.CountedSubmitID{}) {
		return nil, fmt.Errorf("counted_submit_ids table missing")
	}
	_ = db.WithContext(ctx).Exec(`DELETE FROM counted_submit_ids`).Error
	r4 := db.WithContext(ctx).Exec(`
		INSERT INTO counted_submit_ids (submit_id, user_id, platform, created_at)
		SELECT submit_id, user_id, COALESCE(NULLIF(btrim(platform), ''), '?'), NOW()
		FROM submit_logs
		WHERE submit_id IS NOT NULL AND submit_id <> ''
		ON CONFLICT (submit_id) DO NOTHING
	`)
	if r4.Error != nil {
		return nil, fmt.Errorf("rebuild ledger: %w", r4.Error)
	}
	res.LedgerRows = r4.RowsAffected
	log.Infof("submit_retention: counted_submit_ids rows=%d", res.LedgerRows)

	// 5) 校验：账本行数应接近 submit_logs
	var submitN, ledgerN int64
	_ = db.WithContext(ctx).Model(&model.SubmitLog{}).Count(&submitN).Error
	_ = db.WithContext(ctx).Model(&model.CountedSubmitID{}).Count(&ledgerN).Error
	if submitN > 0 && ledgerN == 0 {
		return nil, fmt.Errorf("ledger empty after backfill (submit_logs=%d)", submitN)
	}
	log.Infof("submit_retention: validate submit_logs=%d ledger=%d", submitN, ledgerN)

	// 6) 删除冷明细（仅在回填成功后）
	deleted, err := PruneColdSubmitLogs(ctx, db, time.Now(), 5000)
	if err != nil {
		return nil, fmt.Errorf("prune cold logs: %w", err)
	}
	res.DeletedLogs = deleted
	log.Infof("submit_retention: deleted cold submit_logs=%d", deleted)

	// 7) 标记完成
	markRetentionDone(db)
	if rdb != nil {
		_ = rdb.Set(ctx, submitRetentionDoneKey, "1", 0).Err()
	}

	res.Duration = time.Since(start)
	log.Infof("submit_retention migrate done version=%s daily=%d ac=%d acDay=%d ledger=%d deleted=%d took=%s",
		SubmitRetentionMigrateVersion, res.DailyRows, res.ACRows, res.ACDayRows, res.LedgerRows, res.DeletedLogs, res.Duration)
	return res, nil
}

func ensureDailyPlatformSchema(db *gorm.DB) error {
	if db == nil || !db.Migrator().HasTable(&model.DailyUserStat{}) {
		return nil
	}
	// 旧表无 platform：整表重建
	if !db.Migrator().HasColumn(&model.DailyUserStat{}, "Platform") &&
		!columnExists(db, "daily_user_stats", "platform") {
		log.Infof("submit_retention: rebuild daily_user_stats for platform PK")
		if err := db.Exec(`ALTER TABLE daily_user_stats RENAME TO daily_user_stats_pre_platform`).Error; err != nil {
			return err
		}
		if err := db.AutoMigrate(&model.DailyUserStat{}); err != nil {
			return err
		}
		_ = db.Exec(`DROP TABLE IF EXISTS daily_user_stats_pre_platform`).Error
	}
	// user_ac platform 列
	if db.Migrator().HasTable(&model.UserACProblem{}) && !columnExists(db, "user_ac_problems", "platform") {
		if err := db.Exec(`ALTER TABLE user_ac_problems ADD COLUMN IF NOT EXISTS platform varchar(64) NOT NULL DEFAULT ''`).Error; err != nil {
			return err
		}
	}
	if db.Migrator().HasTable(&model.UserACProblemDay{}) && !columnExists(db, "user_ac_problem_days", "platform") {
		if err := db.Exec(`ALTER TABLE user_ac_problem_days ADD COLUMN IF NOT EXISTS platform varchar(64) NOT NULL DEFAULT ''`).Error; err != nil {
			return err
		}
	}
	return nil
}

func columnExists(db *gorm.DB, table, col string) bool {
	var n int64
	err := db.Raw(`
		SELECT COUNT(*) FROM information_schema.columns
		WHERE table_name = ? AND column_name = ?
	`, table, col).Scan(&n).Error
	return err == nil && n > 0
}

func hasRetentionMetaDone(db *gorm.DB) bool {
	// 用简单 key-value：若存在 counted 且热表已无冷数据，且有标记表
	// 使用 schema_migrations 风格：临时用 counted 表注释不够，建轻量表
	if !db.Migrator().HasTable("submit_retention_meta") {
		_ = db.Exec(`CREATE TABLE IF NOT EXISTS submit_retention_meta (
			key text PRIMARY KEY,
			value text NOT NULL,
			updated_at timestamptz DEFAULT NOW()
		)`).Error
	}
	var v string
	err := db.Raw(`SELECT value FROM submit_retention_meta WHERE key = ?`, submitRetentionDoneKey).Scan(&v).Error
	return err == nil && v == "1"
}

func markRetentionDone(db *gorm.DB) {
	_ = db.Exec(`CREATE TABLE IF NOT EXISTS submit_retention_meta (
		key text PRIMARY KEY,
		value text NOT NULL,
		updated_at timestamptz DEFAULT NOW()
	)`).Error
	_ = db.Exec(`
		INSERT INTO submit_retention_meta (key, value, updated_at) VALUES (?, '1', NOW())
		ON CONFLICT (key) DO UPDATE SET value = '1', updated_at = NOW()
	`, submitRetentionDoneKey).Error
}
