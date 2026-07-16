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
	// v2：禁止在「热表已无冷明细」时用 submit_logs 重建预聚合/账本（会毁掉冷统计并导致全量双计）
	SubmitRetentionMigrateVersion = "v2"
	submitRetentionDoneKey        = "submit_retention:" + SubmitRetentionMigrateVersion + "_done"
	submitRetentionLockKey        = "submit_retention:" + SubmitRetentionMigrateVersion + "_lock"
	// 历史 v1 完成标记：已剪过冷则绝不能再 destructive 重建
	submitRetentionV1DoneKey = "submit_retention:v1_done"
	// 稳定「热表-only 模式」标记：剪冷后预聚合/账本为真相；Purge 后也打此标，避免重启再重建
	submitRetentionHotOnlyKey = "submit_retention:hot_only"
)

// SubmitRetentionResult 清洗结果
type SubmitRetentionResult struct {
	Skipped      bool
	Rebuilt      bool // 是否从 submit_logs 重建了预聚合（仅冷明细仍在时）
	DailyRows    int64
	ACRows       int64
	ACDayRows    int64
	LedgerRows   int64
	DeletedLogs  int64
	Duration     time.Duration
}

// retentionShouldRebuildPreagg 仅当热表仍含冷明细时，才允许从 submit_logs 全量重建写死层。
// 冷已剪掉后 submit_logs 不是生涯真相，重建会抹掉 daily/user_ac/ledger 中的冷统计。
func retentionShouldRebuildPreagg(coldCount int64) bool {
	return coldCount > 0
}

// retentionAllowMarkDoneWithoutRebuild 无冷行时是否可安全 mark done（不重建）。
// hot>0 且 ledger=0：账本残缺，禁止 quiet 用热表重建，交给 Purge+全量重爬。
// 两边都空（新库/刚 Purge）：允许 mark，写路径会重新灌数。
func retentionAllowMarkDoneWithoutRebuild(hotN, ledgerN int64) error {
	if hotN > 0 && ledgerN == 0 {
		return fmt.Errorf(
			"refuse rebuild from hot-only submit_logs: hot=%d ledger=0 (use PurgeSubmitsAndRecrawl)",
			hotN,
		)
	}
	return nil
}

// RunSubmitRetentionMigrate 幂等清洗：
//   - 热表仍有冷行：从全量 submit_logs 重建预聚合+账本，再删冷明细
//   - 热表已无冷行：禁止 DELETE 预聚合/账本；仅校验后 mark done（或 prune no-op）
//
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
		if retentionRedisDone(ctx, rdb) {
			res.Skipped = true
			log.Infof("submit_retention: already done (redis), skip")
			return res, nil
		}
	}

	// 已完成 / 热表-only 模式：禁止 destructive 重建
	if !dryRun && hasRetentionMetaDone(db) {
		res.Skipped = true
		log.Infof("submit_retention: meta done/hot_only, skip")
		return res, nil
	}

	log.Infof("submit_retention migrate start version=%s dryRun=%v", SubmitRetentionMigrateVersion, dryRun)

	cutoff := model.SubmitLogHotCutoff(time.Now())
	var coldCount int64
	_ = db.WithContext(ctx).Model(&model.SubmitLog{}).Where("time < ?", cutoff).Count(&coldCount).Error

	if dryRun {
		res.DeletedLogs = coldCount
		res.Duration = time.Since(start)
		log.Infof("submit_retention dry-run: cold submit_logs=%d cutoff=%s rebuild=%v",
			coldCount, cutoff.Format(time.RFC3339), retentionShouldRebuildPreagg(coldCount))
		return res, nil
	}

	// 1) 确保 schema（platform 列 / 账本表）
	if err := ensureDailyPlatformSchema(db); err != nil {
		return nil, fmt.Errorf("ensure daily schema: %w", err)
	}
	if !db.Migrator().HasTable(&model.CountedSubmitID{}) {
		return nil, fmt.Errorf("counted_submit_ids table missing")
	}

	// 2) 已无冷行：绝不从热表重建写死层
	if !retentionShouldRebuildPreagg(coldCount) {
		var hotN, ledgerN int64
		_ = db.WithContext(ctx).Model(&model.SubmitLog{}).Count(&hotN).Error
		_ = db.WithContext(ctx).Model(&model.CountedSubmitID{}).Count(&ledgerN).Error
		if err := retentionAllowMarkDoneWithoutRebuild(hotN, ledgerN); err != nil {
			log.Errorf("submit_retention: %v", err)
			return nil, err
		}
		// 仍跑一遍 prune（应无删除）
		deleted, err := PruneColdSubmitLogs(ctx, db, time.Now(), 5000)
		if err != nil {
			return nil, fmt.Errorf("prune cold logs: %w", err)
		}
		res.DeletedLogs = deleted
		res.LedgerRows = ledgerN
		markRetentionHotOnly(db)
		markRetentionDone(db)
		if rdb != nil {
			_ = rdb.Set(ctx, submitRetentionDoneKey, "1", 0).Err()
			_ = rdb.Set(ctx, submitRetentionHotOnlyKey, "1", 0).Err()
		}
		res.Duration = time.Since(start)
		log.Infof("submit_retention migrate done version=%s mode=hot_only_no_rebuild hot=%d ledger=%d deleted=%d took=%s",
			SubmitRetentionMigrateVersion, hotN, ledgerN, res.DeletedLogs, res.Duration)
		return res, nil
	}

	// 3) 仍有冷明细：submit_logs 仍是生涯真相，允许一次性重建
	res.Rebuilt = true
	if err := rebuildPreaggFromSubmitLogs(ctx, db, res); err != nil {
		return nil, err
	}

	// 4) 校验：账本行数应接近 submit_logs
	var submitN, ledgerN int64
	_ = db.WithContext(ctx).Model(&model.SubmitLog{}).Count(&submitN).Error
	_ = db.WithContext(ctx).Model(&model.CountedSubmitID{}).Count(&ledgerN).Error
	if submitN > 0 && ledgerN == 0 {
		return nil, fmt.Errorf("ledger empty after backfill (submit_logs=%d)", submitN)
	}
	log.Infof("submit_retention: validate submit_logs=%d ledger=%d", submitN, ledgerN)

	// 5) 删除冷明细（仅在回填成功后）
	deleted, err := PruneColdSubmitLogs(ctx, db, time.Now(), 5000)
	if err != nil {
		return nil, fmt.Errorf("prune cold logs: %w", err)
	}
	res.DeletedLogs = deleted
	log.Infof("submit_retention: deleted cold submit_logs=%d", deleted)

	// 6) 标记完成 + 热表-only 模式（之后禁止再用热表重建）
	markRetentionHotOnly(db)
	markRetentionDone(db)
	if rdb != nil {
		_ = rdb.Set(ctx, submitRetentionDoneKey, "1", 0).Err()
		_ = rdb.Set(ctx, submitRetentionHotOnlyKey, "1", 0).Err()
	}

	res.Duration = time.Since(start)
	log.Infof("submit_retention migrate done version=%s rebuilt=true daily=%d ac=%d acDay=%d ledger=%d deleted=%d took=%s",
		SubmitRetentionMigrateVersion, res.DailyRows, res.ACRows, res.ACDayRows, res.LedgerRows, res.DeletedLogs, res.Duration)
	return res, nil
}

// rebuildPreaggFromSubmitLogs 从当前 submit_logs 全量覆盖写死层（仅冷明细仍在时调用）
func rebuildPreaggFromSubmitLogs(ctx context.Context, db *gorm.DB, res *SubmitRetentionResult) error {
	if err := db.WithContext(ctx).Exec(`DELETE FROM daily_user_stats`).Error; err != nil {
		return fmt.Errorf("truncate daily: %w", err)
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
		return fmt.Errorf("rebuild daily: %w", r1.Error)
	}
	res.DailyRows = r1.RowsAffected
	log.Infof("submit_retention: daily_user_stats rows=%d", res.DailyRows)

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
			return fmt.Errorf("rebuild user_ac_problems: %w", r2.Error)
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
			return fmt.Errorf("rebuild user_ac_problem_days: %w", r3.Error)
		}
		res.ACDayRows = r3.RowsAffected
		log.Infof("submit_retention: user_ac_problem_days rows=%d", res.ACDayRows)
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
		return fmt.Errorf("rebuild ledger: %w", r4.Error)
	}
	res.LedgerRows = r4.RowsAffected
	log.Infof("submit_retention: counted_submit_ids rows=%d", res.LedgerRows)
	return nil
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

func ensureRetentionMetaTable(db *gorm.DB) {
	_ = db.Exec(`CREATE TABLE IF NOT EXISTS submit_retention_meta (
		key text PRIMARY KEY,
		value text NOT NULL,
		updated_at timestamptz DEFAULT NOW()
	)`).Error
}

func retentionMetaValue(db *gorm.DB, key string) string {
	ensureRetentionMetaTable(db)
	var v string
	err := db.Raw(`SELECT value FROM submit_retention_meta WHERE key = ?`, key).Scan(&v).Error
	if err != nil {
		return ""
	}
	return v
}

func setRetentionMeta(db *gorm.DB, key, value string) {
	ensureRetentionMetaTable(db)
	_ = db.Exec(`
		INSERT INTO submit_retention_meta (key, value, updated_at) VALUES (?, ?, NOW())
		ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
	`, key, value).Error
}

func retentionRedisDone(ctx context.Context, rdb *redis.Client) bool {
	if rdb == nil {
		return false
	}
	for _, k := range []string{submitRetentionDoneKey, submitRetentionV1DoneKey, submitRetentionHotOnlyKey} {
		if v, err := rdb.Get(ctx, k).Result(); err == nil && v == "1" {
			return true
		}
	}
	return false
}

func hasRetentionMetaDone(db *gorm.DB) bool {
	// v2 done / v1 done / hot_only 任一成立则不再 destructive migrate
	for _, k := range []string{submitRetentionDoneKey, submitRetentionV1DoneKey, submitRetentionHotOnlyKey} {
		if retentionMetaValue(db, k) == "1" {
			return true
		}
	}
	return false
}

func markRetentionDone(db *gorm.DB) {
	setRetentionMeta(db, submitRetentionDoneKey, "1")
}

func markRetentionHotOnly(db *gorm.DB) {
	setRetentionMeta(db, submitRetentionHotOnlyKey, "1")
}

// MarkRetentionAfterPurge Purge 清空训练数据后调用：
// 标记 hot_only + done，避免服务重启时 migrate 用空/热表错误重建；
// 全量重爬写路径会重新灌 daily/user_ac/ledger。
func MarkRetentionAfterPurge(db *gorm.DB, rdb *redis.Client) {
	if db != nil {
		// 清掉旧锁标记，但立刻写入 hot_only/done，禁止 destructive 重建
		ensureRetentionMetaTable(db)
		_ = db.Exec(`DELETE FROM submit_retention_meta WHERE key LIKE 'submit_retention:%_lock'`).Error
		markRetentionHotOnly(db)
		markRetentionDone(db)
		// 兼容：也写 v1 done，防止旧二进制再跑 v1 重建
		setRetentionMeta(db, submitRetentionV1DoneKey, "1")
	}
	if rdb != nil {
		ctx := context.Background()
		_ = rdb.Del(ctx,
			"submit_retention:v1_lock",
			submitRetentionLockKey,
		).Err()
		_ = rdb.Set(ctx, submitRetentionDoneKey, "1", 0).Err()
		_ = rdb.Set(ctx, submitRetentionV1DoneKey, "1", 0).Err()
		_ = rdb.Set(ctx, submitRetentionHotOnlyKey, "1", 0).Err()
	}
}
