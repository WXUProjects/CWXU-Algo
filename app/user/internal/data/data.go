package data

import (
	"context"
	"os"
	"strings"
	"time"

	"cwxu-algo/app/common/conf"
	gorm2 "cwxu-algo/app/common/data/gorm"
	redis2 "cwxu-algo/app/common/data/redis"
	"cwxu-algo/app/common/sitesettings"
	"cwxu-algo/app/user/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// siteSettingsRefreshInterval 定期把 site_configs 刷进 Redis，供 core_data/agent 读 SMTP。
// 与 sitesettings.RedisTTL 配合：即使缓存被误清/毒缓存被剔除，也会在数分钟内恢复。
const siteSettingsRefreshInterval = 3 * time.Minute

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData)

// Data .
type Data struct {
	DB     *gorm.DB
	CoreDB *gorm.DB // optional: algo_core_data for site backup
	RDB    *redis.Client
}

// NewData .
func NewData(c *conf.Data) (*Data, func(), error) {
	data := &Data{DB: gorm2.InitGorm(c), RDB: redis2.InitRedis(c)}
	if core := openCoreDB(c); core != nil {
		data.CoreDB = core
		log.Info("backup: core database connected")
	} else {
		log.Warn("backup: core database not configured; full site export/import of training data will fail")
	}
	migrateModels(data.DB)
	PublishSiteSettings(data)
	stopRefresh := startSiteSettingsRefresh(data)
	cleanup := func() {
		stopRefresh()
		log.Info("closing the data resources")
		sql, _ := data.DB.DB()
		sql.Close()
		if data.CoreDB != nil {
			if s, err := data.CoreDB.DB(); err == nil {
				_ = s.Close()
			}
		}
		data.RDB.Close()
	}
	return data, cleanup, nil
}

// openCoreDB connects to algo_core_data for backup.
// Priority: CWXU_CORE_DATABASE_SOURCE env → derive from user DSN (algo_user → algo_core_data).
func openCoreDB(c *conf.Data) *gorm.DB {
	src := strings.TrimSpace(os.Getenv("CWXU_CORE_DATABASE_SOURCE"))
	if src == "" && c != nil && c.Database != nil {
		u := c.Database.Source
		if strings.Contains(u, "dbname=algo_user") {
			src = strings.Replace(u, "dbname=algo_user", "dbname=algo_core_data", 1)
		}
	}
	if src == "" {
		return nil
	}
	db, err := gorm.Open(postgres.Open(src), &gorm.Config{
		Logger:      logger.Default.LogMode(logger.Warn),
		PrepareStmt: true,
	})
	if err != nil {
		log.Warnf("backup: open core database failed: %v", err)
		return nil
	}
	sqlDB, err := db.DB()
	if err != nil {
		log.Warnf("backup: core database pool: %v", err)
		return nil
	}
	sqlDB.SetMaxOpenConns(4)
	sqlDB.SetMaxIdleConns(2)
	if err := sqlDB.Ping(); err != nil {
		log.Warnf("backup: core database ping failed: %v", err)
		_ = sqlDB.Close()
		return nil
	}
	return db
}

// migrateModels 合并
func migrateModels(db *gorm.DB) {
	reconcileOrgJoinRequestDuplicates(db)
	err := db.AutoMigrate(
		&model.User{},
		&model.Group{},
		&model.SiteConfig{},
		&model.Org{},
		&model.OrgMember{},
		&model.OrgJoinRequest{},
		&model.PlanQuota{},
		&model.Paste{},
		&model.SiteVisitDaily{},
		&model.BackupJob{},
		&model.UserFollow{},
		&model.Notification{},
	)
	if err != nil {
		panic("数据库：数据库自动合并失败")
	}
	seedPlanQuotas(db)
	seedGoAlgoFramework(db)
	backfillLastLoginAt(db)
	ensureSiteInactiveDays(db)
}

// backfillLastLoginAt 避免上线瞬间全员被判休眠
func backfillLastLoginAt(db *gorm.DB) {
	if db == nil || !db.Migrator().HasColumn(&model.User{}, "last_login_at") {
		return
	}
	if err := db.Exec(`
		UPDATE users
		SET last_login_at = COALESCE(updated_at, created_at, NOW())
		WHERE last_login_at IS NULL
	`).Error; err != nil {
		log.Warnf("backfill last_login_at: %v", err)
	}
}

// ensureSiteInactiveDays 旧行补默认 14
func ensureSiteInactiveDays(db *gorm.DB) {
	if db == nil || !db.Migrator().HasColumn(&model.SiteConfig{}, "inactive_days") {
		return
	}
	if err := db.Exec(`
		UPDATE site_configs SET inactive_days = 14
		WHERE inactive_days IS NULL OR inactive_days <= 0
	`).Error; err != nil {
		log.Warnf("ensure inactive_days: %v", err)
	}
}

// reconcileOrgJoinRequestDuplicates prepares legacy data for the composite
// unique index. Older deployments allowed repeated applications; keep the most
// recently inserted row (highest id) and remove only older duplicates.
func reconcileOrgJoinRequestDuplicates(db *gorm.DB) {
	if db == nil || !db.Migrator().HasTable(&model.OrgJoinRequest{}) {
		return
	}
	result := db.Exec(`
		DELETE FROM org_join_requests
		WHERE id IN (
			SELECT id FROM (
				SELECT id,
					ROW_NUMBER() OVER (PARTITION BY org_id, user_id ORDER BY id DESC) AS duplicate_rank
				FROM org_join_requests
			) AS duplicate_rows
			WHERE duplicate_rank > 1
		)
	`)
	if result.Error != nil {
		panic("数据库：组织加入申请历史重复数据归并失败")
	}
	if result.RowsAffected > 0 {
		log.Warnf("database migration removed %d duplicate org join requests", result.RowsAffected)
	}
}

// PublishSiteSettings 将站点业务配置写入 Redis，供 agent/core_data 热读
func PublishSiteSettings(d *Data) {
	if d == nil || d.DB == nil || d.RDB == nil {
		return
	}
	rt, err := sitesettings.LoadFromDB(d.DB)
	if err != nil || rt == nil {
		return
	}
	if err := sitesettings.PublishRedis(context.Background(), d.RDB, rt); err != nil {
		log.Warnf("publish site settings: %v", err)
	}
}

// startSiteSettingsRefresh 后台定时回填 Redis；返回 stop 在 Data cleanup 时调用。
func startSiteSettingsRefresh(d *Data) func() {
	if d == nil || d.DB == nil || d.RDB == nil {
		return func() {}
	}
	stopCh := make(chan struct{})
	go func() {
		ticker := time.NewTicker(siteSettingsRefreshInterval)
		defer ticker.Stop()
		for {
			select {
			case <-stopCh:
				return
			case <-ticker.C:
				PublishSiteSettings(d)
			}
		}
	}()
	return func() { close(stopCh) }
}

// seedPlanQuotas 幂等写入默认套餐配额模板
func seedPlanQuotas(db *gorm.DB) {
	defaults := []model.PlanQuota{
		{Plan: "free", SeatLimit: 0, DailySyncPerUser: 4, AISummaryPerMonth: 5},
		{Plan: "team", SeatLimit: 50, DailySyncPerUser: 24, AISummaryPerMonth: 200},
		{Plan: "pro", SeatLimit: 200, DailySyncPerUser: 48, AISummaryPerMonth: 1000},
	}
	for _, p := range defaults {
		var n int64
		if db.Model(&model.PlanQuota{}).Where("plan = ?", p.Plan).Count(&n); n == 0 {
			_ = db.Create(&p).Error
		}
	}
}
