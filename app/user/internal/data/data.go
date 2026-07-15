package data

import (
	"context"

	"cwxu-algo/app/common/conf"
	gorm2 "cwxu-algo/app/common/data/gorm"
	redis2 "cwxu-algo/app/common/data/redis"
	"cwxu-algo/app/common/sitesettings"
	"cwxu-algo/app/user/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/google/wire"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// ProviderSet is data providers.
var ProviderSet = wire.NewSet(NewData)

// Data .
type Data struct {
	DB  *gorm.DB
	RDB *redis.Client
}

// NewData .
func NewData(c *conf.Data) (*Data, func(), error) {
	data := &Data{DB: gorm2.InitGorm(c), RDB: redis2.InitRedis(c)}
	migrateModels(data.DB)
	PublishSiteSettings(data)
	cleanup := func() {
		log.Info("closing the data resources")
		sql, _ := data.DB.DB()
		sql.Close()
		data.RDB.Close()
	}
	return data, cleanup, nil
}

// migrateModels 合并
func migrateModels(db *gorm.DB) {
	// 先清软删残留并去掉 deleted_at，再 AutoMigrate（模型已无软删除）
	purgeSoftDelete(db)
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
	)
	if err != nil {
		panic("数据库：数据库自动合并失败")
	}
	seedPlanQuotas(db)
	seedGoAlgoFramework(db)
}

// purgeSoftDelete 硬删除历史软删行并 drop deleted_at 列（幂等）
func purgeSoftDelete(db *gorm.DB) {
	tables := []string{
		"org_members",
		"org_join_requests",
		"pastes",
		"groups",
		"orgs",
		"users",
		"plan_quotas",
	}
	for _, t := range tables {
		if !db.Migrator().HasTable(t) {
			continue
		}
		if !db.Migrator().HasColumn(t, "deleted_at") {
			continue
		}
		_ = db.Exec("DELETE FROM " + t + " WHERE deleted_at IS NOT NULL").Error
		_ = db.Migrator().DropColumn(t, "deleted_at")
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
