package data

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"cwxu-algo/app/user/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

func randomInviteCode() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

func sha256Hex(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

// seedGoAlgoFramework 公共域、全员入域、admin、站点标题、旧 role 迁移
func seedGoAlgoFramework(db *gorm.DB) {
	// 1. 公共域
	var public model.Org
	err := db.Where("slug = ?", model.PublicOrgSlug).First(&public).Error
	if err == gorm.ErrRecordNotFound {
		public = model.Org{
			Name:                 model.PublicOrgName,
			Slug:                 model.PublicOrgSlug,
			Plan:                 "free",
			Status:               model.OrgStatusActive,
			IsSystem:             true,
			JoinMode:             model.OrgJoinAuto,
			InviteCode:           "PUBLIC-" + randomInviteCode(),
			EnableAISummary:      true,
			EnableAIEmail:        true,
			EnableSpider:         true,
			SpiderIntervalMin:    60,
			AISummaryIntervalMin: 180,
			AIEmailSchedule:      "30 7 * * *",
		}
		if e := db.Create(&public).Error; e != nil {
			log.Errorf("seed public org: %v", e)
			return
		}
		log.Infof("seeded system org 公共域 id=%d", public.ID)
	} else if err != nil {
		log.Errorf("query public org: %v", err)
		return
	}

	// 各组织确保有「默认分组」；旧名「未分组」改名
	_ = db.Model(&model.Group{}).Where("name = ?", "未分组").Updates(map[string]interface{}{
		"name":     model.DefaultGroupName,
		"describe": model.DefaultGroupDesc,
	}).Error

	var allOrgs []model.Org
	if e := db.Find(&allOrgs).Error; e == nil {
		for _, o := range allOrgs {
			var defG model.Group
			err := db.Where("org_id = ? AND name = ?", o.ID, model.DefaultGroupName).
				Order("id ASC").First(&defG).Error
			if err != nil {
				defName := model.DefaultGroupName
				defG = model.Group{
					Name:     &defName,
					Describe: model.DefaultGroupDesc,
					OrgID:    o.ID,
				}
				if db.Create(&defG).Error != nil {
					continue
				}
			}
			// 该组织内 group_id=0 或无效的用户，挂到默认分组
			// 仅当用户 current_org 属于本组织，或（公共域）全员无组时
			if o.IsSystem {
				_ = db.Model(&model.User{}).
					Where("group_id = 0 OR group_id IS NULL").
					Update("group_id", defG.ID).Error
			}
		}
	}

	// 2. 旧 group 挂到公共域
	_ = db.Model(&model.Group{}).Where("org_id = 0 OR org_id IS NULL").Update("org_id", public.ID).Error

	// 3. 用户：is_site_admin 从 role_id=1；role 2/3 降为 0；全员入公共域
	_ = db.Model(&model.User{}).Where("role_id = ?", 1).Update("is_site_admin", true).Error
	_ = db.Model(&model.User{}).Where("role_id IN ?", []int{2, 3}).Update("role_id", 0).Error

	var users []model.User
	if e := db.Find(&users).Error; e != nil {
		log.Errorf("list users for org migrate: %v", e)
		return
	}
	now := time.Now()
	for _, u := range users {
		var n int64
		db.Model(&model.OrgMember{}).Where("org_id = ? AND user_id = ?", public.ID, u.ID).Count(&n)
		if n == 0 {
			m := model.OrgMember{
				OrgID:    public.ID,
				UserID:   u.ID,
				Role:     model.OrgRoleMember,
				JoinedAt: now,
			}
			_ = db.Create(&m).Error
		}
		if u.CurrentOrgID == 0 {
			_ = db.Model(&u).Update("current_org_id", public.ID).Error
		}
	}

	// 4. admin 账户
	var adminCount int64
	db.Model(&model.User{}).Where("username = ?", "admin").Count(&adminCount)
	if adminCount == 0 {
		admin := model.User{
			Username:     "admin",
			Password:     sha256Hex("admin"),
			Name:         "站点管理员",
			Email:        "admin@goalgo.local",
			RoleID:       1,
			IsSiteAdmin:  true,
			CurrentOrgID: public.ID,
			EmailEnabled: true,
		}
		if e := db.Create(&admin).Error; e != nil {
			log.Errorf("seed admin user: %v", e)
		} else {
			_ = db.Create(&model.OrgMember{
				OrgID:    public.ID,
				UserID:   admin.ID,
				Role:     model.OrgRoleMember,
				JoinedAt: now,
			}).Error
			log.Warnf("seeded default admin/admin — change password in production")
		}
	} else {
		_ = db.Model(&model.User{}).Where("username = ?", "admin").Updates(map[string]interface{}{
			"is_site_admin": true,
			"role_id":       1,
		}).Error
	}

	// 5. 站点标题默认 GoAlgo
	var sc model.SiteConfig
	if e := db.First(&sc, 1).Error; e == gorm.ErrRecordNotFound {
		_ = db.Create(&model.SiteConfig{ID: 1, SiteTitle: "GoAlgo"}).Error
	} else if e == nil && (sc.SiteTitle == "" || sc.SiteTitle == "Algo-CWUX") {
		_ = db.Model(&model.SiteConfig{}).Where("id = ?", 1).Update("site_title", "GoAlgo").Error
	}

	// 6. 再扫一遍：无有效分组的用户 → 当前组织或公共域的默认分组
	var pubDef model.Group
	if db.Where("org_id = ? AND name = ?", public.ID, model.DefaultGroupName).
		Order("id ASC").First(&pubDef).Error == nil {
		// group_id 指向已删除分组的用户
		_ = db.Exec(`
			UPDATE users SET group_id = ?
			WHERE deleted_at IS NULL AND (
				group_id = 0 OR group_id IS NULL
				OR group_id NOT IN (SELECT id FROM groups WHERE deleted_at IS NULL)
			)
		`, pubDef.ID).Error
	}

	log.Infof("GoAlgo framework seed done public_org_id=%d users=%d", public.ID, len(users))
}

// EnsurePublicOrgID 供业务使用
func EnsurePublicOrgID(db *gorm.DB) (uint, error) {
	var o model.Org
	if err := db.Where("slug = ?", model.PublicOrgSlug).First(&o).Error; err != nil {
		return 0, fmt.Errorf("public org missing: %w", err)
	}
	return o.ID, nil
}
