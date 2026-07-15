package data

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
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
			SeatLimit:            50,
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

	// 历史 seat_limit=0（旧语义未限制）→ 默认 50
	_ = db.Model(&model.Org{}).Where("seat_limit <= 0 OR seat_limit IS NULL").Update("seat_limit", 50).Error

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
			display := strings.TrimSpace(u.Name)
			if display == "" {
				display = u.Username
			}
			m := model.OrgMember{
				OrgID:          public.ID,
				UserID:         u.ID,
				Role:           model.OrgRoleMember,
				OrgDisplayName: display,
				JoinedAt:       now,
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
			IsSiteAdmin:        true,
			CurrentOrgID:       public.ID,
			EmailEnabled:       false,
			EmailWeeklyEnabled: false,
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

	// 6. 无有效 group_id 的用户 → 挂到其 current_org 的默认分组，否则公共域默认分组
	var pubDef model.Group
	if db.Where("org_id = ? AND name IN ?", public.ID, []string{model.DefaultGroupName, "未分组"}).
		Order("id ASC").First(&pubDef).Error != nil {
		defName := model.DefaultGroupName
		pubDef = model.Group{Name: &defName, Describe: model.DefaultGroupDesc, OrgID: public.ID}
		_ = db.Create(&pubDef).Error
	} else if pubDef.Name != nil && *pubDef.Name == "未分组" {
		n := model.DefaultGroupName
		_ = db.Model(&pubDef).Updates(map[string]interface{}{"name": n, "describe": model.DefaultGroupDesc}).Error
	}

	// 按用户 current_org 修正无效 group_id
	type ug struct {
		ID           uint
		GroupID      int64
		CurrentOrgID uint
	}
	var bad []ug
	_ = db.Raw(`
		SELECT u.id, COALESCE(u.group_id, 0) AS group_id, COALESCE(u.current_org_id, 0) AS current_org_id
		FROM users u
		WHERE u.deleted_at IS NULL AND (
			u.group_id IS NULL OR u.group_id = 0
			OR u.group_id NOT IN (SELECT id FROM groups WHERE deleted_at IS NULL)
		)
	`).Scan(&bad).Error
	for _, u := range bad {
		orgID := u.CurrentOrgID
		if orgID == 0 {
			orgID = public.ID
		}
		var defG model.Group
		if db.Where("org_id = ? AND name IN ?", orgID, []string{model.DefaultGroupName, "未分组"}).
			Order("id ASC").First(&defG).Error != nil {
			defName := model.DefaultGroupName
			defG = model.Group{Name: &defName, Describe: model.DefaultGroupDesc, OrgID: orgID}
			if db.Create(&defG).Error != nil {
				defG.ID = pubDef.ID
			}
		}
		if defG.ID > 0 {
			_ = db.Model(&model.User{}).Where("id = ?", u.ID).Update("group_id", defG.ID).Error
		}
	}

	// 删除历史虚拟「未分组」若存在 id=0 脏数据（一般无此行）
	_ = db.Where("name = ? AND id = 0", "未分组").Delete(&model.Group{}).Error

	// 7. 全站默认关闭个人日报/周报（迁移）
	_ = db.Model(&model.User{}).Where("1 = 1").Updates(map[string]interface{}{
		"email_enabled":        false,
		"email_weekly_enabled": false,
	}).Error
	// 组织周报开关：新列默认 true（GORM AutoMigrate）；若列为 false 且从未设置过可保持

	// 8. 姓名语义迁移：users.name → 全局昵称(=username)；旧真实姓名 → 唯一非公共域的组织内名称
	// 幂等：仅当 org_display_name 为空且 name≠username 时拷贝；之后 name 统一为 username
	migrateNameToOrgDisplay(db, public.ID)

	// 9. 公共域称呼 ≡ 全局昵称：空 org_display_name 回填 users.name
	syncPublicOrgDisplayFromName(db, public.ID)

	log.Infof("GoAlgo framework seed done public_org_id=%d users=%d fixed_groups=%d", public.ID, len(users), len(bad))
}

// syncPublicOrgDisplayFromName 将公共域空称呼回填为 users.name（与全局昵称合并）
func syncPublicOrgDisplayFromName(db *gorm.DB, publicOrgID uint) {
	res := db.Exec(`
		UPDATE org_members m
		SET org_display_name = u.name
		FROM users u
		WHERE m.user_id = u.id
		  AND m.org_id = ?
		  AND m.deleted_at IS NULL
		  AND u.deleted_at IS NULL
		  AND (m.org_display_name IS NULL OR TRIM(m.org_display_name) = '')
		  AND COALESCE(TRIM(u.name), '') <> ''
	`, publicOrgID)
	n := int64(0)
	if res != nil {
		n = res.RowsAffected
	}
	log.Infof("sync public org_display from name: updated=%d", n)
}

// migrateNameToOrgDisplay 一次性迁移：
// - 用户有且仅有 1 个非公共域 membership 且 org_display_name 为空 → 写入旧 users.name
// - 全部 users.name 改为 username（全局昵称）
func migrateNameToOrgDisplay(db *gorm.DB, publicOrgID uint) {
	type row struct {
		UserID uint
		Name   string
		Uname  string
		Cnt    int64
	}
	var rows []row
	// 每个用户非公共域 membership 数量
	_ = db.Raw(`
		SELECT u.id AS user_id, COALESCE(u.name,'') AS name, COALESCE(u.username,'') AS uname,
			(SELECT COUNT(*) FROM org_members m
			 JOIN orgs o ON o.id = m.org_id AND o.deleted_at IS NULL
			 WHERE m.user_id = u.id AND m.deleted_at IS NULL
			   AND o.id <> ? AND COALESCE(o.is_system,false) = false) AS cnt
		FROM users u
		WHERE u.deleted_at IS NULL
	`, publicOrgID).Scan(&rows).Error

	copied := 0
	for _, r := range rows {
		if r.Cnt != 1 {
			continue
		}
		oldName := strings.TrimSpace(r.Name)
		if oldName == "" || oldName == r.Uname {
			continue
		}
		// 找到那一条非公共域 membership
		var m model.OrgMember
		err := db.Table("org_members AS m").
			Joins("JOIN orgs o ON o.id = m.org_id AND o.deleted_at IS NULL").
			Where("m.user_id = ? AND m.deleted_at IS NULL AND o.id <> ? AND COALESCE(o.is_system,false) = false",
				r.UserID, publicOrgID).
			Select("m.*").
			First(&m).Error
		if err != nil {
			continue
		}
		if strings.TrimSpace(m.OrgDisplayName) != "" {
			continue
		}
		if db.Model(&model.OrgMember{}).Where("id = ?", m.ID).
			Update("org_display_name", oldName).Error == nil {
			copied++
		}
	}

	// 全局昵称 = 用户名（覆盖旧真实姓名）
	res := db.Exec(`UPDATE users SET name = username WHERE deleted_at IS NULL AND (name IS DISTINCT FROM username)`)
	nChanged := int64(0)
	if res != nil {
		nChanged = res.RowsAffected
	}
	log.Infof("migrate name→org_display: copied=%d name_to_username=%d", copied, nChanged)
}

// EnsurePublicOrgID 供业务使用
func EnsurePublicOrgID(db *gorm.DB) (uint, error) {
	var o model.Org
	if err := db.Where("slug = ?", model.PublicOrgSlug).First(&o).Error; err != nil {
		return 0, fmt.Errorf("public org missing: %w", err)
	}
	return o.ID, nil
}
