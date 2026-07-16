package service

import (
	"time"

	_const "cwxu-algo/app/common/const"
	"cwxu-algo/app/user/internal/data/model"

	"github.com/golang-jwt/jwt/v5"
	"gorm.io/gorm"
)

// IssueJWT 签发含组织上下文的 JWT
func IssueJWT(db *gorm.DB, u *model.User) (string, error) {
	orgID := u.CurrentOrgID
	orgRole := ""
	if orgID == 0 {
		var pub model.Org
		if err := db.Where("slug = ?", model.PublicOrgSlug).First(&pub).Error; err == nil {
			orgID = pub.ID
		}
	}
	if orgID > 0 {
		var m model.OrgMember
		if err := db.Where("org_id = ? AND user_id = ?", orgID, u.ID).First(&m).Error; err == nil {
			orgRole = m.Role
		} else {
			// 当前组织无效 → 回落公共域
			var pub model.Org
			if err := db.Where("slug = ?", model.PublicOrgSlug).First(&pub).Error; err == nil {
				orgID = pub.ID
				_ = db.Model(u).Update("current_org_id", orgID)
				var m2 model.OrgMember
				if err := db.Where("org_id = ? AND user_id = ?", orgID, u.ID).First(&m2).Error; err == nil {
					orgRole = m2.Role
				}
			}
		}
	}

	roleID := u.RoleID
	if u.IsSiteAdmin {
		roleID = 1
	} else if roleID == 1 {
		// 非站点管理员不应保留 admin roleId 语义
		roleID = 0
	}

	roleIdsJSON := []byte("[0]")
	if u.IsSiteAdmin || model.IsOrgStaffRole(orgRole) {
		roleIdsJSON = []byte("[1]")
	}

	// 短期访问令牌；前端活跃时通过 refresh 从数据库重签，以同步权限变更。
	now := time.Now()
	expire := now.Add(2 * time.Hour)
	return jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"userId":      u.ID,
		"username":    u.Username,
		"name":        u.Name,
		"roleId":      roleID,
		"roleIds":     string(roleIdsJSON),
		"isSiteAdmin": u.IsSiteAdmin,
		"orgId":       orgID,
		"orgRole":     orgRole,
		"exp":         expire.Unix(),
		"nbf":         now.Unix(),
		"iat":         now.Unix(),
		"iss":         "goalgo",
		"aud":         "goalgo-web",
	}).SignedString([]byte(_const.JWTSecret()))
}
