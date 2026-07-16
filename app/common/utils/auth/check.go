package auth

import (
	"context"
	"strings"

	_const "cwxu-algo/app/common/const"
	"cwxu-algo/app/common/permission"

	"github.com/go-kratos/kratos/v2/transport"
	"github.com/golang-jwt/jwt/v5"
)

// JwtPayload JWT 载荷
type JwtPayload struct {
	jwt.RegisteredClaims
	UserID      uint   `json:"userId"`
	Username    string `json:"username"`
	Name        string `json:"name"`
	Email       string `json:"email"`
	RoleID      int    `json:"roleId"` // 兼容旧字段
	IsSiteAdmin bool   `json:"isSiteAdmin"`
	OrgID       uint   `json:"orgId"`
	OrgRole     string `json:"orgRole"` // member | coach | captain | org_admin
}

func parseJWTToken(ctx context.Context) string {
	header, _ := transport.FromServerContext(ctx)
	if header == nil {
		return ""
	}
	auths := strings.Fields(header.RequestHeader().Get("Authorization"))
	if len(auths) != 2 || !strings.EqualFold(auths[0], "Bearer") {
		return ""
	}
	return auths[1]
}

func parsePayload(ctx context.Context) *JwtPayload {
	tokenString := parseJWTToken(ctx)
	if tokenString == "" {
		return nil
	}
	pd := &JwtPayload{}
	token, err := jwt.ParseWithClaims(
		tokenString,
		pd,
		func(token *jwt.Token) (interface{}, error) {
			if token.Method != jwt.SigningMethodHS256 {
				return nil, jwt.ErrSignatureInvalid
			}
			return []byte(_const.JWTSecret()), nil
		},
		jwt.WithValidMethods([]string{jwt.SigningMethodHS256.Alg()}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer("goalgo"),
		jwt.WithAudience("goalgo-web"),
	)
	if err != nil || !token.Valid || pd.UserID == 0 {
		return nil
	}
	// 兼容：旧 token 无 isSiteAdmin 时用 roleId==1
	if !pd.IsSiteAdmin && pd.RoleID == permission.RoleAdmin {
		pd.IsSiteAdmin = true
	}
	return pd
}

// VerifyMinRole 兼容旧权限序
func VerifyMinRole(ctx context.Context, minRole int) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	if pd.IsSiteAdmin {
		return true
	}
	// 组织 staff（教练/队长/组织管理员）≈ 旧教练级（管理端）
	if isOrgStaffRole(pd.OrgRole) && permission.RoleRank(minRole) <= permission.RoleRank(permission.RoleCoach) {
		return true
	}
	return permission.RoleRank(pd.RoleID) >= permission.RoleRank(minRole)
}

func isOrgStaffRole(role string) bool {
	return role == "coach" || role == "captain" || role == "org_admin"
}

// VerifySelfOrAbove 自己或站点管理员
func VerifySelfOrAbove(ctx context.Context, targetUserId uint) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	if pd.IsSiteAdmin || pd.RoleID == permission.RoleAdmin {
		return true
	}
	return pd.UserID == targetUserId
}

func GetCurrentUser(ctx context.Context) *JwtPayload {
	return parsePayload(ctx)
}

func GetCurrentUserId(ctx context.Context) uint {
	pd := parsePayload(ctx)
	if pd == nil {
		return 0
	}
	return pd.UserID
}

// VerifyAdmin / VerifySiteAdmin 站点管理员
func VerifyAdmin(ctx context.Context) bool {
	return VerifySiteAdmin(ctx)
}

func VerifySiteAdmin(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	return pd.IsSiteAdmin || pd.RoleID == permission.RoleAdmin
}

// VerifyOrgAdmin 当前 JWT 组织的组织管理员，或站点管理员
func VerifyOrgAdmin(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	if pd.IsSiteAdmin || pd.RoleID == permission.RoleAdmin {
		return true
	}
	return pd.OrgRole == "org_admin" && pd.OrgID > 0
}

// VerifyOrgAdminOf 指定组织的管理员（JWT org 匹配或站点管理员；业务层应再查库）
func VerifyOrgAdminOf(ctx context.Context, orgID uint) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	if pd.IsSiteAdmin || pd.RoleID == permission.RoleAdmin {
		return true
	}
	return pd.OrgRole == "org_admin" && pd.OrgID == orgID
}

func VerifyCoach(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	return pd.RoleID == permission.RoleCoach
}

// VerifyStaff 管理端：站点管理员 或 当前组织教练/队长/组织管理员 或 旧 staff role
func VerifyStaff(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	if pd.IsSiteAdmin || isOrgStaffRole(pd.OrgRole) {
		return true
	}
	return permission.IsStaff(pd.RoleID)
}

// VerifyOrgCoach 当前组织教练及以上（coach/captain/org_admin）或站点管理员
func VerifyOrgCoach(ctx context.Context) bool {
	return VerifyStaff(ctx)
}

func VerifyCaptain(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	if pd.IsSiteAdmin {
		return true
	}
	if pd.OrgRole == "captain" || pd.OrgRole == "org_admin" {
		return true
	}
	return pd.RoleID == permission.RoleCaptain
}
