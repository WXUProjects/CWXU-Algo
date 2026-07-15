package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"

	"cwxu-algo/app/common/permission"

	"github.com/go-kratos/kratos/v2/transport"
)

// JwtPayload JWT 载荷
type JwtPayload struct {
	UserID      uint   `json:"userId"`
	Username    string `json:"username"`
	Name        string `json:"name"`
	Email       string `json:"email"`
	RoleID      int    `json:"roleId"` // 兼容旧字段
	IsSiteAdmin bool   `json:"isSiteAdmin"`
	OrgID       uint   `json:"orgId"`
	OrgRole     string `json:"orgRole"` // member | org_admin
}

func praseJwtToken(ctx context.Context) string {
	header, _ := transport.FromServerContext(ctx)
	if header == nil {
		return ""
	}
	auths := strings.SplitN(header.RequestHeader().Get("Authorization"), " ", 2)
	if len(auths) < 2 {
		return ""
	}
	return auths[1]
}

func parsePayload(ctx context.Context) *JwtPayload {
	parts := strings.Split(praseJwtToken(ctx), ".")
	if len(parts) != 3 {
		return nil
	}
	payloadBase64 := parts[1]
	dstLen := base64.RawURLEncoding.DecodedLen(len(payloadBase64))
	dst := make([]byte, dstLen)
	_, err := base64.RawURLEncoding.Decode(dst, []byte(payloadBase64))
	if err != nil {
		return nil
	}
	pd := JwtPayload{}
	if err := json.Unmarshal(dst, &pd); err != nil {
		return nil
	}
	// 兼容：旧 token 无 isSiteAdmin 时用 roleId==1
	if !pd.IsSiteAdmin && pd.RoleID == permission.RoleAdmin {
		pd.IsSiteAdmin = true
	}
	return &pd
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
	// 组织管理员 ≈ 旧教练级（管理端）
	if pd.OrgRole == "org_admin" && permission.RoleRank(minRole) <= permission.RoleRank(permission.RoleCoach) {
		return true
	}
	return permission.RoleRank(pd.RoleID) >= permission.RoleRank(minRole)
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

// VerifyStaff 管理端：站点管理员 或 当前组织管理员 或 旧 staff role
func VerifyStaff(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	if pd.IsSiteAdmin || pd.OrgRole == "org_admin" {
		return true
	}
	return permission.IsStaff(pd.RoleID)
}

func VerifyCaptain(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	return pd.RoleID == permission.RoleCaptain
}
