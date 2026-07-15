package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"

	"cwxu-algo/app/common/permission"

	"github.com/go-kratos/kratos/v2/transport"
)

// JwtPayload JWT载荷结构体
type JwtPayload struct {
	UserID   uint   `json:"userId"`   // 用户ID
	Username string `json:"username"` // 用户名
	Name     string `json:"name"`     // 姓名
	Email    string `json:"email"`    // 邮箱
	RoleID   int    `json:"roleId"`   // 0队员 1管理员 2教练 3队长
	// 商业化预留：当前签发为 0，解析端兼容缺失字段
	OrgID   uint   `json:"orgId,omitempty"`   // 当前组织 ID
	OrgRole string `json:"orgRole,omitempty"` // owner|coach|member
}

func praseJwtToken(ctx context.Context) string {
	header, ok := transport.FromServerContext(ctx)
	if !ok || header == nil {
		return ""
	}
	reqHeader := header.RequestHeader()
	if reqHeader == nil {
		return ""
	}
	auths := strings.SplitN(reqHeader.Get("Authorization"), " ", 2)
	if len(auths) < 2 {
		return ""
	}
	return auths[1]
}

func parsePayload(ctx context.Context) *JwtPayload {
	token := praseJwtToken(ctx)
	if token == "" {
		return nil
	}
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil
	}
	payloadBase64 := parts[1]
	dst, err := base64.RawURLEncoding.DecodeString(payloadBase64)
	if err != nil {
		// 兼容带 padding 的 base64
		dst, err = base64.URLEncoding.DecodeString(payloadBase64)
		if err != nil {
			return nil
		}
	}
	pd := JwtPayload{}
	if err := json.Unmarshal(dst, &pd); err != nil {
		return nil
	}
	return &pd
}

// VerifyMinRole 校验调用者权限是否不低于 minRole
// 注意：RoleID 数值大小 ≠ 权限高低，须用 permission.RoleRank。
// 例如：VerifyMinRole(ctx, permission.RoleCoach) → 管理员 / 教练 / 队长均通过
func VerifyMinRole(ctx context.Context, minRole int) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	return permission.RoleRank(pd.RoleID) >= permission.RoleRank(minRole)
}

// VerifySelfOrAbove 校验调用者是否能操作目标用户资料类接口
// - 管理员：可操作任何人
// - 教练 / 队长：仅自己（管理他人走专用管理接口）
// - 队员：仅自己
func VerifySelfOrAbove(ctx context.Context, targetUserId uint) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	if pd.RoleID == permission.RoleAdmin {
		return true
	}
	return pd.UserID == targetUserId
}

// GetCurrentUser 获取当前登录用户信息
func GetCurrentUser(ctx context.Context) *JwtPayload {
	return parsePayload(ctx)
}

// GetCurrentUserId 获取当前登录用户ID
func GetCurrentUserId(ctx context.Context) uint {
	pd := parsePayload(ctx)
	if pd == nil {
		return 0
	}
	return pd.UserID
}

// VerifyAdmin 校验是否为管理员（RoleID=RoleAdmin）
func VerifyAdmin(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	return pd.RoleID == permission.RoleAdmin
}

// VerifyCoach 校验是否为纯教练（RoleID=RoleCoach）
func VerifyCoach(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	return pd.RoleID == permission.RoleCoach
}

// VerifyStaff 校验是否具备管理端权限（管理员 / 教练 / 队长）
func VerifyStaff(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	return permission.IsStaff(pd.RoleID)
}

// VerifyCaptain 校验是否为队长
func VerifyCaptain(ctx context.Context) bool {
	pd := parsePayload(ctx)
	if pd == nil {
		return false
	}
	return pd.RoleID == permission.RoleCaptain
}
