package auth

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"strings"

	"github.com/go-kratos/kratos/v2/transport"
)

// JwtPayload JWT载荷结构体
type JwtPayload struct {
	// 业务字段
	UserID   uint   `json:"userId"`   // 用户ID，对应u.ID（假设u.ID是uint类型）
	Username string `json:"username"` // 用户名，对应u.Username
	Name     string `json:"name"`     // 姓名，对应u.Name
	Email    string `json:"email"`    // 邮箱，对应u.Email
	RoleIDs  []uint `json:"roleIds"`  // 角色ID列表，对应u.RoleIDs（假设是[]uint类型）
}

func praseJwtToken(ctx context.Context) string {
	header, _ := transport.FromServerContext(ctx)
	auths := strings.SplitN(header.RequestHeader().Get("Authorization"), " ", 2)
	return auths[1]
}

func VerifyById(ctx context.Context, userId uint) bool {
	parts := strings.Split(praseJwtToken(ctx), ".")
	if len(parts) != 3 {
		return false
	}
	payloadBase64 := parts[1]
	dstLen := base64.RawURLEncoding.DecodedLen(len(payloadBase64))
	dst := make([]byte, dstLen)
	_, err := base64.RawURLEncoding.Decode(dst, []byte(payloadBase64))
	if err != nil {
		return false
	}
	pd := JwtPayload{}
	if err := json.Unmarshal(dst, &pd); err != nil {
		return false
	}
	if len(pd.RoleIDs) != 0 && pd.RoleIDs[0] == 1 {
		return true
	}
	if pd.UserID != userId {
		return false
	}
	return true
}
