package service

import (
	"context"
	"strings"

	"github.com/redis/go-redis/v9"
)

// purposeChangeEmail 修改/绑定邮箱（SendCode purpose）
const purposeChangeEmail = "change_email"

// VerifyEmailCode 校验并消费邮箱验证码（与 AuthService.SendCode 写入的 key 一致）
func VerifyEmailCode(ctx context.Context, rdb *redis.Client, purpose, email, code string) bool {
	if rdb == nil {
		return false
	}
	email = strings.ToLower(strings.TrimSpace(email))
	code = strings.TrimSpace(code)
	purpose = strings.ToLower(strings.TrimSpace(purpose))
	if email == "" || code == "" || purpose == "" {
		return false
	}
	key := codeKeyPrefix + purpose + ":" + email
	attemptKey := codeAttemptPrefix + purpose + ":" + email
	const verifyAndConsume = `
local attempts = redis.call('INCR', KEYS[2])
if attempts == 1 then redis.call('PEXPIRE', KEYS[2], ARGV[3]) end
if attempts > tonumber(ARGV[2]) then
  redis.call('DEL', KEYS[1])
  return 0
end
local stored = redis.call('GET', KEYS[1])
if not stored or stored ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1], KEYS[2])
return 1`
	result, err := rdb.Eval(ctx, verifyAndConsume, []string{key, attemptKey},
		code, maxCodeAttempts, codeTTL.Milliseconds()).Int()
	return err == nil && result == 1
}
