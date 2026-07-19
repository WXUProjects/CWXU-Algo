package service

import "testing"

func TestNowCoderNoAccessIsPermanentNotTransient(t *testing.T) {
	msgs := []string{
		"NowCoder 题面暂无访问权限",
		"NowCoder 题面暂无访问权限，请稍后重试", // 历史文案
		"瞬时失败(退避30s, 自01-01 00:00起可重试至24h): NowCoder 题面暂无访问权限",
		"没有查看题目的权限",
	}
	for _, msg := range msgs {
		if !isPermanentFetchError(msg) {
			t.Errorf("expected permanent: %q", msg)
		}
		if isTransientFetchError(msg) {
			t.Errorf("expected not transient: %q", msg)
		}
		if !isNowCoderNoAccessError(msg) {
			t.Errorf("expected no-access: %q", msg)
		}
	}
}

func TestTransientStillCoversWAFAndDOM(t *testing.T) {
	cases := []string{
		"NowCoder 被 WAF 拦截，请稍后重试",
		"NowCoder 未找到题面 DOM，请稍后重试",
		"NowCoder 需要登录，请稍后重试",
	}
	for _, msg := range cases {
		if isPermanentFetchError(msg) {
			t.Errorf("expected not permanent: %q", msg)
		}
		if !isTransientFetchError(msg) {
			t.Errorf("expected transient: %q", msg)
		}
	}
}
