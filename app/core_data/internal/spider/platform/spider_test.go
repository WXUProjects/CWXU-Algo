package platform

import "testing"

func TestLogin(t *testing.T) {
	gu := NewCodeforces{}
	t.Log(gu.FetchSubmitLog(1, "wanli_", true))
}
