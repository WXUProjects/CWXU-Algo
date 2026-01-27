package platform

import "testing"

func TestLogin(t *testing.T) {
	gu := NewLuoGu{}
	t.Log(gu.FetchSubmitLog(1, "sanenchen", true))
}
