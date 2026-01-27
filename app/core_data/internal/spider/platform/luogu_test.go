package platform

import "testing"

func TestLogin(t *testing.T) {
	gu := NewLuoGu{}
	gu.FetchSubmitLog(1, "sanenchen", true)
}
