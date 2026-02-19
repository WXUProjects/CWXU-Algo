package platform

import "testing"

func TestLogin(t *testing.T) {
	gu := NewNowCoder{}
	t.Log(gu.FetchSubmitLog(1, "832503900", true))
}
