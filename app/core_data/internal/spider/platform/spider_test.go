package platform

import "testing"

func TestLogin(t *testing.T) {
	gu := NewNowCoder{}
	t.Log(gu.FetchContestLog(1, "729716694", true))
}
