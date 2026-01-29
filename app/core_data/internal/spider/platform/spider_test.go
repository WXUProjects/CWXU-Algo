package platform

import "testing"

func TestLogin(t *testing.T) {
	gu := NewNowCoder{}
	t.Log(gu.fetchSub(1, "729716694", false))
}
