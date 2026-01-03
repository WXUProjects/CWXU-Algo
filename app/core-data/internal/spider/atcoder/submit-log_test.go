package atcoder

import "testing"

func TestFetchSubmitLog(t *testing.T) {
	r, err := FetchSubmitLog(0, "sanenchen", true)
	if err != nil {
		t.Errorf("测试出错 %s", err.Error())
	}
	for _, v := range r {
		t.Log(v)
	}
}
