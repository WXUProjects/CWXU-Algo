package test

import (
	"cwxu-algo/app/core_data/internal/spider"
	_ "cwxu-algo/app/core_data/internal/spider/platform"
	"testing"
)

func TestSpider(t *testing.T) {
	pms := []string{spider.CodeForces}
	for _, pm := range pms {
		t.Run(pm, func(t *testing.T) {
			if p, ok := spider.Get(pm); ok {
				if slf, ok := p.(spider.SubmitContestFetcher); ok {
					r, err := slf.FetchContestLog(1, "wanli_", true)
					if err != nil {
						t.Errorf("测试出错 %s", err.Error())
					}
					for _, v := range r {
						t.Log(v)
					}
				}
			} else {
				t.Errorf("没有找到%s提供器", pm)
			}
		})
	}
}
