package task

import (
	"context"
	profile2 "cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/core_data/internal/data"
	"time"

	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/robfig/cron/v3"
	grpc2 "google.golang.org/grpc"
	"gorm.io/gorm"
)

type CronTask struct {
	spider  *SpiderTask
	summary *SummaryTask
	db      *gorm.DB
	reg     *registry.Registrar
}

func NewCronTask(spider *SpiderTask, data *data.Data, summary *SummaryTask, reg *discovery.Register) *CronTask {
	return &CronTask{
		spider:  spider,
		db:      data.DB,
		summary: summary,
		reg:     &reg.Reg,
	}
}
func (t *CronTask) userRPC() (*grpc2.ClientConn, error) {
	return grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*t.reg).(registry.Discovery)),
		grpc.WithTimeout(20*time.Second),
	)
}

func (t *CronTask) getUserIds() []int64 {
	userRpc, err := t.userRPC()
	if err != nil {
		return make([]int64, 0)
	}
	profile := profile2.NewProfileClient(userRpc)
	getUsers := func(pageNum int) (*profile2.GetListRes, error) {
		return profile.GetList(context.Background(), &profile2.GetListReq{
			PageSize: 100,
			PageNum:  1,
		})
	}
	res, err := getUsers(1)
	if err != nil {
		return make([]int64, 0)
	}
	rList := make([]*profile2.GetListRes, 1)
	rList[0] = res
	totalPage := (res.Total + 99) / 100
	for i := 1; i < int(totalPage); i++ {
		r, err := getUsers(i)
		if err != nil {
		}
		rList = append(rList, r)
	}
	var userIds []int64
	//t.db.Model(&model.Platform{}).
	//	Select("DISTINCT user_id").
	//	Pluck("user_id", &userIds)
	for _, v := range rList {
		for _, u := range v.List {
			userIds = append(userIds, int64(u.UserId))
		}
	}
	return userIds
}

func (t *CronTask) Do() {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	cr := cron.New(cron.WithLocation(loc))
	_, _ = cr.AddFunc("1 * * * *", func() {
		// 增量查询
		// 获取所有platform表中存在的userid
		userIds := t.getUserIds()
		for _, v := range userIds {
			t.spider.Do(v, false)
		}
	})
	_, _ = cr.AddFunc("30 7 * * *", func() {
		// 早7点半进行一次总结
		userIds := t.getUserIds()
		for _, v := range userIds {
			t.summary.Do(v, "PersonalLastDay")
		}
	})
	_, _ = cr.AddFunc("1 6,9,12,15,18,21,0 * * *", func() {
		// 每6 9 12 15 18 21 24 进行一次总结
		userIds := t.getUserIds()
		for _, v := range userIds {
			t.summary.Do(v, "PersonalRecent")
		}
	})
	cr.Start()
}
