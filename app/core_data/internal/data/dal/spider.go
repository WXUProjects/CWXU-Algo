package dal

import "context"

// SpiderDal 爬虫数据操作模块
type SpiderDal struct {
}

func NewSpiderDal() *SpiderDal {
	return &SpiderDal{}
}

func GetByUserId(ctx context.Context, userId int64) {

}
