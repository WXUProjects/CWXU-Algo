package spider

import "cwxu-algo/app/core_data/internal/data/model"

// Provider 做题记录提供器
type Provider interface {
	Name() string
}

// SubmitLogFetcher 提交记录 Fetcher
type SubmitLogFetcher interface {
	// FetchSubmitLog 获取提交记录
	//
	// 参数：
	//   - username: 标识将要查询的用户名
	//   - needAll: true为有多少查多少
	//     false为只需要查最近的即可，增量查询，比如可以直接返回最新的一页
	//
	// 返回值：
	//   - res []model.SubmitLog 数据库中的submitLog
	//   - err error 错误返回
	//
	// 注意：
	//   - 有错误要及时return nil, err
	FetchSubmitLog(userId int64, username string, needAll bool) ([]model.SubmitLog, error)
}
