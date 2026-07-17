package spider

import "cwxu-algo/app/core_data/internal/data/model"

// Provider 做题记录提供器
type Provider interface {
	Name() string
}

// SubmitLogFetcher 提交记录抓取接口
//
// 该接口用于从各大 OJ 平台抓取用户的提交记录。
type SubmitLogFetcher interface {
	// FetchSubmitLog 获取提交记录
	//
	// 参数：
	//   - userId: 用户唯一 ID
	//   - username: 平台用户名
	//   - needAll: 是否全量抓取
	//
	// 返回值：
	//   - []model.SubmitLog: 提交记录列表
	FetchSubmitLog(userId int64, username string, needAll bool) ([]model.SubmitLog, error)
}

// SubmitContestFetcher 提交记录 Fetcher
type SubmitContestFetcher interface {
	// FetchContestLog 获取提交记录
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
	FetchContestLog(userId int64, username string, needAll bool) ([]model.ContestLog, error)
}

// RatingFetcher 可选：抓取平台当前 rating（与提交/比赛一并在爬虫任务中调用）
//
// hasRating=false 表示用户无 rating（未参赛等）或平台暂不可用；err 表示请求/解析失败。
type RatingFetcher interface {
	FetchRating(username string) (rating int, hasRating bool, err error)
}
