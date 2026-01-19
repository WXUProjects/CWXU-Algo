package spider

import "cwxu-algo/app/core_data/internal/data/gorm/model"

// Provider 做题记录提供器
type Provider interface {
	Name() string
}

// SubmitLogFetcher 提交记录 Fetcher
type SubmitLogFetcher interface {
	FetchSubmitLog(userId int64, username string, needAll bool) ([]model.SubmitLog, error)
}
