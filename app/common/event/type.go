package event

type SpiderEvent struct {
	UserId   int64  `json:"user_id"`
	NeedAll  bool   `json:"need_all"`
	// Platform 非空时只抓该平台；空则抓用户全部已绑定平台
	Platform string `json:"platform,omitempty"`
}

type SummaryEvent struct {
	UserId int64  `json:"user_id"`
	Type   string `json:"type"`
}

// ProblemFetchEvent 题面爬取任务（problem_fetch 队列，并发 2）
type ProblemFetchEvent struct {
	ProblemID  uint   `json:"problem_id"`
	Platform   string `json:"platform"`
	ExternalID string `json:"external_id"`
	URL        string `json:"url"`
}

// ProblemAnalyzeEvent AI 打标任务（problem_analyze 队列，并发 3）
// 仅在题面 content_md 已落库后投递
type ProblemAnalyzeEvent struct {
	ProblemID uint `json:"problem_id"`
}
