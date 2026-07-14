package event

type SpiderEvent struct {
	UserId  int64 `json:"user_id"`
	NeedAll bool  `json:"need_all"`
}

type SummaryEvent struct {
	UserId int64  `json:"user_id"`
	Type   string `json:"type"`
}

// ProblemFetchEvent 题面爬取 + AI 打标异步任务
type ProblemFetchEvent struct {
	ProblemID  uint   `json:"problem_id"`
	Platform   string `json:"platform"`
	ExternalID string `json:"external_id"`
	URL        string `json:"url"`
}
