package calendar

// Item 归一化后的公开赛程
type Item struct {
	Platform     string
	PlatformName string
	ExternalID   string
	Name         string
	URL          string
	StartTime    int64 // Unix 秒
	EndTime      int64
	Source       string
	IconURL      string
}
