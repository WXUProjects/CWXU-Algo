package platform

import (
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

type NewAtCoder struct{}
type atcJson struct {
	ID            int    `json:"id"`
	EpochSecond   int64  `json:"epoch_second"` // Unix 时间戳（秒）
	ProblemID     string `json:"problem_id"`
	ContestID     string `json:"contest_id"`
	UserID        string `json:"user_id"`
	Language      string `json:"language"`
	Result        string `json:"result"`         // 如 "AC", "WA" 等
	ExecutionTime int    `json:"execution_time"` // 执行时间（毫秒）
}

func (p NewAtCoder) FetchSubmitLog(userId int64, username string, needAll bool) (res []model.SubmitLog, err error) {
	// 比如这里的needAll 如果为true 那么second就为0，表示从头到尾所有数据
	// 如果为false 那么就获取最近一天的数据
	t := time.Unix(0, 0)
	if needAll == false {
		t = time.Now().Add(-(24 * time.Hour))
	}
	url := fmt.Sprintf(
		"https://kenkoooo.com/atcoder/atcoder-api/v3/user/submissions?user=%s&from_second=%d",
		username, int(t.Unix()),
	)
	// 发起 Get 请求
	resp, err := http.Get(url)
	if err != nil {
		return nil, fmt.Errorf("发起http请求失败: %s", err.Error())
	}
	defer resp.Body.Close()
	// 校验状态码
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("请求响应码错误 %d, %s", resp.StatusCode, string(body))
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("解析body错误: %s", err.Error())
	}
	var atc []atcJson
	if err := json.Unmarshal(body, &atc); err != nil {
		return nil, fmt.Errorf("解析json错误：%s", err.Error())
	}
	// 构建res
	for _, v := range atc {
		tmp := model.SubmitLog{
			UserID:   userId,
			Platform: "AtCoder",
			SubmitID: strconv.Itoa(v.ID),
			Contest:  v.ContestID,
			Problem:  v.ProblemID,
			Lang:     v.Language,
			Status:   v.Result,
			Time:     time.Unix(v.EpochSecond, 0),
		}
		res = append(res, tmp)
	}
	return res, nil
}
func (p NewAtCoder) Name() string {
	return spider.AtCoder
}
func init() {
	// 注册到注册中心
	spider.Register(NewAtCoder{})
}
