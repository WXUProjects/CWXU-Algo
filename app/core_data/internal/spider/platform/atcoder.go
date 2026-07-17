package platform

import (
	"cwxu-algo/app/common/utils/ojhttp"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
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

func fetchLog(url string) ([]atcJson, error) {
	// 发起 Get 请求
	resp, err := ojhttp.Get(url)
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
	return atc, nil
}

func (p NewAtCoder) FetchSubmitLog(userId int64, username string, needAll bool) (res []model.SubmitLog, err error) {
	// 比如这里的needAll 如果为true 那么second就为0，表示从头到尾所有数据
	// 如果为false 那么就获取最近一天的数据
	t := time.Unix(0, 0)
	if needAll == false {
		t = time.Now().Add(-60 * (1 * time.Hour))
	}
	url := fmt.Sprintf(
		"https://atc.luckysan.top/atcoder/atcoder-api/v3/user/submissions?user=%s&from_second=%d",
		username, int(t.Unix()),
	)
	atc, err := fetchLog(url)
	if err != nil {
		return nil, err
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
	for len(atc) == 500 {
		url := fmt.Sprintf(
			"https://atc.luckysan.top/atcoder/atcoder-api/v3/user/submissions?user=%s&from_second=%d",
			username, atc[len(atc)-1].EpochSecond,
		)
		atc, err = fetchLog(url)
		if err != nil {
			return nil, err
		}
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
	}
	return res, nil
}
func (p NewAtCoder) Name() string {
	return spider.AtCoder
}

// FetchRating 从 AtCoder 官方 rating 历史取最新 NewRating
func (p NewAtCoder) FetchRating(username string) (int, bool, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return 0, false, fmt.Errorf("atcoder username 为空")
	}
	url := fmt.Sprintf("https://atcoder.jp/users/%s/history/json", username)
	resp, err := ojhttp.Get(url)
	if err != nil {
		return 0, false, fmt.Errorf("atcoder rating 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("atcoder rating 状态码 %d: %s", resp.StatusCode, string(body))
	}
	var hist []struct {
		IsRated   bool `json:"IsRated"`
		NewRating int  `json:"NewRating"`
	}
	if err := json.Unmarshal(body, &hist); err != nil {
		return 0, false, fmt.Errorf("atcoder rating 解析失败: %w", err)
	}
	if len(hist) == 0 {
		return 0, false, nil
	}
	// 优先最后一场 rated 的 NewRating；若全是非 rated，取最后一条 NewRating
	lastRated := -1
	for i, h := range hist {
		if h.IsRated {
			lastRated = i
		}
	}
	if lastRated >= 0 {
		return hist[lastRated].NewRating, true, nil
	}
	// 有历史但无 rated：仍返回末条（通常等于当前展示 rating）
	return hist[len(hist)-1].NewRating, true, nil
}

func init() {
	// 注册到注册中心
	spider.Register(NewAtCoder{})
}
