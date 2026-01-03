package atcoder

import (
	"cwxu-algo/app/core-data/internal/data/gorm/model"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

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
func FetchSubmitLog(userId int64, username string, needAll bool) (res []model.SubmitLog, err error) {
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
	// 发起get请求
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
