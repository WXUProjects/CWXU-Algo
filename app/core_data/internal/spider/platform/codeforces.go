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

type NewCodeforces struct{}

type CFResponse struct {
	Status string   `json:"status"`
	Result []cfJson `json:"result"`
}

type cfJson struct {
	ID        int `json:"id"`
	ContestID int `json:"contestId"`
	Problem   struct {
		Index string `json:"index"`
		Name  string `json:"name"`
	} `json:"problem"`
	ProgrammingLanguage string `json:"programmingLanguage"`
	Verdict             string `json:"verdict"`
	CreationTimeSeconds int64  `json:"creationTimeSeconds"`
}

func (p NewCodeforces) FetchSubmitLog(userId int64, username string, needAll bool) (res []model.SubmitLog, err error) {
	need := 1000
	if needAll == true {
		need = 1000000
	}
	handle := username
	last_commit := 1
	url := fmt.Sprintf(
		"https://codeforces.com/api/user.status?handle=%s&from=%d&count=%d",
		handle, last_commit, need,
	)
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

	var cfResp CFResponse
	err = json.Unmarshal(body, &cfResp)
	if err != nil {
		return nil, fmt.Errorf("解析json错误：%s", err.Error())
	}

	if cfResp.Status != "OK" {
		return nil, fmt.Errorf("API status error:%s", err.Error())
	}

	for _, sub := range cfResp.Result {
		t := model.SubmitLog{
			UserID:   userId,
			Platform: spider.CodeForces,
			SubmitID: strconv.Itoa(sub.ID),
			Contest:  strconv.Itoa(sub.ContestID),
			Problem:  fmt.Sprintf("%s-%s", sub.Problem.Index, sub.Problem.Name),
			Lang:     sub.ProgrammingLanguage,
			Status:   sub.Verdict,
			Time:     time.Unix(sub.CreationTimeSeconds, 0),
		}
		res = append(res, t)
	}
	return res, nil
}
func (p NewCodeforces) Name() string {
	return spider.CodeForces
}
func init() {
	// 注册到注册中心
	spider.Register(NewCodeforces{})
}
