package platform

import (
	"bytes"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
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
		return nil, fmt.Errorf("API status error: %s", cfResp.Status)
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

// FetchContestLog 拉取CodeForces比赛记录
func (p NewCodeforces) FetchContestLog(userId int64, username string, needAll bool) ([]model.ContestLog, error) {
	var contestLogs []model.ContestLog

	url := fmt.Sprintf("https://codeforces.com/contests/with/%s?type=all", username)

	// ⚠️ 注意：标准 http.Get 极易触发 Cloudflare 5秒盾，导致返回 403/503
	// 生产环境中建议替换为 http.Client 配合特定的 Headers 或代理
	req, _ := http.NewRequest("GET", url, nil)
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, errors.New("Network Error: " + strconv.Itoa(resp.StatusCode))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// 检查是否被 Cloudflare 拦截
	if bytes.Contains(body, []byte("Just a moment...")) {
		return nil, errors.New("blocked by Cloudflare protection")
	}

	// 加载 HTML 文档
	doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
	if err != nil {
		return nil, err
	}

	// 设置 Codeforces 默认时区 (莫斯科时间 UTC+3)
	cfZone := time.FixedZone("MSK", 3*3600)

	// 解析表格行
	doc.Find("table.user-contests-table tbody tr").Each(func(i int, s *goquery.Selection) {
		// 如果不需要全部，且已经解析了第一条，则跳出
		if !needAll && i > 0 {
			return
		}

		log := model.ContestLog{
			Platform: "Codeforces",
			UserID:   userId,
		}

		shZone, err := time.LoadLocation("Asia/Shanghai")
		if err != nil {
			// 如果运行环境所在的系统缺失 tzdata（时区数据），做一个硬编码的兜底
			shZone = time.FixedZone("CST", 8*3600)
		}
		// 1. 提取比赛名称和 ID (第2列)
		aNode := s.Find("td").Eq(1).Find("a")
		log.ContestName = strings.TrimSpace(aNode.Text())
		href, exists := aNode.Attr("href") // 例如: "/contest/2217"
		if exists {
			parts := strings.Split(strings.Trim(href, "/"), "/")
			if len(parts) >= 2 {
				log.ContestId = parts[1]
				log.ContestUrl = "https://codeforces.com" + href
			}
		}

		// 2. 提取比赛时间 (第3列) -> "Apr/07/2026 17:35"
		timeStr := strings.TrimSpace(s.Find("td").Eq(2).Find(".format-time").Text())
		if timeStr != "" {
			// Go 的时间模板必须是 2006-01-02 15:04:05 相关的固定值
			parsedTime, err := time.ParseInLocation("Jan/02/2006 15:04", timeStr, cfZone)
			if err == nil {
				log.Time = parsedTime.In(shZone)
			}
		}

		// 3. 提取排名 (第4列)
		rankStr := strings.TrimSpace(s.Find("td").Eq(3).Find("a").Text())
		log.Rank, _ = strconv.Atoi(rankStr)

		// 4. 提取过题数 (第5列)
		acStr := strings.TrimSpace(s.Find("td").Eq(4).Find("a").Text())
		log.AcCount, _ = strconv.Atoi(acStr)
		contestLogs = append(contestLogs, log)
	})

	return contestLogs, nil
}

func (p NewCodeforces) Name() string {
	return spider.CodeForces
}
func init() {
	// 注册到注册中心
	spider.Register(NewCodeforces{})
}
