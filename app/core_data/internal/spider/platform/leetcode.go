package platform

import (
	"cwxu-algo/app/common/utils/ojhttp"
	"bytes"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"
)

// NewLeetCode 力扣（leetcode.cn）爬虫
// 策略：不抓具体提交明细，只把「每日提交次数」和「累计 AC 题数」写成合成 SubmitLog，
// 供热力图 / 总提交 / 总做题数统计；活动流侧会过滤 LeetCode 平台。
type NewLeetCode struct{}

const (
	lcGraphQL = "https://leetcode.cn/graphql/"
	lcCalAPI  = "https://leetcode.cn/api/user_submission_calendar/%s/"
	// 初始化时把历史 AC 锚到很早的日期，避免「今日 AC」被刷成全量
	lcACBaselineDay = "2000-01-01"
)

type lcProfileResp struct {
	Data struct {
		UserProfilePublicProfile *struct {
			SubmissionProgress *struct {
				AcTotal          int `json:"acTotal"`
				TotalSubmissions int `json:"totalSubmissions"`
			} `json:"submissionProgress"`
		} `json:"userProfilePublicProfile"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

func (p NewLeetCode) Name() string {
	return spider.LeetCode
}

func (p NewLeetCode) FetchSubmitLog(userId int64, username string, needAll bool) ([]model.SubmitLog, error) {
	if username == "" {
		return nil, fmt.Errorf("leetcode username 为空")
	}

	cal, err := fetchLeetCodeCalendar(username)
	if err != nil {
		return nil, err
	}
	acTotal, err := fetchLeetCodeAcTotal(username)
	if err != nil {
		return nil, err
	}

	now := time.Now()
	loc := now.Location()
	today := time.Date(now.Year(), now.Month(), now.Day(), 12, 0, 0, 0, loc)

	// 增量：只关心最近几天日历 + 今日 AC 增量占位（由上层按已有数据差量落库；
	// 这里仍产出「全量稳定 ID」记录，依赖 submit_id 唯一 + OnConflict DoNothing 去重）
	cutoff := time.Time{}
	if !needAll {
		// 最近 14 天足够覆盖增量；更早的日历记录若已入库会因 submit_id 冲突被忽略
		cutoff = today.AddDate(0, 0, -14)
	}

	res := make([]model.SubmitLog, 0, 512)

	// 1) 日历 → 每日提交次数（status 不含 AC，只进「提交热力图 / 提交次数」）
	for dayUnix, cnt := range cal {
		if cnt <= 0 {
			continue
		}
		day := time.Unix(dayUnix, 0).In(time.UTC)
		// 用当天中午，避免时区边界把日期挤到前后一天
		dayLocal := time.Date(day.Year(), day.Month(), day.Day(), 12, 0, 0, 0, loc)
		if !needAll && dayLocal.Before(cutoff) {
			continue
		}
		for i := 0; i < cnt; i++ {
			res = append(res, model.SubmitLog{
				UserID:   userId,
				Platform: spider.LeetCode,
				// 稳定 ID：同一天同一序号永远相同，便于全量/增量幂等
				SubmitID: fmt.Sprintf("lc-cal-%d-%s-%d", userId, dayLocal.Format("20060102"), i),
				Contest:  "leetcode",
				Problem:  "leetcode-submit",
				Lang:     "LeetCode",
				Status:   "SUBMIT",
				Time:     dayLocal.Add(time.Duration(i) * time.Second),
			})
		}
	}

	// 2) 累计 AC 题数 → 合成 AC 记录（status 含 AC，进「AC 热力图 / 做题数」）
	//    - 全量初始化：历史 AC 全部锚到 baseline 日，只保证 Total 正确，不污染 Today
	//    - 增量：额外产出「今日可能的新 AC」槽位；真正的「今日增量」依赖：
	//      已有 lc-ac-{userId}-{i} 数量 vs 当前 acTotal 的差，OnConflict 只插入新的 i
	//
	//    problem 用 lc-ac-problem-{i}，保证 COUNT(DISTINCT problem) = acTotal
	baselineDay, _ := time.ParseInLocation("2006-01-02", lcACBaselineDay, loc)
	baselineDay = time.Date(baselineDay.Year(), baselineDay.Month(), baselineDay.Day(), 12, 0, 0, 0, loc)

	for i := 0; i < acTotal; i++ {
		// 增量模式下，历史 AC（除了最近可能新增的一小段）也照样生成；
		// 已存在的 submit_id 会被 DoNothing 忽略，只有 i >= 旧总量 的新记录会插入。
		// 新记录时间用「今天」，这样 Today/本周 AC 增量正确。
		t := baselineDay
		if !needAll {
			// 增量：统一标今天；旧 ID 冲突忽略，新 ID 记到今天
			t = today
		}
		res = append(res, model.SubmitLog{
			UserID:   userId,
			Platform: spider.LeetCode,
			SubmitID: fmt.Sprintf("lc-ac-%d-%d", userId, i),
			Contest:  "leetcode",
			Problem:  fmt.Sprintf("lc-ac-problem-%d", i),
			Lang:     "LeetCode",
			Status:   "AC",
			Time:     t.Add(time.Duration(i%60) * time.Second),
		})
	}

	return res, nil
}

func fetchLeetCodeCalendar(username string) (map[int64]int, error) {
	url := fmt.Sprintf(lcCalAPI, username)
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	setLCHeaders(req, username)

	resp, err := ojhttp.Do(req)
	if err != nil {
		return nil, fmt.Errorf("leetcode calendar 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("leetcode calendar 读 body 失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("leetcode calendar 状态码 %d: %s", resp.StatusCode, string(body))
	}

	// 接口有时直接返回 object，有时返回 JSON 字符串
	var raw json.RawMessage
	if err := json.Unmarshal(body, &raw); err != nil {
		return nil, fmt.Errorf("leetcode calendar 解析失败: %w", err)
	}
	var obj map[string]int
	if len(raw) > 0 && raw[0] == '"' {
		var s string
		if err := json.Unmarshal(raw, &s); err != nil {
			return nil, fmt.Errorf("leetcode calendar 字符串解包失败: %w", err)
		}
		if err := json.Unmarshal([]byte(s), &obj); err != nil {
			return nil, fmt.Errorf("leetcode calendar 内层解析失败: %w", err)
		}
	} else {
		// key 是字符串时间戳
		var tmp map[string]int
		if err := json.Unmarshal(raw, &tmp); err != nil {
			return nil, fmt.Errorf("leetcode calendar object 解析失败: %w", err)
		}
		obj = tmp
	}

	out := make(map[int64]int, len(obj))
	for k, v := range obj {
		ts, err := strconv.ParseInt(k, 10, 64)
		if err != nil {
			continue
		}
		out[ts] = v
	}
	return out, nil
}

func fetchLeetCodeAcTotal(username string) (int, error) {
	payload := map[string]interface{}{
		"query": `query userPublicProfile($userSlug: String!) {
			userProfilePublicProfile(userSlug: $userSlug) {
				submissionProgress { acTotal totalSubmissions }
			}
		}`,
		"variables": map[string]string{"userSlug": username},
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, lcGraphQL, bytes.NewReader(b))
	if err != nil {
		return 0, err
	}
	setLCHeaders(req, username)
	req.Header.Set("Content-Type", "application/json")

	resp, err := ojhttp.Do(req)
	if err != nil {
		return 0, fmt.Errorf("leetcode profile 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, fmt.Errorf("leetcode profile 读 body 失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return 0, fmt.Errorf("leetcode profile 状态码 %d: %s", resp.StatusCode, string(body))
	}

	var pr lcProfileResp
	if err := json.Unmarshal(body, &pr); err != nil {
		return 0, fmt.Errorf("leetcode profile 解析失败: %w", err)
	}
	if len(pr.Errors) > 0 {
		return 0, fmt.Errorf("leetcode profile graphql 错误: %s", pr.Errors[0].Message)
	}
	if pr.Data.UserProfilePublicProfile == nil || pr.Data.UserProfilePublicProfile.SubmissionProgress == nil {
		return 0, fmt.Errorf("leetcode 用户不存在或资料不可见: %s", username)
	}
	return pr.Data.UserProfilePublicProfile.SubmissionProgress.AcTotal, nil
}

func setLCHeaders(req *http.Request, username string) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
	req.Header.Set("Referer", fmt.Sprintf("https://leetcode.cn/u/%s/", username))
	req.Header.Set("Origin", "https://leetcode.cn")
	req.Header.Set("Accept", "application/json, text/plain, */*")
}

func init() {
	spider.Register(NewLeetCode{})
}
