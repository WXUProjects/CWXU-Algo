package platform

import (
	"bytes"
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

// NewLeetCode 力扣（leetcode.cn）爬虫
//
// 策略（submit_id 前缀约定，见 model.CountsTowardSubmitStat）：
//  1. lc-cal-*  日历每日提交次数 → 提交热力 / 提交数
//  2. lc-pad-*  生涯 totalSubmissions 与日历差量 → 补齐生涯提交（修 AC 率）
//  3. lc-prob-* 最近通过明细（有 titleSlug）→ 题库 / AI / 动态与提交历史（默认 AC，无代码）
//  4. lc-ac-*   合成 AC（acTotal 条）→ 生涯做题数；与 lc-prob 并存时总数可能略高
//
// 活动流：仅展示 lc-prob-*；合成行（lc-cal / lc-pad / lc-ac）仍过滤。
type NewLeetCode struct{}

const (
	lcGraphQL    = "https://leetcode.cn/graphql/"
	lcGraphQLNoj = "https://leetcode.cn/graphql/noj-go/"
	lcCalAPI     = "https://leetcode.cn/api/user_submission_calendar/%s/"
	// 历史 AC / 提交补齐锚到很早的日期，避免「今日」被刷成全量
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

type lcRecentACResp struct {
	Data struct {
		RecentACSubmissions []struct {
			SubmissionID int64 `json:"submissionId"`
			SubmitTime   int64 `json:"submitTime"`
			Question     *struct {
				TitleSlug         string `json:"titleSlug"`
				Title             string `json:"title"`
				TranslatedTitle   string `json:"translatedTitle"`
				QuestionFrontendID string `json:"questionFrontendId"`
			} `json:"question"`
		} `json:"recentACSubmissions"`
	} `json:"data"`
	Errors []struct {
		Message string `json:"message"`
	} `json:"errors"`
}

type lcProgress struct {
	AcTotal          int
	TotalSubmissions int
}

type lcRecentAC struct {
	SubmissionID int64
	SubmitTime   time.Time
	TitleSlug    string
	Title        string // 展示用（中文优先）
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
	prog, err := fetchLeetCodeProgress(username)
	if err != nil {
		return nil, err
	}
	// 最近通过失败不阻断热力/总数；题库侧只是少几题
	recent, recentErr := fetchLeetCodeRecentAC(username)
	if recentErr != nil {
		recent = nil
	}

	now := time.Now()
	loc := now.Location()
	today := time.Date(now.Year(), now.Month(), now.Day(), 12, 0, 0, 0, loc)
	baselineDay, _ := time.ParseInLocation("2006-01-02", lcACBaselineDay, loc)
	baselineDay = time.Date(baselineDay.Year(), baselineDay.Month(), baselineDay.Day(), 12, 0, 0, 0, loc)

	// 增量：只关心最近几天日历；更早的日历记录若已入库会因 submit_id 冲突被忽略
	cutoff := time.Time{}
	if !needAll {
		cutoff = today.AddDate(0, 0, -14)
	}

	res := make([]model.SubmitLog, 0, 512+len(recent))
	calSum := 0

	// 1) 日历 → 每日提交次数（status 不含 AC，只进「提交热力图 / 提交次数」）
	for dayUnix, cnt := range cal {
		if cnt <= 0 {
			continue
		}
		day := time.Unix(dayUnix, 0).In(time.UTC)
		dayLocal := time.Date(day.Year(), day.Month(), day.Day(), 12, 0, 0, 0, loc)
		calSum += cnt
		if !needAll && dayLocal.Before(cutoff) {
			// 增量不产出旧日历行，但仍计入 calSum 以便 pad 正确
			// 注意：增量时 pad 依赖全量 calSum，所以仍要扫完全部日历计数
			continue
		}
		for i := 0; i < cnt; i++ {
			res = append(res, model.SubmitLog{
				UserID:   userId,
				Platform: spider.LeetCode,
				SubmitID: fmt.Sprintf("lc-cal-%d-%s-%d", userId, dayLocal.Format("20060102"), i),
				Contest:  "leetcode",
				Problem:  "leetcode-submit",
				Lang:     "LeetCode",
				Status:   "SUBMIT",
				Time:     dayLocal.Add(time.Duration(i) * time.Second),
			})
		}
	}

	// 增量路径：上面 continue 了旧日，calSum 仍是全量；但 pad 需要全量 cal 之和
	// 若 needAll=false 时我们 continue 前已 calSum+=cnt，OK

	// 2) 生涯提交补齐：totalSubmissions - 日历合计 → baseline 日 SUBMIT
	//    修 AC 率：分子生涯 AC 题数 / 分母生涯提交（不再只剩近一年日历）
	pad := prog.TotalSubmissions - calSum
	if pad < 0 {
		pad = 0
	}
	for i := 0; i < pad; i++ {
		res = append(res, model.SubmitLog{
			UserID:   userId,
			Platform: spider.LeetCode,
			SubmitID: fmt.Sprintf("lc-pad-%d-%d", userId, i),
			Contest:  "leetcode",
			Problem:  "leetcode-submit",
			Lang:     "LeetCode",
			Status:   "SUBMIT",
			Time:     baselineDay.Add(time.Duration(i%60) * time.Second),
		})
	}

	// 3) 最近通过 → 真实题级 AC（进题库 / 动态 / 提交历史；不计提交数以免与日历双计）
	//    公开接口常对同一题返回多次 AC → 先按 submissionId / titleSlug 去重（保留最新）。
	//    无源码 → 状态固定 AC。
	for _, r := range dedupeLeetCodeRecentAC(recent) {
		title := r.Title
		if title == "" {
			title = r.TitleSlug
		}
		res = append(res, model.SubmitLog{
			UserID:     userId,
			Platform:   spider.LeetCode,
			SubmitID:   fmt.Sprintf("lc-prob-%d", r.SubmissionID),
			Contest:    "leetcode",
			Problem:    fmt.Sprintf("%s %s", r.TitleSlug, title),
			ExternalID: r.TitleSlug,
			Lang:       "-", // 公开最近通过无语言/代码
			Status:     "AC",
			Time:       r.SubmitTime,
		})
	}

	// 4) 累计 AC 题数 → 合成 AC（status=AC，进「AC 热力图 / 做题数」）
	//    力扣接口只给已去重的 acTotal；稳定 submit_id，全量锚 baseline，增量新行记今天。
	for i := 0; i < prog.AcTotal; i++ {
		t := baselineDay
		if !needAll {
			t = today
		}
		ext := fmt.Sprintf("ac-%d", i)
		res = append(res, model.SubmitLog{
			UserID:     userId,
			Platform:   spider.LeetCode,
			SubmitID:   fmt.Sprintf("lc-ac-%d-%d", userId, i),
			Contest:    "leetcode",
			Problem:    fmt.Sprintf("lc-ac-problem-%d", i),
			ExternalID: ext,
			Lang:       "LeetCode",
			Status:     "AC",
			Time:       t.Add(time.Duration(i%60) * time.Second),
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

func fetchLeetCodeProgress(username string) (lcProgress, error) {
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
		return lcProgress{}, err
	}
	setLCHeaders(req, username)
	req.Header.Set("Content-Type", "application/json")

	resp, err := ojhttp.Do(req)
	if err != nil {
		return lcProgress{}, fmt.Errorf("leetcode profile 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return lcProgress{}, fmt.Errorf("leetcode profile 读 body 失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return lcProgress{}, fmt.Errorf("leetcode profile 状态码 %d: %s", resp.StatusCode, string(body))
	}

	var pr lcProfileResp
	if err := json.Unmarshal(body, &pr); err != nil {
		return lcProgress{}, fmt.Errorf("leetcode profile 解析失败: %w", err)
	}
	if len(pr.Errors) > 0 {
		return lcProgress{}, fmt.Errorf("leetcode profile graphql 错误: %s", pr.Errors[0].Message)
	}
	if pr.Data.UserProfilePublicProfile == nil || pr.Data.UserProfilePublicProfile.SubmissionProgress == nil {
		return lcProgress{}, fmt.Errorf("leetcode 用户不存在或资料不可见: %s", username)
	}
	sp := pr.Data.UserProfilePublicProfile.SubmissionProgress
	return lcProgress{
		AcTotal:          sp.AcTotal,
		TotalSubmissions: sp.TotalSubmissions,
	}, nil
}

// dedupeLeetCodeRecentAC 同一批最近通过：submissionId 去重 + 同一 titleSlug 只留最新一条
// API 列表通常已按时间倒序；同 slug 保留先出现的（更新）。
func dedupeLeetCodeRecentAC(in []lcRecentAC) []lcRecentAC {
	if len(in) == 0 {
		return nil
	}
	seenID := make(map[int64]struct{}, len(in))
	seenSlug := make(map[string]struct{}, len(in))
	out := make([]lcRecentAC, 0, len(in))
	for _, r := range in {
		if r.TitleSlug == "" || r.SubmissionID == 0 {
			continue
		}
		if _, ok := seenID[r.SubmissionID]; ok {
			continue
		}
		if _, ok := seenSlug[r.TitleSlug]; ok {
			continue
		}
		seenID[r.SubmissionID] = struct{}{}
		seenSlug[r.TitleSlug] = struct{}{}
		out = append(out, r)
	}
	return out
}

func fetchLeetCodeRecentAC(username string) ([]lcRecentAC, error) {
	payload := map[string]interface{}{
		"query": `query recentACSubmissions($userSlug: String!) {
			recentACSubmissions(userSlug: $userSlug) {
				submissionId
				submitTime
				question {
					titleSlug
					title
					translatedTitle
					questionFrontendId
				}
			}
		}`,
		"variables": map[string]string{"userSlug": username},
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, lcGraphQLNoj, bytes.NewReader(b))
	if err != nil {
		return nil, err
	}
	setLCHeaders(req, username)
	req.Header.Set("Content-Type", "application/json")

	resp, err := ojhttp.Do(req)
	if err != nil {
		return nil, fmt.Errorf("leetcode recentAC 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("leetcode recentAC 读 body 失败: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("leetcode recentAC 状态码 %d: %s", resp.StatusCode, string(body))
	}

	var pr lcRecentACResp
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, fmt.Errorf("leetcode recentAC 解析失败: %w", err)
	}
	if len(pr.Errors) > 0 {
		return nil, fmt.Errorf("leetcode recentAC graphql 错误: %s", pr.Errors[0].Message)
	}

	out := make([]lcRecentAC, 0, len(pr.Data.RecentACSubmissions))
	for _, item := range pr.Data.RecentACSubmissions {
		if item.Question == nil || item.Question.TitleSlug == "" {
			continue
		}
		title := item.Question.TranslatedTitle
		if title == "" {
			title = item.Question.Title
		}
		// submitTime 为 Unix 秒
		t := time.Unix(item.SubmitTime, 0)
		if item.SubmitTime <= 0 {
			t = time.Now()
		}
		out = append(out, lcRecentAC{
			SubmissionID: item.SubmissionID,
			SubmitTime:   t,
			TitleSlug:    item.Question.TitleSlug,
			Title:        title,
		})
	}
	return out, nil
}

func setLCHeaders(req *http.Request, username string) {
	req.Header.Set("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
	req.Header.Set("Referer", fmt.Sprintf("https://leetcode.cn/u/%s/", username))
	req.Header.Set("Origin", "https://leetcode.cn")
	req.Header.Set("Accept", "application/json, text/plain, */*")
}

// FetchRating 力扣竞赛 rating（noj-go GraphQL；浮点四舍五入为整数）
func (p NewLeetCode) FetchRating(username string) (int, bool, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return 0, false, fmt.Errorf("leetcode username 为空")
	}
	payload := map[string]interface{}{
		"query": `query userContestRanking($userSlug: String!) {
			userContestRanking(userSlug: $userSlug) { rating }
		}`,
		"variables": map[string]string{"userSlug": username},
	}
	b, _ := json.Marshal(payload)
	req, err := http.NewRequest(http.MethodPost, lcGraphQLNoj, bytes.NewReader(b))
	if err != nil {
		return 0, false, err
	}
	setLCHeaders(req, username)
	req.Header.Set("Content-Type", "application/json")

	resp, err := ojhttp.Do(req)
	if err != nil {
		return 0, false, fmt.Errorf("leetcode rating 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("leetcode rating 状态码 %d: %s", resp.StatusCode, string(body))
	}
	var pr struct {
		Data struct {
			UserContestRanking *struct {
				Rating float64 `json:"rating"`
			} `json:"userContestRanking"`
		} `json:"data"`
		Errors []struct {
			Message string `json:"message"`
		} `json:"errors"`
	}
	if err := json.Unmarshal(body, &pr); err != nil {
		return 0, false, fmt.Errorf("leetcode rating 解析失败: %w", err)
	}
	if len(pr.Errors) > 0 {
		return 0, false, fmt.Errorf("leetcode rating graphql: %s", pr.Errors[0].Message)
	}
	// 未参加过竞赛时 userContestRanking 为 null
	if pr.Data.UserContestRanking == nil {
		return 0, false, nil
	}
	return int(pr.Data.UserContestRanking.Rating + 0.5), true, nil
}

func init() {
	spider.Register(NewLeetCode{})
}
