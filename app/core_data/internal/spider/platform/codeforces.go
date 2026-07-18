package platform

import (
	"cwxu-algo/app/common/utils/ojhttp"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strconv"
	"strings"
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

type cfRatingEntry struct {
	ContestID               int    `json:"contestId"`
	ContestName             string `json:"contestName"`
	Handle                  string `json:"handle"`
	Rank                    int    `json:"rank"`
	RatingUpdateTimeSeconds int64  `json:"ratingUpdateTimeSeconds"`
	OldRating               int    `json:"oldRating"`
	NewRating               int    `json:"newRating"`
}

type cfStatusForContest struct {
	ContestID int    `json:"contestId"`
	Verdict   string `json:"verdict"`
	Author    struct {
		ParticipantType string `json:"participantType"`
	} `json:"author"`
	Problem struct {
		Index string `json:"index"`
	} `json:"problem"`
	CreationTimeSeconds int64 `json:"creationTimeSeconds"`
}

type cfContestListEntry struct {
	ID               int    `json:"id"`
	Name             string `json:"name"`
	Phase            string `json:"phase"`
	DurationSeconds  int64  `json:"durationSeconds"`
	StartTimeSeconds int64  `json:"startTimeSeconds"`
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
			// CF 评测中可能省略 verdict → 空串；归一化后写入，避免 UI 显示空白
			Status: NormalizeCodeforcesVerdict(sub.Verdict),
			Time:   time.Unix(sub.CreationTimeSeconds, 0),
		}
		res = append(res, t)
	}
	return res, nil
}

// FetchContestLog 拉取 Codeforces 比赛记录。
//
// HTML 页面 /contests/with/{handle} 会被 Cloudflare 拦截，改走官方 API：
//  1. user.rating → 官方排名 / 比赛名 / 结算时间（仅 rated 且已出分）
//  2. user.status → 按 contestId 统计正式参赛 (CONTESTANT/OUT_OF_COMPETITION) 的 unique OK 作为 AC
//  3. 刚结束尚未出分、或 unrated：rank=0，仍写入 AC；站内榜可按 AC 模拟排名
func (p NewCodeforces) FetchContestLog(userId int64, username string, needAll bool) ([]model.ContestLog, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, fmt.Errorf("codeforces handle 为空")
	}

	ratings, err := fetchCFUserRating(username)
	if err != nil {
		return nil, err
	}
	acByContest, participateTime, err := fetchCFContestACFromStatus(username, needAll)
	if err != nil {
		return nil, err
	}

	// contestId → 合并后的日志
	type draft struct {
		rank     int
		name     string
		ac       int
		timeUnix int64
		fromRate bool
	}
	merged := map[int]*draft{}

	for _, r := range ratings {
		if r.ContestID <= 0 {
			continue
		}
		d := merged[r.ContestID]
		if d == nil {
			d = &draft{}
			merged[r.ContestID] = d
		}
		d.rank = r.Rank
		d.name = strings.TrimSpace(r.ContestName)
		d.timeUnix = r.RatingUpdateTimeSeconds
		d.fromRate = true
	}

	for cid, ac := range acByContest {
		d := merged[cid]
		if d == nil {
			d = &draft{}
			merged[cid] = d
		}
		d.ac = ac
		if d.timeUnix == 0 {
			d.timeUnix = participateTime[cid]
		}
	}

	// 仅有 rating、status 窗口未覆盖到的场次：AC 可能为 0（增量时常见）
	// 需要补比赛名的 id
	needMeta := make([]int, 0)
	for cid, d := range merged {
		if d.name == "" {
			needMeta = append(needMeta, cid)
		}
	}
	meta := map[int]cfContestListEntry{}
	if len(needMeta) > 0 {
		if m, mErr := fetchCFContestListMap(); mErr == nil {
			meta = m
		}
	}

	shZone, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		shZone = time.FixedZone("CST", 8*3600)
	}

	ids := make([]int, 0, len(merged))
	for cid := range merged {
		ids = append(ids, cid)
	}
	// 新→旧，便于 needAll=false 截断
	sort.Slice(ids, func(i, j int) bool {
		ti, tj := merged[ids[i]].timeUnix, merged[ids[j]].timeUnix
		if ti != tj {
			return ti > tj
		}
		return ids[i] > ids[j]
	})

	limit := len(ids)
	if !needAll && limit > 15 {
		limit = 15
	}

	out := make([]model.ContestLog, 0, limit)
	for _, cid := range ids[:limit] {
		d := merged[cid]
		name := d.name
		var t time.Time
		if d.timeUnix > 0 {
			t = time.Unix(d.timeUnix, 0).In(shZone)
		}
		if name == "" {
			if m, ok := meta[cid]; ok {
				name = strings.TrimSpace(m.Name)
				if t.IsZero() && m.StartTimeSeconds > 0 {
					t = time.Unix(m.StartTimeSeconds, 0).In(shZone)
				}
			}
		}
		if name == "" {
			name = fmt.Sprintf("Codeforces Contest %d", cid)
		}
		idStr := strconv.Itoa(cid)
		out = append(out, model.ContestLog{
			Platform:    spider.CodeForces,
			UserID:      userId,
			ContestId:   idStr,
			ContestName: name,
			ContestUrl:  "https://codeforces.com/contest/" + idStr,
			Rank:        d.rank,
			AcCount:     d.ac,
			TotalCount:  0,
			Time:        t,
		})
	}
	return out, nil
}

func fetchCFUserRating(username string) ([]cfRatingEntry, error) {
	url := fmt.Sprintf("https://codeforces.com/api/user.rating?handle=%s", username)
	resp, err := ojhttp.Get(url)
	if err != nil {
		return nil, fmt.Errorf("codeforces user.rating 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("codeforces user.rating 状态码 %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		Status  string          `json:"status"`
		Comment string          `json:"comment"`
		Result  []cfRatingEntry `json:"result"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, fmt.Errorf("codeforces user.rating 解析失败: %w", err)
	}
	if out.Status != "OK" {
		// 未参赛用户 comment 类似 "handles: User with handle ... not found" 或空 result
		if strings.Contains(strings.ToLower(out.Comment), "not found") {
			return nil, fmt.Errorf("codeforces user.rating: %s", out.Comment)
		}
		// 无 rating 历史：空列表
		if len(out.Result) == 0 {
			return []cfRatingEntry{}, nil
		}
		return nil, fmt.Errorf("codeforces user.rating API: %s %s", out.Status, out.Comment)
	}
	return out.Result, nil
}

// fetchCFContestACFromStatus 从 user.status 统计正式参赛过题数与最早提交时间。
// 返回：acByContest[contestId]=unique OK 数；participateTime[contestId]=最早提交 unix。
func fetchCFContestACFromStatus(username string, needAll bool) (map[int]int, map[int]int64, error) {
	need := 2000
	if needAll {
		need = 1000000
	}
	url := fmt.Sprintf(
		"https://codeforces.com/api/user.status?handle=%s&from=1&count=%d",
		username, need,
	)
	resp, err := ojhttp.Get(url)
	if err != nil {
		return nil, nil, fmt.Errorf("codeforces user.status 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, nil, fmt.Errorf("codeforces user.status 状态码 %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		Status  string               `json:"status"`
		Comment string               `json:"comment"`
		Result  []cfStatusForContest `json:"result"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, nil, fmt.Errorf("codeforces user.status 解析失败: %w", err)
	}
	if out.Status != "OK" {
		return nil, nil, fmt.Errorf("codeforces user.status API: %s %s", out.Status, out.Comment)
	}

	acProblems := map[int]map[string]struct{}{}
	participateTime := map[int]int64{}
	for _, s := range out.Result {
		if s.ContestID <= 0 {
			continue
		}
		pt := strings.ToUpper(strings.TrimSpace(s.Author.ParticipantType))
		// 正式参赛 / 非官方分区；练习与虚拟赛不计入比赛榜
		if pt != "CONTESTANT" && pt != "OUT_OF_COMPETITION" {
			continue
		}
		if t, ok := participateTime[s.ContestID]; !ok || (s.CreationTimeSeconds > 0 && s.CreationTimeSeconds < t) {
			if s.CreationTimeSeconds > 0 {
				participateTime[s.ContestID] = s.CreationTimeSeconds
			}
		}
		if !strings.EqualFold(strings.TrimSpace(s.Verdict), "OK") {
			continue
		}
		idx := strings.TrimSpace(s.Problem.Index)
		if idx == "" {
			continue
		}
		set := acProblems[s.ContestID]
		if set == nil {
			set = map[string]struct{}{}
			acProblems[s.ContestID] = set
		}
		set[idx] = struct{}{}
	}
	acByContest := make(map[int]int, len(acProblems))
	for cid, set := range acProblems {
		acByContest[cid] = len(set)
	}
	// 参赛但 0 AC 也要有记录（rank 可能来自 rating）
	for cid := range participateTime {
		if _, ok := acByContest[cid]; !ok {
			acByContest[cid] = 0
		}
	}
	return acByContest, participateTime, nil
}

func fetchCFContestListMap() (map[int]cfContestListEntry, error) {
	url := "https://codeforces.com/api/contest.list?gym=false"
	resp, err := ojhttp.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("codeforces contest.list 状态码 %d", resp.StatusCode)
	}
	var out struct {
		Status string               `json:"status"`
		Result []cfContestListEntry `json:"result"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if out.Status != "OK" {
		return nil, fmt.Errorf("codeforces contest.list status %s", out.Status)
	}
	m := make(map[int]cfContestListEntry, len(out.Result))
	for _, c := range out.Result {
		m[c.ID] = c
	}
	return m, nil
}

func (p NewCodeforces) Name() string {
	return spider.CodeForces
}

// FetchRating 通过 Codeforces API user.info 取当前 rating
func (p NewCodeforces) FetchRating(username string) (int, bool, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return 0, false, fmt.Errorf("codeforces handle 为空")
	}
	url := fmt.Sprintf("https://codeforces.com/api/user.info?handles=%s", username)
	resp, err := ojhttp.Get(url)
	if err != nil {
		return 0, false, fmt.Errorf("codeforces rating 请求失败: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return 0, false, err
	}
	if resp.StatusCode != http.StatusOK {
		return 0, false, fmt.Errorf("codeforces rating 状态码 %d: %s", resp.StatusCode, string(body))
	}
	var out struct {
		Status string `json:"status"`
		Result []struct {
			// 未参赛用户无 rating 字段
			Rating *int `json:"rating"`
		} `json:"result"`
		Comment string `json:"comment"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return 0, false, fmt.Errorf("codeforces rating 解析失败: %w", err)
	}
	if out.Status != "OK" || len(out.Result) == 0 {
		return 0, false, fmt.Errorf("codeforces rating API: %s %s", out.Status, out.Comment)
	}
	if out.Result[0].Rating == nil {
		return 0, false, nil // 未参赛
	}
	return *out.Result[0].Rating, true, nil
}

func init() {
	// 注册到注册中心
	spider.Register(NewCodeforces{})
}
