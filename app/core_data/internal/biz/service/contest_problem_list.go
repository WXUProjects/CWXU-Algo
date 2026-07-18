package service

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cwxu-algo/app/core_data/internal/spider"
)

// ContestProblemSpec 从 OJ 比赛页/API 发现的一题（入库前）。
type ContestProblemSpec struct {
	Label      string // A / B / 1 …
	ExternalID string // 必须与 ParseProblemIdentity 一致
	Title      string
	URL        string
	Platform   string
}

// ListContestProblemSpecs 按平台主动发现比赛题目列表（不依赖用户提交）。
// 失败返回 error；空列表表示该场无题或暂不可用。
func ListContestProblemSpecs(platform, contestID string) ([]ContestProblemSpec, error) {
	platform = strings.TrimSpace(platform)
	contestID = strings.TrimSpace(contestID)
	if platform == "" || contestID == "" {
		return nil, fmt.Errorf("empty platform or contestId")
	}
	switch platform {
	case spider.CodeForces:
		return listCFContestProblems(contestID)
	case spider.AtCoder:
		return listAtCoderContestProblems(contestID)
	case spider.LuoGu:
		return listLuoGuContestProblems(contestID)
	case spider.NowCoder:
		return listNowCoderContestProblems(contestID)
	case spider.QOJ:
		return listQOJContestProblems(contestID)
	case spider.LeetCode:
		return listLeetCodeContestProblems(contestID)
	default:
		return nil, fmt.Errorf("unsupported platform %s", platform)
	}
}

func contestHTTPGet(rawURL string) ([]byte, int, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("User-Agent", `Mozilla/5.0 (compatible; GoAlgo/1.0; +https://algo.zhiyuansofts.cn)`)
	req.Header.Set("Accept", "application/json,text/html,*/*")
	client := &http.Client{Timeout: 25 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	if err != nil {
		return nil, resp.StatusCode, err
	}
	return body, resp.StatusCode, nil
}

// --- Codeforces ---

func listCFContestProblems(contestID string) ([]ContestProblemSpec, error) {
	// gym 前缀：contest_logs 里可能是纯数字 gym id，也可能带 gym
	cid := contestID
	isGym := false
	if strings.HasPrefix(strings.ToLower(cid), "gym") {
		isGym = true
		cid = cid[3:]
	}
	// CF API：正式赛用正 contestId；gym 用负数
	apiID := cid
	if isGym {
		if !strings.HasPrefix(apiID, "-") {
			apiID = "-" + apiID
		}
	}
	url := fmt.Sprintf("https://codeforces.com/api/contest.standings?contestId=%s&from=1&count=1", apiID)
	body, code, err := contestHTTPGet(url)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("cf standings status %d", code)
	}
	var out struct {
		Status string `json:"status"`
		Result struct {
			Problems []struct {
				ContestID int    `json:"contestId"`
				Index     string `json:"index"`
				Name      string `json:"name"`
			} `json:"problems"`
		} `json:"result"`
		Comment string `json:"comment"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if out.Status != "OK" {
		return nil, fmt.Errorf("cf standings: %s", firstNonEmpty(out.Comment, out.Status))
	}
	specs := make([]ContestProblemSpec, 0, len(out.Result.Problems))
	for i, p := range out.Result.Problems {
		idx := strings.TrimSpace(p.Index)
		if idx == "" {
			continue
		}
		var ext, problemURL string
		if isGym || p.ContestID < 0 {
			gid := cid
			if p.ContestID < 0 {
				gid = strconv.Itoa(-p.ContestID)
			}
			ext = "gym" + gid + idx
			problemURL = fmt.Sprintf("https://codeforces.com/gym/%s/problem/%s", gid, idx)
		} else {
			cidStr := contestID
			if p.ContestID > 0 {
				cidStr = strconv.Itoa(p.ContestID)
			}
			ext = cidStr + idx
			problemURL = fmt.Sprintf("https://codeforces.com/contest/%s/problem/%s", cidStr, idx)
		}
		specs = append(specs, ContestProblemSpec{
			Label:      idx,
			ExternalID: ext,
			Title:      firstNonEmpty(strings.TrimSpace(p.Name), idx),
			URL:        problemURL,
			Platform:   spider.CodeForces,
		})
		_ = i
	}
	return specs, nil
}

// --- AtCoder ---

var reAtCoderTaskLink = regexp.MustCompile(`(?i)/contests/([a-z0-9_]+)/tasks/([a-z0-9_]+)`)

func listAtCoderContestProblems(contestID string) ([]ContestProblemSpec, error) {
	contestID = strings.ToLower(strings.TrimSpace(contestID))
	// 去掉旧式 .contest.atcoder.jp
	if i := strings.Index(contestID, ".contest."); i > 0 {
		contestID = contestID[:i]
	}
	url := fmt.Sprintf("https://atcoder.jp/contests/%s/tasks", contestID)
	body, code, err := contestHTTPGet(url)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("atcoder tasks status %d", code)
	}
	html := string(body)
	seen := map[string]struct{}{}
	var specs []ContestProblemSpec
	matches := reAtCoderTaskLink.FindAllStringSubmatch(html, -1)
	for _, m := range matches {
		if len(m) < 3 {
			continue
		}
		c, task := m[1], m[2]
		if !strings.EqualFold(c, contestID) {
			continue
		}
		if _, ok := seen[task]; ok {
			continue
		}
		seen[task] = struct{}{}
		label := atCoderTaskLabel(task)
		specs = append(specs, ContestProblemSpec{
			Label:      label,
			ExternalID: task,
			Title:      task,
			URL:        fmt.Sprintf("https://atcoder.jp/contests/%s/tasks/%s", contestID, task),
			Platform:   spider.AtCoder,
		})
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("atcoder: no tasks found for %s", contestID)
	}
	return specs, nil
}

func atCoderTaskLabel(taskID string) string {
	// abc123_a → A
	if i := strings.LastIndex(taskID, "_"); i >= 0 && i+1 < len(taskID) {
		suf := taskID[i+1:]
		if len(suf) == 1 {
			return strings.ToUpper(suf)
		}
		return strings.ToUpper(suf)
	}
	return taskID
}

// --- 洛谷 ---

func listLuoGuContestProblems(contestID string) ([]ContestProblemSpec, error) {
	// 洛谷比赛：/contest/{id} 页面或 API
	url := fmt.Sprintf("https://www.luogu.com.cn/contest/%s?_contentOnly=1", contestID)
	body, code, err := contestHTTPGet(url)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("luogu contest status %d", code)
	}
	// 宽松解析：从 JSON 文本里抠 problem 列表
	s := string(body)
	// "problems":{"1000":{...},"P1001":{...}} 或数组
	rePID := regexp.MustCompile(`"(P\d+|B\d+|CF[A-Z0-9]+|AT_[a-z0-9_]+|T\d+|UVA\d+|SP\d+)"`)
	seen := map[string]struct{}{}
	var pids []string
	for _, m := range rePID.FindAllStringSubmatch(s, -1) {
		pid := m[1]
		if _, ok := seen[pid]; ok {
			continue
		}
		// 过滤 contest 无关噪声：仅保留看起来像题号
		seen[pid] = struct{}{}
		pids = append(pids, pid)
		if len(pids) >= 50 {
			break
		}
	}
	if len(pids) == 0 {
		return nil, fmt.Errorf("luogu: no problems in contest %s", contestID)
	}
	specs := make([]ContestProblemSpec, 0, len(pids))
	for i, pid := range pids {
		label := string(rune('A' + i))
		if i >= 26 {
			label = strconv.Itoa(i + 1)
		}
		specs = append(specs, ContestProblemSpec{
			Label:      label,
			ExternalID: pid,
			Title:      pid,
			URL:        "https://www.luogu.com.cn/problem/" + pid,
			Platform:   spider.LuoGu,
		})
	}
	return specs, nil
}

// --- 牛客 ---

func listNowCoderContestProblems(contestID string) ([]ContestProblemSpec, error) {
	// 公开接口：比赛题目列表
	url := fmt.Sprintf("https://ac.nowcoder.com/acm/contest/problem-list?id=%s", contestID)
	body, code, err := contestHTTPGet(url)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		// 兜底：HTML
		return listNowCoderContestProblemsHTML(contestID)
	}
	var out struct {
		Code int `json:"code"`
		Data struct {
			Data []struct {
				ProblemID   json.Number `json:"problemId"`
				Index       string      `json:"index"`
				ProblemName string      `json:"problemName"`
			} `json:"data"`
		} `json:"data"`
		Msg string `json:"msg"`
	}
	if err := json.Unmarshal(body, &out); err != nil || out.Code != 0 || len(out.Data.Data) == 0 {
		return listNowCoderContestProblemsHTML(contestID)
	}
	specs := make([]ContestProblemSpec, 0, len(out.Data.Data))
	for _, p := range out.Data.Data {
		pid := strings.TrimSpace(p.ProblemID.String())
		if pid == "" || pid == "0" {
			continue
		}
		label := strings.TrimSpace(p.Index)
		if label == "" {
			label = pid
		}
		specs = append(specs, ContestProblemSpec{
			Label:      label,
			ExternalID: pid,
			Title:      firstNonEmpty(strings.TrimSpace(p.ProblemName), label),
			URL:        "https://ac.nowcoder.com/acm/problem/" + pid,
			Platform:   spider.NowCoder,
		})
	}
	if len(specs) == 0 {
		return listNowCoderContestProblemsHTML(contestID)
	}
	return specs, nil
}

var reNowCoderProbID = regexp.MustCompile(`(?i)/acm/problem/(\d+)`)

func listNowCoderContestProblemsHTML(contestID string) ([]ContestProblemSpec, error) {
	url := fmt.Sprintf("https://ac.nowcoder.com/acm/contest/%s", contestID)
	body, code, err := contestHTTPGet(url)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("nowcoder contest status %d", code)
	}
	html := string(body)
	// 优先数字 problem id
	seen := map[string]struct{}{}
	var specs []ContestProblemSpec
	for _, m := range reNowCoderProbID.FindAllStringSubmatch(html, -1) {
		pid := m[1]
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		label := string(rune('A' + len(specs)))
		if len(specs) >= 26 {
			label = strconv.Itoa(len(specs) + 1)
		}
		specs = append(specs, ContestProblemSpec{
			Label:      label,
			ExternalID: pid,
			Title:      pid,
			URL:        "https://ac.nowcoder.com/acm/problem/" + pid,
			Platform:   spider.NowCoder,
		})
		if len(specs) >= 40 {
			break
		}
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("nowcoder: no problems for contest %s", contestID)
	}
	return specs, nil
}

// --- QOJ ---

var reQOJProblem = regexp.MustCompile(`(?i)/problem/(\d+)`)

func listQOJContestProblems(contestID string) ([]ContestProblemSpec, error) {
	url := fmt.Sprintf("https://qoj.ac/contest/%s", contestID)
	body, code, err := contestHTTPGet(url)
	if err != nil {
		return nil, err
	}
	if code != 200 {
		return nil, fmt.Errorf("qoj contest status %d", code)
	}
	seen := map[string]struct{}{}
	var specs []ContestProblemSpec
	for _, m := range reQOJProblem.FindAllStringSubmatch(string(body), -1) {
		pid := m[1]
		if _, ok := seen[pid]; ok {
			continue
		}
		seen[pid] = struct{}{}
		label := string(rune('A' + len(specs)))
		if len(specs) >= 26 {
			label = strconv.Itoa(len(specs) + 1)
		}
		specs = append(specs, ContestProblemSpec{
			Label:      label,
			ExternalID: pid,
			Title:      pid,
			URL:        "https://qoj.ac/problem/" + pid,
			Platform:   spider.QOJ,
		})
		if len(specs) >= 40 {
			break
		}
	}
	if len(specs) == 0 {
		return nil, fmt.Errorf("qoj: no problems for contest %s", contestID)
	}
	return specs, nil
}

// --- LeetCode ---

func listLeetCodeContestProblems(contestID string) ([]ContestProblemSpec, error) {
	// contestID 多为 weekly-contest-xxx / biweekly-contest-xxx
	slug := strings.TrimSpace(contestID)
	query := `{"query":"query contest($titleSlug:String!){contest(titleSlug:$titleSlug){title titleSlug questions{title titleSlug questionId}}}","variables":{"titleSlug":"` + escapeJSON(slug) + `"}}`
	req, err := http.NewRequest(http.MethodPost, "https://leetcode.cn/graphql", strings.NewReader(query))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", `Mozilla/5.0 (compatible; GoAlgo/1.0)`)
	client := &http.Client{Timeout: 25 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 2<<20))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("leetcode contest status %d", resp.StatusCode)
	}
	var out struct {
		Data struct {
			Contest *struct {
				Questions []struct {
					Title     string `json:"title"`
					TitleSlug string `json:"titleSlug"`
				} `json:"questions"`
			} `json:"contest"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &out); err != nil {
		return nil, err
	}
	if out.Data.Contest == nil || len(out.Data.Contest.Questions) == 0 {
		return nil, fmt.Errorf("leetcode: no questions for %s", slug)
	}
	specs := make([]ContestProblemSpec, 0, len(out.Data.Contest.Questions))
	for i, q := range out.Data.Contest.Questions {
		ts := strings.TrimSpace(q.TitleSlug)
		if ts == "" {
			continue
		}
		label := string(rune('A' + i))
		if i >= 26 {
			label = strconv.Itoa(i + 1)
		}
		specs = append(specs, ContestProblemSpec{
			Label:      label,
			ExternalID: ts,
			Title:      firstNonEmpty(strings.TrimSpace(q.Title), ts),
			URL:        "https://leetcode.cn/problems/" + ts + "/",
			Platform:   spider.LeetCode,
		})
	}
	return specs, nil
}

func escapeJSON(s string) string {
	b, _ := json.Marshal(s)
	if len(b) >= 2 {
		return string(b[1 : len(b)-1])
	}
	return s
}

// labelSortKey A < B < … < Z < AA
func labelSortKey(label string) int {
	label = strings.TrimSpace(strings.ToUpper(label))
	if label == "" {
		return 9999
	}
	if n, err := strconv.Atoi(label); err == nil {
		return 1000 + n
	}
	n := 0
	for _, r := range label {
		if r < 'A' || r > 'Z' {
			return 2000 + int(r)
		}
		n = n*26 + int(r-'A'+1)
	}
	return n
}

