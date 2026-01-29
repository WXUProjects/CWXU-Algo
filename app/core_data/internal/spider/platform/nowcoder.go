package platform

import (
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

type Submission struct {
	RunID      string
	Problem    string
	Result     string
	Score      string
	TimeMS     string
	MemoryKB   string
	CodeLen    string
	Language   string
	SubmitTime string
}
type NewNowCoder struct{}

// getSubLogResp 获取submissionLog信息
func getSubLogResp(url string) (*goquery.Document, error) {
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
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("解析html失败")
	}
	return doc, nil
}

// analysisSubs 解析submission
func analysisSubs(doc *goquery.Document) []Submission {
	var subs []Submission
	doc.Find("table.table-hover tbody tr").Each(func(i int, tr *goquery.Selection) {
		tds := tr.Find("td")
		if tds.Length() < 9 {
			return
		}
		sub := Submission{
			RunID:      strings.TrimSpace(tds.Eq(0).Text()),
			Problem:    strings.TrimSpace(tds.Eq(1).Text()),
			Result:     strings.TrimSpace(tds.Eq(2).Text()),
			Score:      strings.TrimSpace(tds.Eq(3).Text()),
			TimeMS:     strings.TrimSpace(tds.Eq(4).Text()),
			MemoryKB:   strings.TrimSpace(tds.Eq(5).Text()),
			CodeLen:    strings.TrimSpace(tds.Eq(6).Text()),
			Language:   strings.TrimSpace(tds.Eq(7).Text()),
			SubmitTime: strings.TrimSpace(tds.Eq(8).Text()),
		}
		subs = append(subs, sub)
	})
	return subs
}

func (nc NewNowCoder) fetchSub(userId int64, username string, needAll bool) []model.SubmitLog {
	// ===== record 定义（必须是命名类型）=====
	type Record struct {
		Problem struct {
			QuestionNum string `json:"questionNum"`
			Title       string `json:"title"`
		} `json:"problem"`
		Submission struct {
			ID          int64 `json:"id"`
			CreatedDate int64 `json:"createdDate"`
		} `json:"submission"`
		Language string `json:"language"`
		Status   struct {
			Desc string `json:"desc"`
		} `json:"status"`
	}

	type Resp struct {
		Success bool `json:"success"`
		Data    struct {
			TotalPage int      `json:"totalPage"`
			Records   []Record `json:"records"`
		} `json:"data"`
	}

	const api = "https://gw-c.nowcoder.com/api/sparta/user/question-training/submission-history"

	pageSize := 50
	limit := 150
	if needAll {
		limit = math.MaxInt
	}

	doReq := func(page int) (*Resp, error) {
		body := fmt.Sprintf(`{"pageNo":%d,"pageSize":%d,"userId":%s}`, page, pageSize, username)
		req, err := http.NewRequest("POST", api, strings.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer resp.Body.Close()

		bs, err := io.ReadAll(resp.Body)
		if err != nil {
			return nil, err
		}

		var r Resp
		if err := json.Unmarshal(bs, &r); err != nil {
			return nil, err
		}
		return &r, nil
	}

	result := make([]model.SubmitLog, 0)

	handle := func(records []Record) bool {
		for _, it := range records {
			result = append(result, model.SubmitLog{
				UserID:   userId,
				Platform: spider.NowCoder,
				SubmitID: strconv.FormatInt(it.Submission.ID, 10),
				Contest:  "main|" + username,
				Problem:  it.Problem.QuestionNum + " " + it.Problem.Title,
				Lang:     it.Language,
				Status:   it.Status.Desc,
				Time:     time.Unix(it.Submission.CreatedDate/1000, 0),
			})
			if len(result) >= limit {
				return false
			}
		}
		return true
	}

	// ===== 第 1 页 =====
	first, err := doReq(1)
	if err != nil {
		return result
	}

	totalPage := first.Data.TotalPage
	if !handle(first.Data.Records) {
		return result
	}

	// ===== 后续分页 =====
	for page := 2; page <= totalPage; page++ {
		r, err := doReq(page)
		if err != nil {
			break
		}
		if len(r.Data.Records) == 0 {
			break
		}
		if !handle(r.Data.Records) {
			break
		}
	}

	return result
}

func (nc NewNowCoder) FetchSubmitLog(userId int64, username string, needAll bool) ([]model.SubmitLog, error) {
	url := fmt.Sprintf(
		"https://ac.nowcoder.com/acm/contest/profile/%s/practice-coding?pageSize=100&page=1",
		username,
	)
	doc, err := getSubLogResp(url)
	if err != nil {
		return nil, err
	}
	totalSubmit := ""
	doc.Find(".my-state-item").Each(func(i int, s *goquery.Selection) {
		label := strings.TrimSpace(s.Find("span").Text())
		if label == "次提交" {
			totalSubmit = strings.TrimSpace(s.Find(".state-num").Text())
		}
	})
	totalS, _ := strconv.Atoi(totalSubmit)
	// 先把当前这些数据怼进来
	var subs []Submission
	subs = append(subs, analysisSubs(doc)...)
	if needAll {
		// 再获取其他页的数据
		totPage := (totalS + 99) / 100
		for i := 2; i <= totPage; i++ {
			url := fmt.Sprintf(
				"https://ac.nowcoder.com/acm/contest/profile/%s/practice-coding?pageSize=100&page=%d",
				username, i,
			)
			doc, err := getSubLogResp(url)
			if err != nil {
				return nil, err
			}
			subs = append(subs, analysisSubs(doc)...)
		}
	}
	// 转为model类型
	res := make([]model.SubmitLog, 0)
	for _, v := range subs {
		loc, _ := time.LoadLocation("Asia/Shanghai")
		timeParse, _ := time.ParseInLocation("2006-01-02 15:04:05", v.SubmitTime, loc)
		tmp := model.SubmitLog{
			UserID:   userId,
			Platform: spider.NowCoder,
			SubmitID: v.RunID,
			Contest:  "",
			Problem:  v.Problem,
			Lang:     v.Language,
			Status:   v.Result,
			Time:     timeParse,
		}
		res = append(res, tmp)
	}
	res = append(res, nc.fetchSub(userId, username, needAll)...)
	return res, nil
}

func (nc NewNowCoder) Name() string {
	return spider.NowCoder
}
func init() {
	spider.Register(NewNowCoder{})
}
