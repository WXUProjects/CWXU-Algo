package platform

import (
	"cwxu-algo/app/core_data/internal/data/gorm/model"
	"cwxu-algo/app/core_data/internal/spider"
	"fmt"
	"io"
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
	return res, nil
}

func (nc NewNowCoder) Name() string {
	return spider.NowCoder
}
func init() {
	spider.Register(NewNowCoder{})
}
