package platform

import (
	"bytes"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"encoding/json"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
)

type NewLuoGu struct{}

func ocrImage(client *http.Client, url string, img []byte) (string, error) {
	var buf bytes.Buffer
	w := multipart.NewWriter(&buf)

	part, err := w.CreateFormFile("image", "captcha.png")
	if err != nil {
		return "", err
	}
	if _, err = part.Write(img); err != nil {
		return "", err
	}
	w.Close()
	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return "", err
	}
	req.Header.Set("Content-Type", w.FormDataContentType())
	resp, err := client.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
func doLogin(
	client *http.Client,
	url, username, password, captcha string,
) (success bool, body string, err error) {
	payload := fmt.Sprintf(
		`{"username":"%s","password":"%s","captcha":"%s"}`,
		username, password, captcha,
	)
	resp, err := client.Post(
		url,
		"application/json",
		bytes.NewReader([]byte(payload)),
	)
	if err != nil {
		return false, "", err
	}
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		return false, "", err
	}
	body = string(b)
	// 只要出现 errorCode，就认为失败，交给外层重试
	if strings.Contains(body, "errorCode") {
		return false, body, nil
	}
	return true, body, nil
}

func login(username, password string) (*http.Client, error) {
	const (
		captchaURL = "https://www.luogu.com.cn/lg4/captcha"
		ocrURL     = "https://api.alistgo.com/ocr/file"
		loginURL   = "https://www.luogu.com.cn/do-auth/password"
		maxRetry   = 20
	)

	jar, _ := cookiejar.New(nil)
	client := &http.Client{Jar: jar}

	for attempt := 1; attempt <= maxRetry; attempt++ {
		// 1. 拉验证码（cookie 在这里生成）
		resp, err := client.Get(captchaURL)
		if err != nil {
			return nil, err
		}
		imgBytes, err := io.ReadAll(resp.Body)
		resp.Body.Close()
		if err != nil {
			return nil, err
		}
		// 2. OCR 识别验证码
		code, err := ocrImage(client, ocrURL, imgBytes)
		if err != nil {
			return nil, err
		}
		code = strings.TrimSpace(code)
		// 3. 发起登录
		ok, body, err := doLogin(client, loginURL, username, password, code)
		if err != nil {
			return nil, err
		}
		// 没有 errorCode，说明成功
		if ok {
			log.Info("login success:", body)
			return client, err
		}
		log.Info("msg", "login retry", "attempt", attempt, "total", maxRetry, "captcha", code)
	}
	return nil, fmt.Errorf("login failed after %d retries", maxRetry)
}

type Injection struct {
	Code        int `json:"code"`
	CurrentData struct {
		Records struct {
			Result  []Record `json:"result"`
			PerPage int      `json:"perPage"`
			Count   int      `json:"count"`
		} `json:"records"`
	} `json:"currentData"`
}

type Record struct {
	ID         int64 `json:"id"`
	SubmitTime int64 `json:"submitTime"`
	Status     int   `json:"status"`
	Score      *int  `json:"score"`
	Time       int   `json:"time"`
	Memory     int   `json:"memory"`
	Language   int   `json:"language"`
	Problem    struct {
		Pid        string `json:"pid"`
		Title      string `json:"title"`
		Difficulty int    `json:"difficulty"`
	} `json:"problem"`
}

func parseLuoGuHTML(html string) (*Injection, error) {
	// 抠 decodeURIComponent 里的字符串
	re := regexp.MustCompile(`window\._feInjection\s*=\s*JSON\.parse\(decodeURIComponent\("(.+?)"\)\)`)
	m := re.FindStringSubmatch(html)
	if len(m) != 2 {
		return nil, fmt.Errorf("未找到 _feInjection")
	}

	// URL 解码
	decoded, err := url.QueryUnescape(m[1])
	if err != nil {
		return nil, err
	}

	var inj Injection
	if err := json.Unmarshal([]byte(decoded), &inj); err != nil {
		return nil, err
	}

	return &inj, nil
}

func (lg NewLuoGu) FetchSubmitLog(userId int64, username string, needAll bool) ([]model.SubmitLog, error) {
	baseUrl := fmt.Sprintf("https://www.luogu.com.cn/record/list?user=%s&page=", username)
	client, err := login("sanenchen", "sanenchen123")
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequest("GET", baseUrl+"1", nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	var subs []Record
	inj, err := parseLuoGuHTML(string(rb))
	if err != nil {
		return nil, err
	}
	subs = inj.CurrentData.Records.Result
	if needAll {
		for i := 2; i <= inj.CurrentData.Records.Count/inj.CurrentData.Records.PerPage+1; i++ {
			req, _ := http.NewRequest("GET", baseUrl+fmt.Sprint(i), nil)
			resp, err := client.Do(req)
			if err != nil {
				return nil, err
			}
			defer resp.Body.Close()
			rb, _ := io.ReadAll(resp.Body)
			inj, err := parseLuoGuHTML(string(rb))
			if err != nil {
				return nil, err
			}
			subs = append(subs, inj.CurrentData.Records.Result...)
		}
	}
	fmt.Println(subs)
	var res []model.SubmitLog
	for _, sub := range subs {
		var status, lang string
		if sub.Status != 12 {
			status = "WA"
		} else {
			status = "AC"
		}
		if sub.Language == 34 {
			lang = "C++"
		} else {
			lang = "Others"
		}
		res = append(res, model.SubmitLog{
			UserID:   userId,
			Platform: spider.LuoGu,
			SubmitID: fmt.Sprint(sub.ID),
			Problem:  sub.Problem.Pid + " " + sub.Problem.Title,
			Lang:     lang,
			Status:   status,
			Time:     time.Unix(sub.SubmitTime, 0),
		})
	}
	return res, nil
}

func (lg NewLuoGu) Name() string {
	return spider.LuoGu
}

func init() {
	spider.Register(NewLuoGu{})
}
