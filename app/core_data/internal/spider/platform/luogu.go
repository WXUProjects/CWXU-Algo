package platform

import (
	"bytes"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"fmt"
	"io"
	"mime/multipart"
	"net/http"
	"net/http/cookiejar"
	"strings"
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
			fmt.Println("login success:", body)
			return client, err
		}
		fmt.Printf("retry %d/%d, captcha=%s, resp=%s\n",
			attempt, maxRetry, code, body)
	}
	return nil, fmt.Errorf("login failed after %d retries", maxRetry)
}

func (lg NewLuoGu) FetchSubmitLog(userId int64, username string, needAll bool) ([]model.SubmitLog, error) {
	client, err := login("sanenchen", "sanenchen123")
	if err != nil {
		return nil, err
	}
	req, _ := http.NewRequest("GET", "https://www.luogu.com.cn/record/list?user=sanenchen&page=1", nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	rb, _ := io.ReadAll(resp.Body)
	fmt.Println(string(rb))
	return nil, nil
}

func (lg NewLuoGu) Name() string {
	return spider.LuoGu
}

func init() {
	spider.Register(NewLuoGu{})
}
