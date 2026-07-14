package problem_fetch

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// FetchedContent 爬取结果
type FetchedContent struct {
	Title     string
	ContentMD string
}

// Fetch 按平台爬取题面 Markdown（LeetCode 调用方应跳过）
func Fetch(platform, externalID, url string) (*FetchedContent, error) {
	switch platform {
	case "CodeForces":
		return fetchCodeforces(externalID, url)
	case "AtCoder":
		return fetchAtCoder(url)
	case "LuoGu":
		return fetchLuoGu(externalID, url)
	case "QOJ":
		return fetchGeneric(url)
	case "NowCoder":
		if url == "" {
			return nil, fmt.Errorf("NowCoder 缺少题面 URL，跳过爬取")
		}
		return fetchGeneric(url)
	case "LeetCode":
		return nil, fmt.Errorf("LeetCode 不支持爬取")
	default:
		if url != "" {
			return fetchGeneric(url)
		}
		return nil, fmt.Errorf("不支持的平台: %s", platform)
	}
}

func httpGet(url string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; CWXU-Algo-ProblemBot/1.0)")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/json")
	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}

func fetchCodeforces(externalID, url string) (*FetchedContent, error) {
	if url == "" {
		// externalID 形如 1843C
		contest, index := splitCF(externalID)
		if contest == "" || index == "" {
			return nil, fmt.Errorf("无法解析 CF external_id: %s", externalID)
		}
		url = fmt.Sprintf("https://codeforces.com/contest/%s/problem/%s", contest, index)
	}
	resp, err := httpGet(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("CF status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}
	title := strings.TrimSpace(doc.Find("div.title").First().Text())
	if title == "" {
		title = strings.TrimSpace(doc.Find("div.header div.title").First().Text())
	}
	// 去掉前缀 "C. "
	if i := strings.Index(title, ". "); i >= 0 && i < 5 {
		title = strings.TrimSpace(title[i+2:])
	}
	var parts []string
	doc.Find("div.problem-statement").Each(func(_ int, s *goquery.Selection) {
		// 去掉 input/output 文件名等无关
		s.Find("div.header").Remove()
		text := strings.TrimSpace(s.Text())
		if text != "" {
			parts = append(parts, text)
		}
	})
	if len(parts) == 0 {
		// fallback html
		html, _ := doc.Find("div.problem-statement").Html()
		if html == "" {
			return nil, fmt.Errorf("CF 未找到题面")
		}
		parts = append(parts, htmlToRoughMD(html))
	}
	md := strings.Join(parts, "\n\n")
	return &FetchedContent{Title: title, ContentMD: md}, nil
}

func splitCF(externalID string) (contest, index string) {
	// 1843C / 1843C1 / 1A
	for i := 0; i < len(externalID); i++ {
		if externalID[i] >= 'A' && externalID[i] <= 'Z' || externalID[i] >= 'a' && externalID[i] <= 'z' {
			return externalID[:i], externalID[i:]
		}
	}
	return "", ""
}

func fetchAtCoder(url string) (*FetchedContent, error) {
	if url == "" {
		return nil, fmt.Errorf("AtCoder 缺少 URL")
	}
	resp, err := httpGet(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("AtCoder status %d", resp.StatusCode)
	}
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}
	title := strings.TrimSpace(doc.Find("span.h2").First().Text())
	if title == "" {
		title = strings.TrimSpace(doc.Find("h2").First().Text())
	}
	// 优先英文题面
	var mdParts []string
	doc.Find("#task-statement span.lang-en, #task-statement .lang-en").Each(func(_ int, s *goquery.Selection) {
		t := strings.TrimSpace(s.Text())
		if t != "" {
			mdParts = append(mdParts, t)
		}
	})
	if len(mdParts) == 0 {
		t := strings.TrimSpace(doc.Find("#task-statement").Text())
		if t != "" {
			mdParts = append(mdParts, t)
		}
	}
	if len(mdParts) == 0 {
		return nil, fmt.Errorf("AtCoder 未找到题面")
	}
	return &FetchedContent{Title: title, ContentMD: strings.Join(mdParts, "\n\n")}, nil
}

func fetchLuoGu(externalID, url string) (*FetchedContent, error) {
	if url == "" {
		url = "https://www.luogu.com.cn/problem/" + externalID
	}
	// 洛谷需要 _contentOnly 有时更友好
	apiURL := url
	if !strings.Contains(apiURL, "_contentOnly") {
		if strings.Contains(apiURL, "?") {
			apiURL += "&_contentOnly=1"
		} else {
			apiURL += "?_contentOnly=1"
		}
	}
	req, err := http.NewRequest(http.MethodGet, apiURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (compatible; CWXU-Algo-ProblemBot/1.0)")
	req.Header.Set("X-Requested-With", "XMLHttpRequest")
	client := &http.Client{Timeout: 30 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("洛谷 status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	// 尝试 JSON
	if strings.Contains(resp.Header.Get("Content-Type"), "json") || strings.HasPrefix(strings.TrimSpace(string(body)), "{") {
		return parseLuoGuJSON(body)
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(body)))
	if err != nil {
		return nil, err
	}
	title := strings.TrimSpace(doc.Find("h1").First().Text())
	text := strings.TrimSpace(doc.Find("#app").Text())
	if text == "" {
		text = strings.TrimSpace(doc.Text())
	}
	if text == "" {
		return nil, fmt.Errorf("洛谷未找到题面")
	}
	return &FetchedContent{Title: title, ContentMD: text}, nil
}

func parseLuoGuJSON(body []byte) (*FetchedContent, error) {
	// 轻量解析，避免引入复杂结构
	s := string(body)
	title := extractJSONString(s, `"title"`)
	// content 可能在 currentData.problem.content
	content := extractJSONString(s, `"description"`)
	if content == "" {
		content = extractJSONString(s, `"content"`)
	}
	if content == "" {
		// 整段作为文本
		content = truncate(s, 8000)
	}
	// 洛谷 description 常为 markdown
	return &FetchedContent{Title: title, ContentMD: content}, nil
}

func extractJSONString(s, key string) string {
	i := strings.Index(s, key)
	if i < 0 {
		return ""
	}
	rest := s[i+len(key):]
	// 找 :
	ci := strings.Index(rest, ":")
	if ci < 0 {
		return ""
	}
	rest = strings.TrimSpace(rest[ci+1:])
	if !strings.HasPrefix(rest, `"`) {
		return ""
	}
	// 简单反转义扫描
	var b strings.Builder
	escaped := false
	for i := 1; i < len(rest); i++ {
		c := rest[i]
		if escaped {
			switch c {
			case 'n':
				b.WriteByte('\n')
			case 't':
				b.WriteByte('\t')
			case 'r':
				b.WriteByte('\r')
			case '"', '\\', '/':
				b.WriteByte(c)
			default:
				b.WriteByte(c)
			}
			escaped = false
			continue
		}
		if c == '\\' {
			escaped = true
			continue
		}
		if c == '"' {
			break
		}
		b.WriteByte(c)
	}
	return b.String()
}

func fetchGeneric(url string) (*FetchedContent, error) {
	if url == "" {
		return nil, fmt.Errorf("empty url")
	}
	resp, err := httpGet(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("status %d", resp.StatusCode)
	}
	doc, err := goquery.NewDocumentFromReader(resp.Body)
	if err != nil {
		return nil, err
	}
	title := strings.TrimSpace(doc.Find("h1").First().Text())
	if title == "" {
		title = strings.TrimSpace(doc.Find("title").Text())
	}
	// 去掉 script/style
	doc.Find("script,style,nav,footer").Remove()
	text := strings.TrimSpace(doc.Find("body").Text())
	if text == "" {
		return nil, fmt.Errorf("empty page")
	}
	return &FetchedContent{Title: title, ContentMD: truncate(text, 15000)}, nil
}

func htmlToRoughMD(html string) string {
	// 极简：去标签
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return html
	}
	return strings.TrimSpace(doc.Text())
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
