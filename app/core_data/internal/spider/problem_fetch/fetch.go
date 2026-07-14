package problem_fetch

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"
	"unicode"

	"github.com/PuerkitoBio/goquery"
)

// FetchedContent 爬取结果
type FetchedContent struct {
	Title     string
	ContentMD string
}

// Fetch 按平台爬取题面 Markdown（LeetCode 调用方应跳过）
func Fetch(platform, externalID, problemURL string) (*FetchedContent, error) {
	switch platform {
	case "CodeForces":
		return fetchCodeforces(externalID, problemURL)
	case "AtCoder":
		return fetchAtCoder(problemURL)
	case "LuoGu":
		return fetchLuoGu(externalID, problemURL)
	case "QOJ":
		return fetchQOJ(externalID, problemURL)
	case "NowCoder":
		return fetchNowCoder(externalID, problemURL)
	case "LeetCode":
		return nil, fmt.Errorf("LeetCode 不支持爬取")
	default:
		if problemURL != "" {
			return fetchGeneric(problemURL)
		}
		return nil, fmt.Errorf("不支持的平台: %s", platform)
	}
}

func httpGet(rawURL string) (*http.Response, error) {
	req, err := http.NewRequest(http.MethodGet, rawURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8")
	req.Header.Set("Accept-Language", "zh-CN,zh;q=0.9,en;q=0.8")
	client := &http.Client{Timeout: 30 * time.Second}
	return client.Do(req)
}

func fetchCodeforces(externalID, problemURL string) (*FetchedContent, error) {
	if problemURL == "" {
		contest, index := splitCF(externalID)
		if contest == "" || index == "" {
			return nil, fmt.Errorf("无法解析 CF external_id: %s", externalID)
		}
		// problemset 路径有时比 contest 路径更稳定
		problemURL = fmt.Sprintf("https://codeforces.com/problemset/problem/%s/%s", contest, index)
	}
	resp, err := httpGet(problemURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("CF status %d: %s", resp.StatusCode, truncate(string(body), 200))
	}
	html := string(body)
	// Cloudflare 拦截
	if strings.Contains(html, "Just a moment") || strings.Contains(html, "cf-browser-verification") {
		return nil, fmt.Errorf("CF 被 Cloudflare 拦截，请稍后重试或换网络")
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}
	stmt := doc.Find("div.problem-statement").First()
	if stmt.Length() == 0 {
		return nil, fmt.Errorf("CF 未找到题面")
	}

	title := strings.TrimSpace(stmt.Find("div.header div.title").First().Text())
	if title == "" {
		title = strings.TrimSpace(doc.Find("div.title").First().Text())
	}
	if i := strings.Index(title, ". "); i >= 0 && i < 6 {
		title = strings.TrimSpace(title[i+2:])
	}

	// 按语义块转 Markdown，避免整段 Text 粘连
	var b strings.Builder
	b.WriteString("# ")
	b.WriteString(title)
	b.WriteString("\n\n")

	// 时间/内存限制
	if tl := strings.TrimSpace(stmt.Find("div.time-limit").Text()); tl != "" {
		b.WriteString("**")
		b.WriteString(normalizeSpace(tl))
		b.WriteString("**\n\n")
	}
	if ml := strings.TrimSpace(stmt.Find("div.memory-limit").Text()); ml != "" {
		b.WriteString("**")
		b.WriteString(normalizeSpace(ml))
		b.WriteString("**\n\n")
	}

	// 去掉 header（含 title/limits），保留正文结构
	clone := stmt.Clone()
	clone.Find("div.header").Remove()
	clone.Children().Each(func(_ int, s *goquery.Selection) {
		class, _ := s.Attr("class")
		class = strings.TrimSpace(class)
		switch {
		case class == "input-specification":
			b.WriteString("## 输入\n\n")
			b.WriteString(selectionToMD(s))
			b.WriteString("\n\n")
		case class == "output-specification":
			b.WriteString("## 输出\n\n")
			b.WriteString(selectionToMD(s))
			b.WriteString("\n\n")
		case class == "sample-tests":
			b.WriteString("## 样例\n\n")
			s.Find("div.sample-test").Each(func(i int, sample *goquery.Selection) {
				b.WriteString(fmt.Sprintf("### 样例 %d\n\n", i+1))
				in := sample.Find("div.input pre").First()
				out := sample.Find("div.output pre").First()
				if in.Length() > 0 {
					b.WriteString("**输入**\n\n```\n")
					b.WriteString(strings.TrimRight(in.Text(), "\n"))
					b.WriteString("\n```\n\n")
				}
				if out.Length() > 0 {
					b.WriteString("**输出**\n\n```\n")
					b.WriteString(strings.TrimRight(out.Text(), "\n"))
					b.WriteString("\n```\n\n")
				}
			})
		case class == "note":
			b.WriteString("## 说明\n\n")
			b.WriteString(selectionToMD(s))
			b.WriteString("\n\n")
		default:
			// 主描述段落
			txt := selectionToMD(s)
			if strings.TrimSpace(txt) != "" {
				b.WriteString(txt)
				b.WriteString("\n\n")
			}
		}
	})

	md := strings.TrimSpace(b.String())
	if md == "" || len(md) < 10 {
		// fallback
		md = "# " + title + "\n\n" + selectionToMD(clone)
	}
	// 清理 CF 公式 $$$...$$$ → $...$
	md = strings.ReplaceAll(md, "$$$", "$")
	md = collapseBlankLines(md)
	return &FetchedContent{Title: title, ContentMD: md}, nil
}

func selectionToMD(s *goquery.Selection) string {
	// 处理常见子节点
	var b strings.Builder
	s.Contents().Each(func(_ int, n *goquery.Selection) {
		if goquery.NodeName(n) == "#text" {
			b.WriteString(n.Text())
			return
		}
		name := goquery.NodeName(n)
		switch name {
		case "p":
			b.WriteString(normalizeSpace(n.Text()))
			b.WriteString("\n\n")
		case "ul", "ol":
			n.Find("li").Each(func(_ int, li *goquery.Selection) {
				b.WriteString("- ")
				b.WriteString(normalizeSpace(li.Text()))
				b.WriteString("\n")
			})
			b.WriteString("\n")
		case "pre":
			b.WriteString("\n```\n")
			b.WriteString(strings.TrimRight(n.Text(), "\n"))
			b.WriteString("\n```\n\n")
		case "div":
			// section-title 等
			if n.HasClass("section-title") {
				b.WriteString("### ")
				b.WriteString(normalizeSpace(n.Text()))
				b.WriteString("\n\n")
			} else {
				b.WriteString(selectionToMD(n))
			}
		case "span":
			// tex math often in span
			if alt, ok := n.Attr("class"); ok && strings.Contains(alt, "tex") {
				b.WriteString("$")
				b.WriteString(normalizeSpace(n.Text()))
				b.WriteString("$")
			} else {
				b.WriteString(n.Text())
			}
		case "br":
			b.WriteString("\n")
		default:
			b.WriteString(n.Text())
		}
	})
	return strings.TrimSpace(b.String())
}

func splitCF(externalID string) (contest, index string) {
	for i := 0; i < len(externalID); i++ {
		if (externalID[i] >= 'A' && externalID[i] <= 'Z') || (externalID[i] >= 'a' && externalID[i] <= 'z') {
			return externalID[:i], externalID[i:]
		}
	}
	return "", ""
}

func fetchAtCoder(problemURL string) (*FetchedContent, error) {
	if problemURL == "" {
		return nil, fmt.Errorf("AtCoder 缺少 URL")
	}
	resp, err := httpGet(problemURL)
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
	var mdParts []string
	doc.Find("#task-statement span.lang-en, #task-statement .lang-en").Each(func(_ int, s *goquery.Selection) {
		t := selectionToMD(s)
		if t != "" {
			mdParts = append(mdParts, t)
		}
	})
	if len(mdParts) == 0 {
		t := selectionToMD(doc.Find("#task-statement"))
		if t != "" {
			mdParts = append(mdParts, t)
		}
	}
	if len(mdParts) == 0 {
		return nil, fmt.Errorf("AtCoder 未找到题面")
	}
	md := "# " + title + "\n\n" + collapseBlankLines(strings.Join(mdParts, "\n\n"))
	return &FetchedContent{Title: title, ContentMD: md}, nil
}

func fetchLuoGu(externalID, problemURL string) (*FetchedContent, error) {
	if problemURL == "" {
		problemURL = "https://www.luogu.com.cn/problem/" + externalID
	}
	apiURL := problemURL
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
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36")
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
	return &FetchedContent{Title: title, ContentMD: collapseBlankLines(text)}, nil
}

func parseLuoGuJSON(body []byte) (*FetchedContent, error) {
	var root map[string]interface{}
	if err := json.Unmarshal(body, &root); err != nil {
		// fallback 旧逻辑
		s := string(body)
		return &FetchedContent{
			Title:     extractJSONString(s, `"title"`),
			ContentMD: firstNonEmpty(extractJSONString(s, `"description"`), extractJSONString(s, `"content"`), truncate(s, 8000)),
		}, nil
	}
	// 常见结构 currentData.problem
	title, desc := "", ""
	if cd, ok := root["currentData"].(map[string]interface{}); ok {
		if p, ok := cd["problem"].(map[string]interface{}); ok {
			if t, ok := p["title"].(string); ok {
				title = t
			}
			if d, ok := p["description"].(string); ok {
				desc = d
			}
			if desc == "" {
				if d, ok := p["content"].(string); ok {
					desc = d
				}
			}
		}
	}
	if title == "" {
		title = extractJSONString(string(body), `"title"`)
	}
	if desc == "" {
		desc = firstNonEmpty(extractJSONString(string(body), `"description"`), extractJSONString(string(body), `"content"`))
	}
	if desc == "" {
		return nil, fmt.Errorf("洛谷 JSON 无题面")
	}
	return &FetchedContent{Title: title, ContentMD: collapseBlankLines(desc)}, nil
}

func fetchNowCoder(externalID, problemURL string) (*FetchedContent, error) {
	if problemURL == "" {
		if externalID == "" {
			return nil, fmt.Errorf("NowCoder 缺少题面 URL 与 external_id")
		}
		// 数字题号 → 练习题；从 external_id 提取纯数字前缀
		id := externalID
		if !isAllDigits(id) {
			if m := regexp.MustCompile(`^\d+`).FindString(id); m != "" {
				id = m
			}
		}
		if isAllDigits(id) {
			problemURL = "https://ac.nowcoder.com/acm/problem/" + id
		} else {
			return nil, fmt.Errorf("NowCoder 竞赛题无稳定题面 URL，跳过爬取")
		}
	}
	resp, err := httpGet(problemURL)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	html := string(body)
	if resp.StatusCode != 200 {
		return nil, fmt.Errorf("NowCoder status %d", resp.StatusCode)
	}
	// 登录墙 / 空壳页
	if strings.Contains(html, "请先登录") || strings.Contains(html, "login-btn") && !strings.Contains(html, "subject-question") {
		return nil, fmt.Errorf("NowCoder 需要登录或被拦截")
	}
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return nil, err
	}
	title := strings.TrimSpace(doc.Find(".question-title h1, .question-title, .terminal-topic-title, .title, h1").First().Text())
	if title == "" {
		title = strings.TrimSpace(doc.Find("title").First().Text())
	}
	title = strings.TrimSpace(strings.Split(title, "_")[0])
	title = strings.TrimSpace(strings.Split(title, "-")[0])
	title = strings.TrimSpace(strings.Split(title, "|")[0])

	// 主内容：多 selector 兼容页面改版
	q := doc.Find("div.subject-question").First()
	if q.Length() == 0 {
		q = doc.Find(".question-content, .problem-content, .topic-sentence, #questionContent, .nc-post-content").First()
	}
	if q.Length() == 0 {
		return nil, fmt.Errorf("NowCoder 未找到题面 DOM")
	}

	// 将 equation img 替换为 $alt$
	replaceNowCoderMath(q)
	// br → newline
	q.Find("br").Each(func(_ int, br *goquery.Selection) {
		br.ReplaceWithHtml("\n")
	})

	var b strings.Builder
	if title != "" {
		b.WriteString("# ")
		b.WriteString(title)
		b.WriteString("\n\n")
	}
	// 主体
	bodyText := normalizeNowCoderText(q.Text())
	b.WriteString(bodyText)
	b.WriteString("\n\n")

	// 输入输出 / 样例：h2 / h3 / .question-oi-cont 等
	sectionSel := doc.Find("h2, h3, .question-oi-cont h2, .question-oi-cont h3")
	sectionSel.Each(func(_ int, h *goquery.Selection) {
		ht := strings.TrimSpace(h.Text())
		if ht == "" {
			return
		}
		next := h.Next()
		content := strings.TrimSpace(next.Text())
		if content == "" {
			content = strings.TrimSpace(h.Parent().Contents().Not("h2,h3").Text())
		}
		// 样例 pre：当前块或后续兄弟
		preNodes := h.Parent().Find("pre")
		if preNodes.Length() == 0 {
			preNodes = next.Find("pre")
		}
		if strings.Contains(ht, "输入") && !strings.Contains(ht, "输出") {
			b.WriteString("## 输入描述\n\n")
			b.WriteString(normalizeNowCoderText(content))
			b.WriteString("\n\n")
		} else if strings.Contains(ht, "输出") {
			b.WriteString("## 输出描述\n\n")
			b.WriteString(normalizeNowCoderText(content))
			b.WriteString("\n\n")
		} else if strings.Contains(ht, "示例") || strings.Contains(ht, "样例") {
			b.WriteString("## ")
			b.WriteString(ht)
			b.WriteString("\n\n")
			if preNodes.Length() > 0 {
				preNodes.Each(func(_ int, pre *goquery.Selection) {
					b.WriteString("```\n")
					b.WriteString(strings.TrimRight(pre.Text(), "\n"))
					b.WriteString("\n```\n\n")
				})
			} else if content != "" {
				b.WriteString(normalizeNowCoderText(content))
				b.WriteString("\n\n")
			}
		}
	})

	md := collapseBlankLines(strings.TrimSpace(b.String()))
	// 清理可能重复的标题行
	md = strings.ReplaceAll(md, "$$$", "$")
	if len(md) < 8 {
		return nil, fmt.Errorf("NowCoder 题面为空")
	}
	return &FetchedContent{Title: title, ContentMD: md}, nil
}

func replaceNowCoderMath(q *goquery.Selection) {
	q.Find("img").Each(func(_ int, img *goquery.Selection) {
		alt, _ := img.Attr("alt")
		src, _ := img.Attr("src")
		tex := alt
		if tex == "" && strings.Contains(src, "equation?tex=") {
			if u, err := url.Parse(src); err == nil {
				tex, _ = url.QueryUnescape(u.Query().Get("tex"))
			}
		}
		if tex == "" && strings.Contains(src, "tex=") {
			if u, err := url.Parse(src); err == nil {
				tex, _ = url.QueryUnescape(u.Query().Get("tex"))
			}
		}
		tex = strings.TrimSpace(tex)
		if tex == "" || tex == `\hspace{15pt}` || (strings.HasPrefix(tex, `\hspace`) && !strings.Contains(tex, "bullet")) {
			img.ReplaceWithHtml("")
			return
		}
		if strings.Contains(tex, `bullet`) {
			img.ReplaceWithHtml("\n- ")
			return
		}
		// 避免公式内未转义的 $ 破坏定界
		tex = strings.ReplaceAll(tex, "$", "")
		img.ReplaceWithHtml("$" + tex + "$")
	})
}

func fetchQOJ(externalID, problemURL string) (*FetchedContent, error) {
	if problemURL == "" && externalID != "" {
		problemURL = "https://qoj.ac/problem/" + externalID
	}
	return fetchGeneric(problemURL)
}

func fetchGeneric(problemURL string) (*FetchedContent, error) {
	if problemURL == "" {
		return nil, fmt.Errorf("empty url")
	}
	resp, err := httpGet(problemURL)
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
	doc.Find("script,style,nav,footer,header").Remove()
	text := collapseBlankLines(strings.TrimSpace(doc.Find("body").Text()))
	if text == "" {
		return nil, fmt.Errorf("empty page")
	}
	return &FetchedContent{Title: title, ContentMD: truncate(text, 15000)}, nil
}

func extractJSONString(s, key string) string {
	i := strings.Index(s, key)
	if i < 0 {
		return ""
	}
	rest := s[i+len(key):]
	ci := strings.Index(rest, ":")
	if ci < 0 {
		return ""
	}
	rest = strings.TrimSpace(rest[ci+1:])
	if !strings.HasPrefix(rest, `"`) {
		return ""
	}
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
			case 'u':
				// 简化：保留
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

func normalizeSpace(s string) string {
	return strings.Join(strings.Fields(s), " ")
}

func normalizeNowCoderText(s string) string {
	// 压缩空白，保留换行
	lines := strings.Split(s, "\n")
	var out []string
	for _, ln := range lines {
		ln = strings.TrimSpace(ln)
		if ln == "" {
			if len(out) > 0 && out[len(out)-1] != "" {
				out = append(out, "")
			}
			continue
		}
		out = append(out, ln)
	}
	return strings.TrimSpace(strings.Join(out, "\n"))
}

func collapseBlankLines(s string) string {
	re := regexp.MustCompile(`\n{3,}`)
	return strings.TrimSpace(re.ReplaceAllString(s, "\n\n"))
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if !unicode.IsDigit(r) {
			return false
		}
	}
	return true
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			return s
		}
	}
	return ""
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
