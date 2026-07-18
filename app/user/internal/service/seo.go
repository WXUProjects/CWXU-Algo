package service

import (
	"fmt"
	"html"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"cwxu-algo/app/user/internal/biz/blogaccess"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"

	khttp "github.com/go-kratos/kratos/v2/transport/http"
)

const (
	defaultPublicOrigin = "https://algo.zhiyuansofts.cn"
	defaultSiteTitle    = "GoAlgo"
	defaultSiteDesc     = "算法训练与竞赛社区：题库、博客、比赛与数据统计。"
	maxSEODescRunes     = 160
	maxSitemapArticles  = 5000
)

// SEOService serves crawler-friendly HTML meta for SPA public routes.
// Does not increment blog view_count.
type SEOService struct {
	data *data.Data
}

func NewSEOService(d *data.Data) *SEOService {
	return &SEOService{data: d}
}

// RegisterSEORoutes public SEO endpoints (no JWT).
func RegisterSEORoutes(srv *khttp.Server, ss *SEOService) {
	r := srv.Route("/")
	r.GET("/v1/user/seo/html", ss.handleHTML)
	r.GET("/v1/user/seo/meta", ss.handleMetaJSON)
	r.GET("/v1/user/seo/sitemap.xml", ss.handleSitemap)
}

type seoPage struct {
	Title       string
	Description string
	Image       string
	URL         string
	Type        string // website | article | profile
	SiteName    string
	// Optional body for noscript / first paint
	BodyTitle string
	BodyText  string
	Author    string
}

func publicOrigin(req *http.Request) string {
	if v := strings.TrimSpace(os.Getenv("CWXU_PUBLIC_ORIGIN")); v != "" {
		return strings.TrimRight(v, "/")
	}
	// Production default: always absolute HTTPS site origin (share crawlers require https images).
	if req != nil {
		host := strings.TrimSpace(req.Header.Get("X-Forwarded-Host"))
		if host == "" {
			host = strings.TrimSpace(req.Host)
		}
		// Strip port for public site URL
		if i := strings.IndexByte(host, ':'); i > 0 && !strings.Contains(host, "]") {
			h := host[:i]
			if h == "algo.zhiyuansofts.cn" || strings.HasSuffix(h, ".zhiyuansofts.cn") {
				return "https://" + h
			}
		}
		if host == "algo.zhiyuansofts.cn" || strings.HasSuffix(host, ".zhiyuansofts.cn") {
			return "https://" + host
		}
		if xf := strings.TrimSpace(req.Header.Get("X-Forwarded-Proto")); xf != "" && host != "" {
			// Prefer https when proto missing or internal http hop
			if xf == "https" || xf == "http" {
				if xf == "http" && (host == "algo.zhiyuansofts.cn" || strings.Contains(host, "zhiyuansofts")) {
					return "https://" + host
				}
				return xf + "://" + host
			}
		}
		if host != "" && !strings.Contains(host, "localhost") && !strings.HasPrefix(host, "127.") {
			if req.TLS != nil {
				return "https://" + host
			}
			return "https://" + host
		}
	}
	return defaultPublicOrigin
}

func (s *SEOService) siteBrand() (title, logo, favicon string) {
	title = defaultSiteTitle
	var row model.SiteConfig
	if s.data != nil && s.data.DB != nil {
		if err := s.data.DB.Select("site_title", "site_logo", "favicon").First(&row, 1).Error; err == nil {
			if t := strings.TrimSpace(row.SiteTitle); t != "" {
				title = t
			}
			logo = strings.TrimSpace(row.SiteLogo)
			favicon = strings.TrimSpace(row.Favicon)
		}
	}
	return title, logo, favicon
}

func absURL(origin, pathOrURL string) string {
	pathOrURL = strings.TrimSpace(pathOrURL)
	if pathOrURL == "" {
		return ""
	}
	if strings.HasPrefix(pathOrURL, "http://") || strings.HasPrefix(pathOrURL, "https://") {
		return pathOrURL
	}
	if !strings.HasPrefix(pathOrURL, "/") {
		pathOrURL = "/" + pathOrURL
	}
	return strings.TrimRight(origin, "/") + pathOrURL
}

func clipDesc(s string, max int) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return ""
	}
	// strip simple markdown noise for meta
	s = regexp.MustCompile("`+").ReplaceAllString(s, "")
	s = regexp.MustCompile(`\[([^\]]+)\]\([^)]+\)`).ReplaceAllString(s, "$1")
	s = regexp.MustCompile(`[#>*_~\-]+`).ReplaceAllString(s, " ")
	s = regexp.MustCompile(`\s+`).ReplaceAllString(s, " ")
	s = strings.TrimSpace(s)
	if max <= 0 {
		max = maxSEODescRunes
	}
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max-1]) + "…"
}

func normalizePath(raw string) string {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "/"
	}
	if u, err := url.Parse(raw); err == nil {
		if u.Path != "" {
			raw = u.Path
			if u.RawQuery != "" && (strings.HasPrefix(u.Path, "/profile") || strings.Contains(u.RawQuery, "id=")) {
				// keep query for profile?id=
				raw = u.Path + "?" + u.RawQuery
			}
		}
	}
	if !strings.HasPrefix(raw, "/") {
		raw = "/" + raw
	}
	// strip fragment
	if i := strings.IndexByte(raw, '#'); i >= 0 {
		raw = raw[:i]
	}
	return raw
}

func (s *SEOService) resolvePage(req *http.Request, path string) seoPage {
	origin := publicOrigin(req)
	siteTitle, siteLogo, siteFav := s.siteBrand()
	path = normalizePath(path)

	// split path and query
	pathOnly := path
	q := url.Values{}
	if i := strings.IndexByte(path, '?'); i >= 0 {
		pathOnly = path[:i]
		q, _ = url.ParseQuery(path[i+1:])
	}
	pathOnly = strings.TrimRight(pathOnly, "/")
	if pathOnly == "" {
		pathOnly = "/"
	}

	defaultImg := absURL(origin, firstNonEmpty(siteLogo, siteFav, "/favicon.png"))
	page := seoPage{
		Title:       siteTitle,
		Description: defaultSiteDesc,
		Image:       defaultImg,
		URL:         absURL(origin, pathOnly),
		Type:        "website",
		SiteName:    siteTitle,
		BodyTitle:   siteTitle,
		BodyText:    defaultSiteDesc,
	}

	// /blog/:user/:slug  (article) — not manage/categories/archives/about
	if m := regexp.MustCompile(`^/blog/([^/]+)/([^/]+)$`).FindStringSubmatch(pathOnly); len(m) == 3 {
		user, slug := m[1], m[2]
		if slug != "manage" && slug != "categories" && slug != "archives" && slug != "about" {
			if p, ok := s.metaBlogArticle(origin, siteTitle, defaultImg, user, slug); ok {
				return p
			}
		}
	}

	// /blog/:user or secondary pages
	if m := regexp.MustCompile(`^/blog/([^/]+)(?:/(categories|archives|about))?$`).FindStringSubmatch(pathOnly); len(m) >= 2 {
		if p, ok := s.metaBlogHome(origin, siteTitle, defaultImg, m[1], ""); ok {
			if len(m) >= 3 && m[2] != "" {
				label := map[string]string{"categories": "分类", "archives": "归档", "about": "关于"}[m[2]]
				if label != "" {
					p.Title = label + " - " + p.Title
					p.BodyTitle = p.Title
				}
			}
			return p
		}
	}

	// /blog-plaza
	if pathOnly == "/blog-plaza" {
		page.Title = "博客广场 - " + siteTitle
		page.Description = "浏览社区公开博客与题解分享。"
		page.BodyTitle = page.Title
		page.BodyText = page.Description
		page.URL = absURL(origin, "/blog-plaza")
		return page
	}

	// /question-bank/detail/:id
	if m := regexp.MustCompile(`^/question-bank/detail/(\d+)$`).FindStringSubmatch(pathOnly); len(m) == 2 {
		if id, err := strconv.ParseUint(m[1], 10, 64); err == nil {
			if p, ok := s.metaProblem(origin, siteTitle, defaultImg, uint(id)); ok {
				return p
			}
		}
	}

	// /profile?id= or /profile
	if pathOnly == "/profile" {
		if idStr := strings.TrimSpace(q.Get("id")); idStr != "" {
			if id, err := strconv.ParseUint(idStr, 10, 64); err == nil {
				if p, ok := s.metaProfile(origin, siteTitle, defaultImg, uint(id), ""); ok {
					return p
				}
			}
		}
		page.Title = "个人资料 - " + siteTitle
		page.URL = absURL(origin, "/profile")
		return page
	}

	// /p/:slug paste
	if m := regexp.MustCompile(`^/p/([A-Za-z0-9]+)$`).FindStringSubmatch(pathOnly); len(m) == 2 {
		if p, ok := s.metaPaste(origin, siteTitle, defaultImg, m[1]); ok {
			return p
		}
		page.Title = "粘贴板 - " + siteTitle
		page.Description = "内容不存在或已过期。"
		page.BodyTitle = page.Title
		page.BodyText = page.Description
		page.URL = absURL(origin, pathOnly)
		return page
	}

	// /tools/paste
	if pathOnly == "/tools/paste" {
		page.Title = "粘贴板 - " + siteTitle
		page.Description = "发布与分享代码、文本片段。"
		page.BodyTitle = page.Title
		page.BodyText = page.Description
		page.URL = absURL(origin, "/tools/paste")
		return page
	}

	// home & defaults
	if pathOnly == "/" {
		page.Title = siteTitle
		page.URL = absURL(origin, "/")
		return page
	}

	// generic: keep site defaults with path canonical
	page.URL = absURL(origin, pathOnly)
	return page
}

func firstNonEmpty(ss ...string) string {
	for _, s := range ss {
		if strings.TrimSpace(s) != "" {
			return strings.TrimSpace(s)
		}
	}
	return ""
}

func (s *SEOService) metaBlogArticle(origin, siteTitle, defaultImg, username, slug string) (seoPage, bool) {
	if s.data == nil || s.data.DB == nil {
		return seoPage{}, false
	}
	var u model.User
	if err := s.data.DB.Select("id", "username", "name", "avatar").Where("username = ?", username).First(&u).Error; err != nil {
		return seoPage{}, false
	}
	var a model.BlogArticle
	if err := s.data.DB.Where("user_id = ? AND slug = ?", u.ID, slug).First(&a).Error; err != nil {
		return seoPage{}, false
	}
	d := blogaccess.Evaluate(blogaccess.ArticleAccess{
		Visibility:  a.Visibility,
		OwnerID:     a.UserID,
		HasPassword: a.PasswordHash != "",
	}, 0, false)
	if !d.CanSeeMeta {
		return seoPage{}, false
	}

	authorName := firstNonEmpty(u.Name, u.Username)
	desc := clipDesc(a.Summary, maxSEODescRunes)
	if desc == "" && d.CanSeeBody {
		desc = clipDesc(a.Content, maxSEODescRunes)
	}
	if desc == "" {
		desc = authorName + " 的文章 · " + siteTitle
	}
	img := absURL(origin, firstNonEmpty(a.CoverURL, u.Avatar, defaultImg))
	path := fmt.Sprintf("/blog/%s/%s", u.Username, a.Slug)
	title := a.Title
	if authorName != "" {
		title = a.Title + " - " + authorName
	}
	return seoPage{
		Title:       title,
		Description: desc,
		Image:       img,
		URL:         absURL(origin, path),
		Type:        "article",
		SiteName:    siteTitle,
		BodyTitle:   a.Title,
		BodyText:    desc,
		Author:      authorName,
	}, true
}

func (s *SEOService) metaBlogHome(origin, siteTitle, defaultImg, username, _ string) (seoPage, bool) {
	if s.data == nil || s.data.DB == nil {
		return seoPage{}, false
	}
	var u model.User
	if err := s.data.DB.Select("id", "username", "name", "avatar").Where("username = ?", username).First(&u).Error; err != nil {
		return seoPage{}, false
	}
	authorName := firstNonEmpty(u.Name, u.Username)
	subtitle := ""
	var cfg model.BlogSiteConfig
	if err := s.data.DB.Where("user_id = ?", u.ID).First(&cfg).Error; err == nil {
		subtitle = strings.TrimSpace(cfg.Subtitle)
	}
	desc := clipDesc(firstNonEmpty(subtitle, authorName+" 的算法博客 · "+siteTitle), maxSEODescRunes)
	img := absURL(origin, firstNonEmpty(u.Avatar, defaultImg))
	path := "/blog/" + u.Username
	return seoPage{
		Title:       authorName + " 的博客 - " + siteTitle,
		Description: desc,
		Image:       img,
		URL:         absURL(origin, path),
		Type:        "profile",
		SiteName:    siteTitle,
		BodyTitle:   authorName + " 的博客",
		BodyText:    desc,
		Author:      authorName,
	}, true
}

func (s *SEOService) metaProfile(origin, siteTitle, defaultImg string, id uint, username string) (seoPage, bool) {
	if s.data == nil || s.data.DB == nil {
		return seoPage{}, false
	}
	var u model.User
	q := s.data.DB.Select("id", "username", "name", "avatar")
	var err error
	if id > 0 {
		err = q.First(&u, id).Error
	} else if username != "" {
		err = q.Where("username = ?", username).First(&u).Error
	} else {
		return seoPage{}, false
	}
	if err != nil {
		return seoPage{}, false
	}
	name := firstNonEmpty(u.Name, u.Username)
	desc := name + " 的个人资料 · " + siteTitle
	img := absURL(origin, firstNonEmpty(u.Avatar, defaultImg))
	path := fmt.Sprintf("/profile?id=%d", u.ID)
	return seoPage{
		Title:       name + " - " + siteTitle,
		Description: desc,
		Image:       img,
		URL:         absURL(origin, path),
		Type:        "profile",
		SiteName:    siteTitle,
		BodyTitle:   name,
		BodyText:    desc,
		Author:      name,
	}, true
}

// core problem row (read-only from CoreDB when available)
type seoProblemRow struct {
	ID       uint   `gorm:"column:id"`
	Title    string `gorm:"column:title"`
	Platform string `gorm:"column:platform"`
	Diff     string `gorm:"column:difficulty"`
}

func (seoProblemRow) TableName() string { return "problems" }

func (s *SEOService) metaPaste(origin, siteTitle, defaultImg, slug string) (seoPage, bool) {
	if s.data == nil || s.data.DB == nil || slug == "" {
		return seoPage{}, false
	}
	var p model.Paste
	if err := s.data.DB.Where("slug = ?", slug).First(&p).Error; err != nil {
		return seoPage{}, false
	}
	if p.ExpireAt != nil && p.ExpireAt.Before(time.Now()) {
		return seoPage{}, false
	}
	title := strings.TrimSpace(p.Title)
	if title == "" {
		lang := strings.TrimSpace(p.Language)
		if lang == "" || lang == "text" {
			title = "粘贴板分享"
		} else {
			title = lang + " 代码片段"
		}
	}
	desc := clipDesc(p.Content, maxSEODescRunes)
	if desc == "" {
		desc = "在 " + siteTitle + " 查看这段分享内容"
	}
	lang := strings.TrimSpace(p.Language)
	if lang != "" && lang != "text" {
		desc = lang + " · " + desc
	}
	path := "/p/" + p.Slug
	return seoPage{
		Title:       title + " - " + siteTitle,
		Description: desc,
		Image:       defaultImg,
		URL:         absURL(origin, path),
		Type:        "article",
		SiteName:    siteTitle,
		BodyTitle:   title,
		BodyText:    desc,
	}, true
}

func (s *SEOService) metaProblem(origin, siteTitle, defaultImg string, id uint) (seoPage, bool) {
	if s.data == nil || s.data.CoreDB == nil {
		return seoPage{}, false
	}
	var p seoProblemRow
	if err := s.data.CoreDB.Select("id", "title", "platform", "difficulty").First(&p, id).Error; err != nil {
		return seoPage{}, false
	}
	title := strings.TrimSpace(p.Title)
	if title == "" {
		title = fmt.Sprintf("题目 #%d", p.ID)
	}
	parts := []string{}
	if pl := strings.TrimSpace(p.Platform); pl != "" {
		parts = append(parts, pl)
	}
	if d := strings.TrimSpace(p.Diff); d != "" {
		parts = append(parts, "难度 "+d)
	}
	parts = append(parts, siteTitle)
	desc := strings.Join(parts, " · ")
	path := fmt.Sprintf("/question-bank/detail/%d", p.ID)
	return seoPage{
		Title:       title + " - " + siteTitle,
		Description: desc,
		Image:       defaultImg,
		URL:         absURL(origin, path),
		Type:        "article",
		SiteName:    siteTitle,
		BodyTitle:   title,
		BodyText:    desc,
	}, true
}

func (s *SEOService) handleMetaJSON(ctx khttp.Context) error {
	req := ctx.Request()
	path := req.URL.Query().Get("path")
	if path == "" {
		path = req.URL.Query().Get("url")
	}
	p := s.resolvePage(req, path)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"title":       p.Title,
			"description": p.Description,
			"image":       p.Image,
			"url":         p.URL,
			"type":        p.Type,
			"siteName":    p.SiteName,
			"author":      p.Author,
		},
	})
	return nil
}

func (s *SEOService) handleHTML(ctx khttp.Context) error {
	req := ctx.Request()
	path := req.URL.Query().Get("path")
	if path == "" {
		// allow path from header when nginx proxies original URI
		path = req.Header.Get("X-Original-URI")
	}
	if path == "" {
		path = req.URL.Query().Get("url")
	}
	p := s.resolvePage(req, path)
	w := ctx.Response()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=120")
	w.Header().Set("X-Robots-Tag", "all")
	w.WriteHeader(200)
	_, _ = w.Write([]byte(renderSEOHTML(p)))
	return nil
}

func renderSEOHTML(p seoPage) string {
	esc := html.EscapeString
	title := esc(p.Title)
	desc := esc(p.Description)
	img := esc(p.Image)
	pageURL := esc(p.URL)
	site := esc(p.SiteName)
	ogType := esc(firstNonEmpty(p.Type, "website"))
	bodyTitle := esc(firstNonEmpty(p.BodyTitle, p.Title))
	bodyText := esc(firstNonEmpty(p.BodyText, p.Description))
	author := esc(p.Author)

	var b strings.Builder
	b.WriteString("<!doctype html>\n<html lang=\"zh-CN\">\n<head>\n")
	b.WriteString("<meta charset=\"UTF-8\" />\n")
	b.WriteString("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0, viewport-fit=cover\" />\n")
	b.WriteString("<title>")
	b.WriteString(title)
	b.WriteString("</title>\n")
	b.WriteString("<meta name=\"description\" content=\"")
	b.WriteString(desc)
	b.WriteString("\" />\n")
	b.WriteString("<link rel=\"canonical\" href=\"")
	b.WriteString(pageURL)
	b.WriteString("\" />\n")
	b.WriteString("<link rel=\"icon\" href=\"/favicon.png\" type=\"image/png\" />\n")
	b.WriteString("<link rel=\"apple-touch-icon\" href=\"/favicon.png\" />\n")
	// Open Graph
	b.WriteString("<meta property=\"og:site_name\" content=\"")
	b.WriteString(site)
	b.WriteString("\" />\n")
	b.WriteString("<meta property=\"og:type\" content=\"")
	b.WriteString(ogType)
	b.WriteString("\" />\n")
	b.WriteString("<meta property=\"og:title\" content=\"")
	b.WriteString(title)
	b.WriteString("\" />\n")
	b.WriteString("<meta property=\"og:description\" content=\"")
	b.WriteString(desc)
	b.WriteString("\" />\n")
	b.WriteString("<meta property=\"og:url\" content=\"")
	b.WriteString(pageURL)
	b.WriteString("\" />\n")
	if img != "" {
		b.WriteString("<meta property=\"og:image\" content=\"")
		b.WriteString(img)
		b.WriteString("\" />\n")
	}
	if author != "" {
		b.WriteString("<meta property=\"article:author\" content=\"")
		b.WriteString(author)
		b.WriteString("\" />\n")
	}
	// Twitter
	b.WriteString("<meta name=\"twitter:card\" content=\"summary_large_image\" />\n")
	b.WriteString("<meta name=\"twitter:title\" content=\"")
	b.WriteString(title)
	b.WriteString("\" />\n")
	b.WriteString("<meta name=\"twitter:description\" content=\"")
	b.WriteString(desc)
	b.WriteString("\" />\n")
	if img != "" {
		b.WriteString("<meta name=\"twitter:image\" content=\"")
		b.WriteString(img)
		b.WriteString("\" />\n")
	}
	// Bootstrap SPA assets from live index.html (keeps hashed bundles in sync)
	b.WriteString(`<script>
(function(){
  function boot(){
    fetch("/index.html",{credentials:"same-origin",cache:"no-cache"})
      .then(function(r){return r.text()})
      .then(function(html){
        var doc=new DOMParser().parseFromString(html,"text/html");
        var head=document.head;
        doc.querySelectorAll('link[rel="stylesheet"],link[rel="modulepreload"]').forEach(function(n){
          if(n.getAttribute("href")&&!document.querySelector('link[href="'+n.getAttribute("href")+'"]')){
            head.appendChild(n.cloneNode(true));
          }
        });
        var scripts=Array.prototype.slice.call(doc.querySelectorAll("script[src]"));
        function next(i){
          if(i>=scripts.length)return;
          var s=scripts[i];
          var el=document.createElement("script");
          if(s.type)el.type=s.type;
          if(s.hasAttribute("crossorigin"))el.crossOrigin=s.crossOrigin||"";
          el.src=s.src;
          el.onload=function(){next(i+1)};
          el.onerror=function(){next(i+1)};
          head.appendChild(el);
        }
        next(0);
      }).catch(function(){});
  }
  if(document.readyState==="loading")document.addEventListener("DOMContentLoaded",boot);
  else boot();
})();
</script>
`)
	b.WriteString("</head>\n<body>\n")
	b.WriteString("<div id=\"root\">\n")
	b.WriteString("<main style=\"max-width:720px;margin:2rem auto;padding:0 1rem;font-family:system-ui,sans-serif;line-height:1.6;color:#111\">\n")
	b.WriteString("<h1>")
	b.WriteString(bodyTitle)
	b.WriteString("</h1>\n")
	if author != "" {
		b.WriteString("<p style=\"color:#666\">")
		b.WriteString(author)
		b.WriteString("</p>\n")
	}
	b.WriteString("<p>")
	b.WriteString(bodyText)
	b.WriteString("</p>\n")
	b.WriteString("<p><a href=\"")
	b.WriteString(pageURL)
	b.WriteString("\">打开完整页面</a></p>\n")
	b.WriteString("</main>\n</div>\n</body>\n</html>\n")
	return b.String()
}

func (s *SEOService) handleSitemap(ctx khttp.Context) error {
	req := ctx.Request()
	origin := publicOrigin(req)
	siteTitle, _, _ := s.siteBrand()
	_ = siteTitle

	var urls []string
	urls = append(urls, origin+"/")
	urls = append(urls, origin+"/blog-plaza")
	urls = append(urls, origin+"/question-bank")
	urls = append(urls, origin+"/contest")
	urls = append(urls, origin+"/bulletin")

	if s.data != nil && s.data.DB != nil {
		type row struct {
			Username string
			Slug     string
			Updated  time.Time
		}
		var list []row
		// public articles only; join users for username
		_ = s.data.DB.Raw(`
			SELECT u.username AS username, a.slug AS slug, a.updated_at AS updated
			FROM blog_articles a
			INNER JOIN users u ON u.id = a.user_id
			WHERE a.visibility = ?
			ORDER BY a.updated_at DESC
			LIMIT ?
		`, model.BlogVisPublic, maxSitemapArticles).Scan(&list).Error
		seenAuthor := map[string]struct{}{}
		for _, r := range list {
			if r.Username == "" || r.Slug == "" {
				continue
			}
			urls = append(urls, fmt.Sprintf("%s/blog/%s/%s", origin, url.PathEscape(r.Username), url.PathEscape(r.Slug)))
			if _, ok := seenAuthor[r.Username]; !ok {
				seenAuthor[r.Username] = struct{}{}
				urls = append(urls, fmt.Sprintf("%s/blog/%s", origin, url.PathEscape(r.Username)))
			}
		}
	}

	w := ctx.Response()
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Header().Set("Cache-Control", "public, max-age=600")
	w.WriteHeader(200)
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">` + "\n")
	for _, u := range urls {
		b.WriteString("  <url><loc>")
		b.WriteString(html.EscapeString(u))
		b.WriteString("</loc></url>\n")
	}
	b.WriteString("</urlset>\n")
	_, _ = w.Write([]byte(b.String()))
	return nil
}
