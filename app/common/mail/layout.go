package mail

import (
	"html"
	"regexp"
	"strings"
)

// Brand color aligned with training report / GoAlgo indigo.
const (
	BrandColor   = "#4f46e5"
	SiteHomeURL  = "https://algo.zhiyuansofts.cn"
	DefaultBrand = "GoAlgo"
)

// LayoutOpts controls the shared email shell.
type LayoutOpts struct {
	// Brand site title (default GoAlgo)
	Brand string
	// Title shown under brand in header bar
	Title string
	// Preheader is hidden preview text in some inbox list UIs
	Preheader string
	// HomeURL optional; default SiteHomeURL
	HomeURL string
}

// Escape HTML-escapes plain text for safe insertion into templates.
func Escape(s string) string {
	return html.EscapeString(s)
}

// Paragraphs turns plain text into HTML paragraphs (newlines → <br> within one block).
func Paragraphs(plain string) string {
	plain = strings.TrimSpace(plain)
	if plain == "" {
		return ""
	}
	parts := strings.Split(plain, "\n\n")
	var b strings.Builder
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		p = html.EscapeString(p)
		p = strings.ReplaceAll(p, "\n", "<br>")
		b.WriteString(`<p style="margin:0 0 12px;font-size:14px;line-height:1.6;color:#222;">`)
		b.WriteString(p)
		b.WriteString(`</p>`)
	}
	return b.String()
}

// Wrap builds a full HTML email document with branded header/footer around innerHTML.
// innerHTML must already be safe HTML (caller escapes user content).
func Wrap(opts LayoutOpts, innerHTML string) string {
	brand := strings.TrimSpace(opts.Brand)
	if brand == "" {
		brand = DefaultBrand
	}
	title := strings.TrimSpace(opts.Title)
	home := strings.TrimSpace(opts.HomeURL)
	if home == "" {
		home = SiteHomeURL
	}
	pre := strings.TrimSpace(opts.Preheader)

	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html><head><meta charset="utf-8">`)
	b.WriteString(`<meta name="viewport" content="width=device-width,initial-scale=1">`)
	if title != "" {
		b.WriteString(`<title>`)
		b.WriteString(html.EscapeString(title))
		b.WriteString(`</title>`)
	}
	b.WriteString(`</head>`)
	b.WriteString(`<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;font-size:14px;line-height:1.5;color:#222;">`)
	if pre != "" {
		// Hidden preheader for inbox preview
		b.WriteString(`<div style="display:none;max-height:0;overflow:hidden;mso-hide:all;">`)
		b.WriteString(html.EscapeString(pre))
		b.WriteString(`</div>`)
	}
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="background:#f0f2f5;"><tr><td align="center" style="padding:16px 8px;">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="max-width:640px;width:100%;background:#ffffff;border-radius:8px;overflow:hidden;">`)

	// Header
	b.WriteString(`<tr><td style="background:`)
	b.WriteString(BrandColor)
	b.WriteString(`;color:#ffffff;padding:16px 18px;">`)
	b.WriteString(`<div style="font-size:12px;opacity:0.9;letter-spacing:0.02em;">`)
	b.WriteString(html.EscapeString(brand))
	b.WriteString(`</div>`)
	if title != "" {
		b.WriteString(`<div style="font-size:18px;font-weight:bold;margin-top:6px;">`)
		b.WriteString(html.EscapeString(title))
		b.WriteString(`</div>`)
	}
	b.WriteString(`</td></tr>`)

	// Body
	b.WriteString(`<tr><td style="padding:18px 16px 8px;">`)
	b.WriteString(innerHTML)
	b.WriteString(`</td></tr>`)

	// Footer
	b.WriteString(`<tr><td style="padding:8px 16px 20px;border-top:1px solid #e5e7eb;">`)
	b.WriteString(`<p style="margin:0;font-size:12px;color:#94a3b8;line-height:1.5;">本邮件由 `)
	b.WriteString(html.EscapeString(brand))
	b.WriteString(` 自动发送，请勿直接回复。`)
	b.WriteString(` · <a href="`)
	b.WriteString(html.EscapeString(home))
	b.WriteString(`" style="color:`)
	b.WriteString(BrandColor)
	b.WriteString(`;text-decoration:none;">打开主站</a></p>`)
	b.WriteString(`</td></tr>`)

	b.WriteString(`</table></td></tr></table></body></html>`)
	return b.String()
}

// IsFullHTMLDocument reports whether s looks like a complete HTML document.
func IsFullHTMLDocument(s string) bool {
	lower := strings.ToLower(strings.TrimSpace(s))
	return strings.HasPrefix(lower, "<!doctype") || strings.HasPrefix(lower, "<html")
}

// InjectBeforeBodyClose inserts snippet just before </body>, or before </html>, or appends if neither found.
// Used for report footers so content is never outside the document.
func InjectBeforeBodyClose(doc, snippet string) string {
	doc = strings.TrimSpace(doc)
	snippet = strings.TrimSpace(snippet)
	if snippet == "" {
		return doc
	}
	if doc == "" {
		return snippet
	}
	lower := strings.ToLower(doc)
	if i := strings.LastIndex(lower, "</body>"); i >= 0 {
		return doc[:i] + snippet + doc[i:]
	}
	if i := strings.LastIndex(lower, "</html>"); i >= 0 {
		return doc[:i] + snippet + doc[i:]
	}
	return doc + snippet
}

var (
	reHTMLTag     = regexp.MustCompile(`(?is)<[^>]+>`)
	reHTMLComment = regexp.MustCompile(`(?is)<!--.*?-->`)
	reMultiSpace  = regexp.MustCompile(`[ \t\x0b\f\r]+`)
	reMultiNL     = regexp.MustCompile(`\n{3,}`)
)

// PlainFromHTML strips tags for multipart/alternative text/plain part.
func PlainFromHTML(htmlBody string) string {
	s := htmlBody
	s = reHTMLComment.ReplaceAllString(s, "")
	// block-ish tags → newlines
	for _, tag := range []string{"br", "p", "div", "tr", "li", "h1", "h2", "h3", "h4", "hr", "table"} {
		re := regexp.MustCompile(`(?i)</?` + tag + `[^>]*>`)
		s = re.ReplaceAllString(s, "\n")
	}
	s = reHTMLTag.ReplaceAllString(s, "")
	s = html.UnescapeString(s)
	s = reMultiSpace.ReplaceAllString(s, " ")
	// normalize lines
	lines := strings.Split(s, "\n")
	var out []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" {
			out = append(out, line)
		}
	}
	s = strings.Join(out, "\n")
	s = reMultiNL.ReplaceAllString(s, "\n\n")
	return strings.TrimSpace(s)
}

// EnsureDocument wraps a fragment into a minimal full HTML document if needed.
func EnsureDocument(fragment string) string {
	s := strings.TrimSpace(fragment)
	if s == "" {
		return s
	}
	if IsFullHTMLDocument(s) {
		return s
	}
	return `<!DOCTYPE html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"></head>` +
		`<body style="margin:0;padding:0;background:#f0f2f5;font-family:Arial,'PingFang SC','Microsoft YaHei',sans-serif;font-size:14px;line-height:1.5;color:#222;">` +
		s + `</body></html>`
}
