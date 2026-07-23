package mail

import "strings"

// shadcn/ui light tokens (:root in newUI index.css) as email-safe hex.
// Email clients do not support oklch; these match primary/muted/border/radius.
const (
	// ColorBackground page chrome (muted-ish canvas)
	ColorBackground = "#fafafa"
	// ColorCard card surface
	ColorCard = "#ffffff"
	// ColorForeground body text
	ColorForeground = "#0a0a0a"
	// ColorPrimary primary / buttons (oklch 0.205)
	ColorPrimary = "#171717"
	// ColorPrimaryFg on primary
	ColorPrimaryFg = "#fafafa"
	// ColorMuted muted surface / secondary
	ColorMuted = "#f5f5f5"
	// ColorMutedFg secondary text
	ColorMutedFg = "#737373"
	// ColorBorder hairline borders
	ColorBorder = "#e5e5e5"
	// ColorRing subtle focus-ish line (oklch 0.708)
	ColorRing = "#a3a3a3"
	// ColorDestructive soft alert text/bg edge
	ColorDestructive = "#dc2626"
	// ColorDestructiveBg badge-like warm muted
	ColorDestructiveBg = "#fef2f2"
	// ColorSuccessFg empty-positive note
	ColorSuccessFg = "#15803d"
	// ColorSuccessBg
	ColorSuccessBg = "#f0fdf4"

	// BrandColor kept for callers; aliases primary (shadcn monochrome).
	BrandColor = ColorPrimary

	SiteHomeURL  = "https://algo.zhiyuansofts.cn"
	DefaultBrand = "GoAlgo"

	RadiusLg = "10px" // --radius 0.625rem
	RadiusMd = "8px"
	RadiusSm = "6px"

	FontStack = `ui-sans-serif,system-ui,-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue","PingFang SC","Hiragino Sans GB","Microsoft YaHei",Arial,sans-serif`
)

// Style fragments for consistent shadcn-like email markup.
const (
	StyleBody = `margin:0;padding:0;background:` + ColorBackground + `;font-family:` + FontStack + `;font-size:14px;line-height:1.5;color:` + ColorForeground + `;`
	StylePage = `background:` + ColorBackground + `;`
	StyleCard = `max-width:640px;width:100%;background:` + ColorCard + `;border:1px solid ` + ColorBorder + `;border-radius:` + RadiusLg + `;overflow:hidden;`
	StyleMutedText = `color:` + ColorMutedFg + `;`
	StyleLink = `color:` + ColorPrimary + `;text-decoration:underline;text-underline-offset:2px;`
	StyleLinkQuiet = `color:` + ColorPrimary + `;text-decoration:none;font-weight:600;`
)

// BtnPrimary returns an inline-block primary button (shadcn default Button).
func BtnPrimary(href, label string) string {
	return `<a href="` + Escape(href) + `" style="display:inline-block;padding:10px 16px;background:` + ColorPrimary +
		`;color:` + ColorPrimaryFg + `;text-decoration:none;border-radius:` + RadiusMd +
		`;font-size:14px;font-weight:500;line-height:1.25;border:1px solid ` + ColorPrimary + `;">` +
		Escape(label) + `</a>`
}

// BtnOutline outline button variant.
func BtnOutline(href, label string) string {
	return `<a href="` + Escape(href) + `" style="display:inline-block;padding:10px 16px;background:` + ColorCard +
		`;color:` + ColorForeground + `;text-decoration:none;border-radius:` + RadiusMd +
		`;font-size:14px;font-weight:500;line-height:1.25;border:1px solid ` + ColorBorder + `;">` +
		Escape(label) + `</a>`
}

// BadgeSecondary muted pill (shadcn Badge secondary).
func BadgeSecondary(label string) string {
	return `<span style="display:inline-block;background:` + ColorMuted + `;color:` + ColorForeground +
		`;border:1px solid ` + ColorBorder + `;border-radius:9999px;padding:2px 10px;margin:2px 4px 2px 0;font-size:12px;font-weight:500;line-height:1.5;">` +
		Escape(label) + `</span>`
}

// BadgeWarn warm secondary for inactive names etc.
func BadgeWarn(label string) string {
	return `<span style="display:inline-block;background:` + ColorDestructiveBg + `;color:` + ColorDestructive +
		`;border:1px solid ` + ColorBorder + `;border-radius:9999px;padding:2px 10px;margin:2px 4px 2px 0;font-size:12px;font-weight:500;line-height:1.5;">` +
		Escape(label) + `</span>`
}

// KPICell is a small metric card (muted surface + border).
func KPICell(valueHTML, label, hint string) string {
	out := `<td width="50%" valign="top" style="background:` + ColorMuted + `;border:1px solid ` + ColorBorder +
		`;border-radius:` + RadiusMd + `;padding:12px 10px;">` +
		`<div style="font-size:22px;font-weight:600;letter-spacing:-0.02em;color:` + ColorForeground + `;">` + valueHTML + `</div>` +
		`<div style="font-size:12px;color:` + ColorMutedFg + `;margin-top:2px;">` + Escape(label) + `</div>`
	if hint != "" {
		out += `<div style="font-size:11px;color:` + ColorMutedFg + `;margin-top:4px;">` + Escape(hint) + `</div>`
	}
	out += `</td>`
	return out
}

// SectionTitle bar used inside report sections.
func SectionTitle(title string) string {
	return `<div style="font-size:14px;font-weight:600;color:` + ColorForeground +
		`;margin:0 0 10px;padding-bottom:8px;border-bottom:1px solid ` + ColorBorder + `;">` +
		Escape(title) + `</div>`
}

// DataTableOpen / header / cell helpers for list tables.
func DataTableOpen() string {
	return `<table width="100%" cellpadding="0" cellspacing="0" border="0" style="border-collapse:collapse;font-size:13px;width:100%;">`
}

func TH(label string, align string) string {
	if align == "" {
		align = "left"
	}
	return `<th align="` + align + `" style="padding:8px 6px;background:` + ColorMuted +
		`;border-bottom:1px solid ` + ColorBorder + `;font-size:12px;font-weight:500;color:` + ColorMutedFg +
		`;text-align:` + align + `;">` + Escape(label) + `</th>`
}

func TD(htmlInner string, align string) string {
	if align == "" {
		align = "left"
	}
	return `<td align="` + align + `" style="padding:8px 6px;border-bottom:1px solid ` + ColorBorder +
		`;color:` + ColorForeground + `;font-size:13px;text-align:` + align + `;">` + htmlInner + `</td>`
}

// CodeBlock large OTP display (InputOTP-ish).
func CodeBlock(code string) string {
	return `<div style="margin:16px 0;padding:16px;text-align:center;background:` + ColorMuted +
		`;border:1px solid ` + ColorBorder + `;border-radius:` + RadiusMd + `;">` +
		`<span style="font-size:28px;font-weight:600;letter-spacing:0.35em;color:` + ColorForeground +
		`;font-family:ui-monospace,SFMono-Regular,Menlo,Monaco,Consolas,monospace;">` + Escape(code) + `</span></div>`
}

// RowKV label/value definition list row.
func RowKV(label, valueHTML string) string {
	return `<tr><td style="padding:6px 12px 6px 0;color:` + ColorMutedFg +
		`;vertical-align:top;width:88px;font-size:13px;">` + Escape(label) +
		`</td><td style="padding:6px 0;color:` + ColorForeground + `;font-size:14px;">` + valueHTML + `</td></tr>`
}

// P body paragraph.
func P(text string) string {
	return `<p style="margin:0 0 12px;font-size:14px;line-height:1.6;color:` + ColorForeground + `;">` + Escape(text) + `</p>`
}

// PMuted muted paragraph (no escape if already HTML — use carefully).
func PMutedHTML(innerHTML string) string {
	return `<p style="margin:0 0 12px;font-size:13px;line-height:1.6;color:` + ColorMutedFg + `;">` + innerHTML + `</p>`
}

// Link home link markup.
func Link(href, label string) string {
	return `<a href="` + Escape(href) + `" style="` + StyleLinkQuiet + `">` + Escape(label) + `</a>`
}

// DocShellOpen returns <!DOCTYPE>… through card open (for full report documents).
// headerBrand/title/subtitle are escaped; headerExtraHTML is raw (e.g. links already escaped).
func DocShellOpen(pageTitle, headerBrand, headerTitle, headerSubtitle, headerExtraHTML string) string {
	var b strings.Builder
	b.WriteString(`<!DOCTYPE html><html><head><meta charset="utf-8">`)
	b.WriteString(`<meta name="viewport" content="width=device-width,initial-scale=1">`)
	if pageTitle != "" {
		b.WriteString(`<title>`)
		b.WriteString(Escape(pageTitle))
		b.WriteString(`</title>`)
	}
	b.WriteString(`</head><body style="`)
	b.WriteString(StyleBody)
	b.WriteString(`">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="`)
	b.WriteString(StylePage)
	b.WriteString(`"><tr><td align="center" style="padding:24px 12px;">`)
	b.WriteString(`<table role="presentation" width="100%" cellpadding="0" cellspacing="0" border="0" style="`)
	b.WriteString(StyleCard)
	b.WriteString(`">`)
	// Header — primary bar (shadcn primary)
	b.WriteString(`<tr><td style="background:`)
	b.WriteString(ColorPrimary)
	b.WriteString(`;color:`)
	b.WriteString(ColorPrimaryFg)
	b.WriteString(`;padding:20px 18px;">`)
	if headerBrand != "" {
		b.WriteString(`<div style="font-size:12px;font-weight:500;opacity:0.85;letter-spacing:0.02em;">`)
		b.WriteString(Escape(headerBrand))
		b.WriteString(`</div>`)
	}
	if headerTitle != "" {
		b.WriteString(`<div style="font-size:20px;font-weight:600;letter-spacing:-0.02em;margin-top:6px;line-height:1.3;">`)
		b.WriteString(Escape(headerTitle))
		b.WriteString(`</div>`)
	}
	if headerSubtitle != "" {
		b.WriteString(`<div style="font-size:12px;opacity:0.8;margin-top:6px;line-height:1.45;">`)
		b.WriteString(Escape(headerSubtitle))
		b.WriteString(`</div>`)
	}
	if headerExtraHTML != "" {
		b.WriteString(headerExtraHTML)
	}
	b.WriteString(`</td></tr>`)
	return b.String()
}

// DocShellClose closes card/page/body/html.
func DocShellClose() string {
	return `</table></td></tr></table></body></html>`
}

// DocFooter center muted footer row inside card.
func DocFooter(htmlInner string) string {
	return `<tr><td style="padding:14px 16px 18px;text-align:center;font-size:11px;color:` + ColorMutedFg +
		`;border-top:1px solid ` + ColorBorder + `;background:` + ColorCard + `;">` + htmlInner + `</td></tr>`
}
