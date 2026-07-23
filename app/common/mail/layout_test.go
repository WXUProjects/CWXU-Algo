package mail

import (
	"strings"
	"testing"
)

func TestWrap_HasBrandShell(t *testing.T) {
	html := Wrap(LayoutOpts{Brand: "GoAlgo", Title: "测试标题", Preheader: "预览"}, `<p>你好</p>`)
	for _, want := range []string{
		"<!DOCTYPE html>",
		`charset="utf-8"`,
		"GoAlgo",
		"测试标题",
		"你好",
		"本邮件由",
		"打开主站",
		BrandColor,
		"</html>",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("missing %q in:\n%s", want, html[:min(200, len(html))])
		}
	}
}

func TestInjectBeforeBodyClose(t *testing.T) {
	doc := `<!DOCTYPE html><html><body><p>hi</p></body></html>`
	out := InjectBeforeBodyClose(doc, `<hr><p>foot</p>`)
	if !strings.Contains(out, `<p>hi</p><hr><p>foot</p></body>`) {
		t.Fatalf("inject failed: %s", out)
	}
	// no body: before html
	doc2 := `<html><p>x</p></html>`
	out2 := InjectBeforeBodyClose(doc2, `<span>y</span>`)
	if !strings.HasSuffix(out2, `<span>y</span></html>`) {
		t.Fatalf("html inject: %s", out2)
	}
}

func TestPlainFromHTML(t *testing.T) {
	plain := PlainFromHTML(`<p>你好</p><div>第二行</div>`)
	if !strings.Contains(plain, "你好") || !strings.Contains(plain, "第二行") {
		t.Fatalf("plain=%q", plain)
	}
	if strings.Contains(plain, "<") {
		t.Fatalf("still has tags: %q", plain)
	}
}

func TestIsFullHTMLDocument(t *testing.T) {
	if !IsFullHTMLDocument(`<!DOCTYPE html><html></html>`) {
		t.Fatal("doctype")
	}
	if !IsFullHTMLDocument(`  <html><body></body></html>`) {
		t.Fatal("html")
	}
	if IsFullHTMLDocument(`<p>only frag</p>`) {
		t.Fatal("frag should be false")
	}
}

func TestEnsureDocument(t *testing.T) {
	full := EnsureDocument(`<!DOCTYPE html><html><body>a</body></html>`)
	if !strings.HasPrefix(strings.ToLower(strings.TrimSpace(full)), "<!doctype") {
		t.Fatal(full)
	}
	frag := EnsureDocument(`<p>x</p>`)
	if !IsFullHTMLDocument(frag) || !strings.Contains(frag, "<p>x</p>") {
		t.Fatal(frag)
	}
}

func TestParagraphs(t *testing.T) {
	h := Paragraphs("a\nb\n\nc")
	if !strings.Contains(h, "<br>") || !strings.Contains(h, "c") {
		t.Fatal(h)
	}
	if strings.Contains(h, "<script>") {
		t.Fatal("xss")
	}
	esc := Paragraphs(`<b>x</b>`)
	if strings.Contains(esc, "<b>") {
		t.Fatal(esc)
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
