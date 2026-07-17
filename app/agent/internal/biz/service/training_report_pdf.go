package service

import (
	"fmt"
	"strings"
)

// RenderSimplePDF 从训练报告数据生成最小可用 PDF（纯文本布局，无第三方依赖）。
// 用于邮件附件与下载；复杂排版以 HTML 为准。
func RenderSimplePDF(data *TrainingReportData, brand string) []byte {
	if data == nil {
		return minimalPDF("empty report")
	}
	if brand == "" {
		brand = "GoAlgo"
	}
	var lines []string
	lines = append(lines, brand+" Training Report")
	lines = append(lines, fmt.Sprintf("Range: %s ~ %s", data.StartDate, data.EndDate))
	lines = append(lines, fmt.Sprintf("Scope: %s | Members: %d | Active: %d", data.ScopeLabel, data.MemberCount, data.ActiveMembers))
	lines = append(lines, fmt.Sprintf("Submits: %d (prev %d) | AC: %d", data.TotalSubmits, data.PrevTotalSubmits, data.TotalAC))
	lines = append(lines, "")
	lines = append(lines, "Top Submit:")
	for _, r := range data.TopSubmit {
		lines = append(lines, fmt.Sprintf("  #%d %s %d", r.Rank, sanitizePDFText(r.Name), r.Score))
	}
	if len(data.TopSubmit) == 0 {
		lines = append(lines, "  (none)")
	}
	lines = append(lines, "Top AC:")
	for _, r := range data.TopAC {
		lines = append(lines, fmt.Sprintf("  #%d %s %d", r.Rank, sanitizePDFText(r.Name), r.Score))
	}
	if len(data.TopAC) == 0 {
		lines = append(lines, "  (none)")
	}
	lines = append(lines, "Inactive:")
	if len(data.InactiveMembers) == 0 {
		lines = append(lines, "  (none)")
	} else {
		for i, n := range data.InactiveMembers {
			if i >= 30 {
				lines = append(lines, "  ...")
				break
			}
			lines = append(lines, "  - "+sanitizePDFText(n))
		}
	}
	lines = append(lines, "")
	lines = append(lines, "Daily trend:")
	for _, d := range data.DailyTrend {
		lines = append(lines, fmt.Sprintf("  %s: %d", d.Date, d.Count))
	}
	return minimalPDF(strings.Join(lines, "\n"))
}

func sanitizePDFText(s string) string {
	// PDF 内置 Helvetica 对中文支持差：保留可打印 ASCII，其余用 ?
	var b strings.Builder
	for _, r := range s {
		if r >= 32 && r < 127 {
			b.WriteRune(r)
		} else if r == '\n' || r == '\r' {
			b.WriteByte(' ')
		} else {
			b.WriteByte('?')
		}
	}
	return b.String()
}

// minimalPDF 生成单页 PDF（Helvetica 文本）
func minimalPDF(text string) []byte {
	// 转义 PDF 字符串
	esc := strings.ReplaceAll(text, "\\", "\\\\")
	esc = strings.ReplaceAll(esc, "(", "\\(")
	esc = strings.ReplaceAll(esc, ")", "\\)")
	// 分页：按行画，每行 14pt，从 y=800 往下
	lines := strings.Split(esc, "\n")
	var content strings.Builder
	content.WriteString("BT /F1 10 Tf 40 800 Td 14 TL\n")
	for i, line := range lines {
		if i > 0 {
			content.WriteString("T*\n")
		}
		// 过长截断
		if len(line) > 100 {
			line = line[:100]
		}
		content.WriteString("(")
		content.WriteString(line)
		content.WriteString(") Tj\n")
		if i > 50 {
			content.WriteString("( ... ) Tj\n")
			break
		}
	}
	content.WriteString("ET")
	stream := content.String()

	objs := []string{
		"1 0 obj<< /Type /Catalog /Pages 2 0 R >>endobj\n",
		"2 0 obj<< /Type /Pages /Kids [3 0 R] /Count 1 >>endobj\n",
		"3 0 obj<< /Type /Page /Parent 2 0 R /MediaBox [0 0 612 792] /Contents 4 0 R /Resources<< /Font<< /F1 5 0 R >> >> >>endobj\n",
		fmt.Sprintf("4 0 obj<< /Length %d >>stream\n%s\nendstream endobj\n", len(stream), stream),
		"5 0 obj<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>endobj\n",
	}

	var out strings.Builder
	out.WriteString("%PDF-1.4\n")
	offsets := make([]int, len(objs)+1)
	for i, o := range objs {
		offsets[i+1] = out.Len()
		out.WriteString(o)
	}
	xrefPos := out.Len()
	out.WriteString(fmt.Sprintf("xref\n0 %d\n", len(objs)+1))
	out.WriteString("0000000000 65535 f \n")
	for i := 1; i <= len(objs); i++ {
		out.WriteString(fmt.Sprintf("%010d 00000 n \n", offsets[i]))
	}
	out.WriteString(fmt.Sprintf("trailer<< /Size %d /Root 1 0 R >>\nstartxref\n%d\n%%%%EOF\n", len(objs)+1, xrefPos))
	return []byte(out.String())
}

// PDFLooksValid 粗检 PDF 头
func PDFLooksValid(b []byte) bool {
	return len(b) > 8 && string(b[:5]) == "%PDF-"
}
