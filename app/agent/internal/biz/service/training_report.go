package service

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	agenttool "cwxu-algo/app/agent/internal/agent/tool"
	"cwxu-algo/app/agent/internal/agent/tool/core_data"
	"cwxu-algo/app/common/mail"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	"github.com/volcengine/volcengine-go-sdk/volcengine"
)

// StartTrainingReportParams 启动参数
type StartTrainingReportParams struct {
	OrgID     int64
	GroupID   int64
	StartDate string
	EndDate   string
	UseAI     bool
	CreatedBy int64
	Source    string // manual | weekly
}

// GetTrainingReportJob 供 HTTP 层查询
func (uc *SummaryUseCase) GetTrainingReportJob(ctx context.Context, jobID string) (*TrainingReportJob, error) {
	return uc.getJob(ctx, jobID)
}

// ListTrainingReportJobs 供 HTTP 层列表
func (uc *SummaryUseCase) ListTrainingReportJobs(ctx context.Context, orgID, limit int64) ([]*TrainingReportJob, error) {
	return uc.listJobs(ctx, orgID, limit)
}

// StartTrainingReport 创建异步任务并后台执行，立即返回 jobId
func (uc *SummaryUseCase) StartTrainingReport(ctx context.Context, p StartTrainingReportParams) (string, error) {
	if p.OrgID <= 0 {
		return "", fmt.Errorf("缺少组织 id")
	}
	start, end, err := ParseDateRange(p.StartDate, p.EndDate)
	if err != nil {
		return "", err
	}
	_ = start
	_ = end
	if err := ensureReportDir(); err != nil {
		return "", fmt.Errorf("创建报告目录失败: %w", err)
	}
	jobID := newJobID()
	now := time.Now()
	job := &TrainingReportJob{
		JobID:     jobID,
		Status:    ReportStatusPending,
		Progress:  0,
		Message:   "排队中",
		StartDate: p.StartDate,
		EndDate:   p.EndDate,
		GroupID:   p.GroupID,
		UseAI:     p.UseAI,
		OrgID:     p.OrgID,
		CreatedBy: p.CreatedBy,
		CreatedAt: now.Unix(),
		Source:    p.Source,
	}
	if job.Source == "" {
		job.Source = "manual"
	}
	if err := uc.saveJob(ctx, job); err != nil {
		return "", fmt.Errorf("保存任务失败: %w", err)
	}
	go uc.runTrainingReportJob(jobID)
	return jobID, nil
}

func (uc *SummaryUseCase) runTrainingReportJob(jobID string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	defer func() {
		if r := recover(); r != nil {
			uc.failJob(ctx, jobID, fmt.Sprintf("panic: %v", r))
		}
	}()

	_ = uc.updateJob(ctx, jobID, func(j *TrainingReportJob) {
		j.Status = ReportStatusRunning
		j.Progress = 5
		j.Message = "加载数据中"
	})

	job, err := uc.getJob(ctx, jobID)
	if err != nil || job == nil {
		log.Errorf("training report job missing %s: %v", jobID, err)
		return
	}

	start, end, err := ParseDateRange(job.StartDate, job.EndDate)
	if err != nil {
		uc.failJob(ctx, jobID, err.Error())
		return
	}

	data, err := uc.LoadTrainingReportData(ctx, job.OrgID, job.GroupID, job.CreatedBy, start, end)
	if err != nil {
		uc.failJob(ctx, jobID, "加载数据失败: "+err.Error())
		return
	}

	_ = uc.updateJob(ctx, jobID, func(j *TrainingReportJob) {
		j.Progress = 40
		j.Message = "生成报告中"
	})

	brand := uc.brandTitle(ctx)
	var html string
	if job.UseAI {
		html, err = uc.generateTrainingReportAI(ctx, data)
		if err != nil {
			log.Warnf("training report AI failed job=%s: %v; fallback rule template", jobID, err)
			html = RenderRuleTemplateHTML(data, brand)
			_ = uc.updateJob(ctx, jobID, func(j *TrainingReportJob) {
				j.Message = "AI 不可用，已用规则模板"
			})
		}
	} else {
		html = RenderRuleTemplateHTML(data, brand)
	}
	html = stripCodeFence(html)
	if strings.TrimSpace(html) == "" {
		uc.failJob(ctx, jobID, "报告内容为空")
		return
	}

	pdfBytes := RenderSimplePDF(data, brand)
	htmlPath, pdfPath := jobArtifactPaths(jobID)
	if err := os.WriteFile(htmlPath, []byte(html), 0o644); err != nil {
		uc.failJob(ctx, jobID, "写入 HTML 失败: "+err.Error())
		return
	}
	if err := os.WriteFile(pdfPath, pdfBytes, 0o644); err != nil {
		uc.failJob(ctx, jobID, "写入 PDF 失败: "+err.Error())
		return
	}

	finished := time.Now()
	expires := finished.Add(reportDownloadTTL)
	fileName := fmt.Sprintf("training-report-%s-%s.pdf", job.StartDate, job.EndDate)

	// 就地更新本地 job 再落盘，保证 notify 使用含 ExpiresAt/FileName 的快照
	job.Status = ReportStatusDone
	job.Progress = 100
	job.Message = "已完成"
	job.FinishedAt = finished.Unix()
	job.ExpiresAt = expires.Unix()
	job.HTMLPath = htmlPath
	job.PDFPath = pdfPath
	job.FileName = fileName
	if err := uc.saveJob(ctx, job); err != nil {
		uc.failJob(ctx, jobID, "保存完成状态失败: "+err.Error())
		return
	}

	// 再读一次，确保与 Redis 一致后再发邮件
	if fresh, err := uc.getJob(ctx, jobID); err == nil && fresh != nil {
		job = fresh
	}

	if err := uc.notifyTrainingReportDone(ctx, data, job, html, pdfPath); err != nil {
		log.Warnf("training report notify job=%s: %v", jobID, err)
	}
	log.Infof("training report done job=%s org=%d", jobID, job.OrgID)
}

func (uc *SummaryUseCase) failJob(ctx context.Context, jobID, detail string) {
	_ = uc.updateJob(ctx, jobID, func(j *TrainingReportJob) {
		j.Status = ReportStatusFailed
		j.Progress = 100
		j.Message = "失败"
		j.ErrorDetail = detail
		j.FinishedAt = time.Now().Unix()
	})
	log.Errorf("training report failed job=%s: %s", jobID, detail)
}

func (uc *SummaryUseCase) generateTrainingReportAI(ctx context.Context, data *TrainingReportData) (string, error) {
	if uc.chat == nil {
		return "", fmt.Errorf("chat 未初始化")
	}
	// 工具使用 elevated agent 身份上下文（org 写入 token）
	toolCtx, err := ContextWithElevatedAgent(ctx, uint(data.OrgID))
	if err != nil {
		log.Warnf("elevated agent token: %v", err)
		toolCtx = ctx
	}
	tools := DomainAgentTools(uc.reg, uint(data.OrgID), toolCtx)
	msgs := []*model.ChatCompletionMessage{
		{
			Role: model.ChatMessageRoleSystem,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(trainingReportSystemPrompt()),
			},
		},
		{
			Role: model.ChatMessageRoleUser,
			Content: &model.ChatCompletionMessageContent{
				StringValue: volcengine.String(trainingReportUserPrompt(data)),
			},
		},
	}
	return uc.chat.Chat(ctx, msgs, tools...)
}

// BuildNotifyEmail 纯函数：构造通知邮件主题/正文/附件名（可单测，不依赖 SMTP）
func BuildNotifyEmail(job *TrainingReportJob, brand, html string) (subject, body, attachName string) {
	if brand == "" {
		brand = "GoAlgo"
	}
	start, end, id := "", "", ""
	var expiresAt int64
	if job != nil {
		start, end, id = job.StartDate, job.EndDate, job.JobID
		expiresAt = job.ExpiresAt
		attachName = strings.TrimSpace(job.FileName)
	}
	if attachName == "" {
		attachName = "training-report.pdf"
	}
	subject = fmt.Sprintf("【%s 训练报告】%s ~ %s 已生成", brand, start, end)
	body = html
	if strings.TrimSpace(body) == "" {
		body = fmt.Sprintf(`<p>您好，您发起的训练报告（%s ~ %s）已生成完成。</p>
<p>下载有效期 24 小时，请尽快在组织管理页下载。</p>
<p>任务 ID：%s</p>`, start, end, id)
	}
	expStr := "—"
	if expiresAt > 0 {
		expStr = time.Unix(expiresAt, 0).Format("2006-01-02 15:04")
	}
	body += fmt.Sprintf(`<hr><p style="font-size:12px;color:#666">任务 %s · 下载有效期至 %s（24 小时）· 附件为 PDF 摘要</p>`,
		id, expStr)
	return subject, body, attachName
}

func (uc *SummaryUseCase) notifyTrainingReportDone(ctx context.Context, data *TrainingReportData, job *TrainingReportJob, html, pdfPath string) error {
	email := ""
	if data != nil {
		email = data.InitiatorEmail
	}
	if email == "" && job != nil && job.CreatedBy > 0 {
		email = uc.userContactEmail(job.CreatedBy)
	}
	if email == "" {
		return fmt.Errorf("发起人未绑定邮箱")
	}
	brand := uc.brandTitle(ctx)
	subject, body, attachName := BuildNotifyEmail(job, brand, html)

	rt := uc.runtime(ctx)
	sender := mail.NewSender(rt.SMTPConf())
	if !sender.Configured() {
		return fmt.Errorf("SMTP 未配置")
	}
	var atts []mail.Attachment
	if pdfPath != "" {
		if _, err := os.Stat(pdfPath); err == nil {
			atts = append(atts, mail.Attachment{
				Filename:    attachName,
				Path:        pdfPath,
				ContentType: "application/pdf",
			})
		}
	}
	return sender.SendWithAttachments(email, subject, body, atts)
}

// GenerateTrainingReportSync 同步生成（周报管道复用）：返回 HTML，并可选落盘 job
func (uc *SummaryUseCase) GenerateTrainingReportSync(ctx context.Context, p StartTrainingReportParams) (html string, data *TrainingReportData, err error) {
	start, end, err := ParseDateRange(p.StartDate, p.EndDate)
	if err != nil {
		return "", nil, err
	}
	data, err = uc.LoadTrainingReportData(ctx, p.OrgID, p.GroupID, p.CreatedBy, start, end)
	if err != nil {
		return "", nil, err
	}
	brand := uc.brandTitle(ctx)
	if p.UseAI {
		html, err = uc.generateTrainingReportAI(ctx, data)
		if err != nil {
			log.Warnf("weekly training AI fallback: %v", err)
			html = RenderRuleTemplateHTML(data, brand)
			err = nil
		}
	} else {
		html = RenderRuleTemplateHTML(data, brand)
	}
	html = stripCodeFence(html)
	return html, data, nil
}

// DomainAgentTools 训练/周报 AI 可用的域数据工具集。
// toolCtx 必须是 ContextWithElevatedAgent 结果，Bearer 会注入每个工具的 gRPC 调用。
func DomainAgentTools(reg *registry.Registrar, orgID uint, toolCtx context.Context) []agenttool.AgentToolFactory {
	if reg == nil {
		return nil
	}
	_ = orgID // org 已写入 elevated JWT claims
	ctx := toolCtx
	return []agenttool.AgentToolFactory{
		core_data.NewStatisticPeriod(reg, ctx),
		core_data.NewSubmitCnt(reg, ctx),
		core_data.NewSubmitLog(reg, ctx),
		core_data.NewGetProfileById(reg, ctx),
		core_data.NewRankTool(reg, ctx),
		core_data.NewHeatmapTool(reg, ctx),
		core_data.NewOrgMembersTool(reg, ctx),
		core_data.NewGroupMembersTool(reg, ctx),
		core_data.NewLastSubmitTool(reg, ctx),
		core_data.NewPeriodACTool(reg, ctx),
		// 题目标签：全站标签表 / 用户标签画像 / 按 problemId 取标签
		core_data.NewProblemTagsTool(reg, ctx),
	}
}

// DailyAgentTools 个人日报 AI 工具：标签 + 提交明细 + 热力（轻量）。
func DailyAgentTools(reg *registry.Registrar, toolCtx context.Context) []agenttool.AgentToolFactory {
	if reg == nil {
		return nil
	}
	ctx := toolCtx
	return []agenttool.AgentToolFactory{
		core_data.NewProblemTagsTool(reg, ctx),
		core_data.NewSubmitLog(reg, ctx),
		core_data.NewHeatmapTool(reg, ctx),
		core_data.NewPeriodACTool(reg, ctx),
	}
}

// ToolAuthContexts 取出工具携带的 elevated context（测试用）
func ToolAuthContexts(tools []agenttool.AgentToolFactory) []context.Context {
	out := make([]context.Context, 0, len(tools))
	for _, t := range tools {
		type authC interface{ AuthContext() context.Context }
		if a, ok := t.(authC); ok {
			out = append(out, a.AuthContext())
		}
	}
	return out
}

// ResolveArtifactAbs 校验并返回可读文件
func ResolveArtifactAbs(job *TrainingReportJob, preferPDF bool) (abs string, contentType string, name string, err error) {
	if job == nil {
		return "", "", "", fmt.Errorf("job nil")
	}
	if !job.IsDownloadable(time.Now()) {
		return "", "", "", fmt.Errorf("报告不存在或已过期")
	}
	if preferPDF && job.PDFPath != "" {
		abs = job.PDFPath
		contentType = "application/pdf"
		name = job.FileName
		if name == "" {
			name = filepath.Base(abs)
		}
	} else if job.HTMLPath != "" {
		abs = job.HTMLPath
		contentType = "text/html; charset=utf-8"
		name = strings.TrimSuffix(job.FileName, ".pdf") + ".html"
		if job.FileName == "" {
			name = filepath.Base(abs)
		}
	} else if job.PDFPath != "" {
		abs = job.PDFPath
		contentType = "application/pdf"
		name = job.FileName
	} else {
		return "", "", "", fmt.Errorf("无可用文件")
	}
	abs = filepath.Clean(abs)
	base := filepath.Clean(reportDir())
	rel, err := filepath.Rel(base, abs)
	if err != nil || strings.HasPrefix(rel, "..") {
		return "", "", "", fmt.Errorf("非法路径")
	}
	if _, err := os.Stat(abs); err != nil {
		return "", "", "", fmt.Errorf("文件不存在")
	}
	return abs, contentType, name, nil
}
