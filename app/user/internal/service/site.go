package service

import (
	"context"
	"fmt"
	"strings"
	"time"

	"cwxu-algo/api/user/v1/site"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/mail"
	"cwxu-algo/app/common/sitesettings"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
)

type SiteService struct {
	site.UnimplementedSiteServer
	data     *data.Data
	yamlSMTP *conf.SMTP
}

func NewSiteService(d *data.Data, smtp *conf.SMTP) *SiteService {
	return &SiteService{data: d, yamlSMTP: smtp}
}

func (s *SiteService) ensureRow(ctx context.Context) (*model.SiteConfig, error) {
	var row model.SiteConfig
	err := s.data.DB.WithContext(ctx).First(&row, 1).Error
	if err == nil {
		if s.backfillFromYaml(ctx, &row) {
			s.publish(ctx, &row)
		}
		return &row, nil
	}
	row = model.SiteConfig{
		ID:        1,
		SiteTitle: "GoAlgo",
		FooterIcp: "苏ICP备2025217901号",
		SMTPPort:  465,
	}
	if s.yamlSMTP != nil {
		row.SMTPHost = s.yamlSMTP.Host
		row.SMTPPort = int(s.yamlSMTP.Port)
		if row.SMTPPort <= 0 {
			row.SMTPPort = 465
		}
		row.SMTPUsername = s.yamlSMTP.Username
		row.SMTPPassword = s.yamlSMTP.Password
		row.SMTPFrom = s.yamlSMTP.From
	}
	if e := s.data.DB.WithContext(ctx).Create(&row).Error; e != nil {
		if e2 := s.data.DB.WithContext(ctx).First(&row, 1).Error; e2 == nil {
			return &row, nil
		}
		return nil, e
	}
	s.publish(ctx, &row)
	return &row, nil
}

func (s *SiteService) backfillFromYaml(ctx context.Context, row *model.SiteConfig) bool {
	if s.yamlSMTP == nil || row == nil {
		return false
	}
	if strings.TrimSpace(row.SMTPHost) != "" {
		return false
	}
	if strings.TrimSpace(s.yamlSMTP.Host) == "" {
		return false
	}
	port := int(s.yamlSMTP.Port)
	if port <= 0 {
		port = 465
	}
	updates := map[string]interface{}{
		"smtp_host":     s.yamlSMTP.Host,
		"smtp_port":     port,
		"smtp_username": s.yamlSMTP.Username,
		"smtp_password": s.yamlSMTP.Password,
		"smtp_from":     s.yamlSMTP.From,
	}
	if e := s.data.DB.WithContext(ctx).Model(&model.SiteConfig{}).Where("id = ?", 1).Updates(updates).Error; e != nil {
		return false
	}
	row.SMTPHost = s.yamlSMTP.Host
	row.SMTPPort = port
	row.SMTPUsername = s.yamlSMTP.Username
	row.SMTPPassword = s.yamlSMTP.Password
	row.SMTPFrom = s.yamlSMTP.From
	return true
}

func (s *SiteService) publish(ctx context.Context, row *model.SiteConfig) {
	rt := rowToRuntime(row)
	if err := sitesettings.PublishRedis(ctx, s.data.RDB, rt); err != nil {
		log.Warnf("publish site settings redis: %v", err)
	}
}

func rowToRuntime(row *model.SiteConfig) *sitesettings.Runtime {
	if row == nil {
		return &sitesettings.Runtime{SiteTitle: "GoAlgo"}
	}
	port := row.SMTPPort
	if port <= 0 {
		port = 465
	}
	title := strings.TrimSpace(row.SiteTitle)
	if title == "" {
		title = "GoAlgo"
	}
	return &sitesettings.Runtime{
		SiteTitle:         title,
		SMTPHost:          strings.TrimSpace(row.SMTPHost),
		SMTPPort:          port,
		SMTPUsername:      strings.TrimSpace(row.SMTPUsername),
		SMTPPassword:      row.SMTPPassword,
		SMTPFrom:          strings.TrimSpace(row.SMTPFrom),
		AgentModel:        strings.TrimSpace(row.AgentModel),
		AgentSecret:       row.AgentSecret,
		AiAnalyzeEndpoint: strings.TrimSpace(row.AiAnalyzeEndpoint),
		AiAnalyzeModel:    strings.TrimSpace(row.AiAnalyzeModel),
		AiAnalyzeSecret:   row.AiAnalyzeSecret,
	}
}

func (s *SiteService) effectiveRuntime(row *model.SiteConfig) *sitesettings.Runtime {
	return rowToRuntime(row).MergeFallback(s.yamlSMTP, nil, nil)
}

func (s *SiteService) GetConfig(ctx context.Context, _ *site.GetConfigReq) (*site.GetConfigRes, error) {
	row, err := s.ensureRow(ctx)
	if err != nil {
		return nil, errors.InternalServer("site config", err.Error())
	}
	return &site.GetConfigRes{
		Code:      0,
		Message:   "success",
		SiteTitle: row.SiteTitle,
		SiteLogo:  row.SiteLogo,
		Favicon:   row.Favicon,
		FooterIcp: row.FooterIcp,
	}, nil
}

func (s *SiteService) GetAdminConfig(ctx context.Context, _ *site.GetAdminConfigReq) (*site.GetAdminConfigRes, error) {
	if !auth.VerifySiteAdmin(ctx) {
		return &site.GetAdminConfigRes{Code: 1, Message: "仅站点管理员可查看"}, nil
	}
	row, err := s.ensureRow(ctx)
	if err != nil {
		return nil, errors.InternalServer("site config", err.Error())
	}
	rt := s.effectiveRuntime(row)
	return &site.GetAdminConfigRes{
		Code:                  0,
		Message:               "success",
		SiteTitle:             row.SiteTitle,
		SiteLogo:              row.SiteLogo,
		Favicon:               row.Favicon,
		SmtpHost:              rt.SMTPHost,
		SmtpPort:              int32(rt.SMTPPort),
		SmtpUsername:          rt.SMTPUsername,
		SmtpPasswordMasked:    sitesettings.MaskSecret(rt.SMTPPassword),
		SmtpPasswordSet:       strings.TrimSpace(rt.SMTPPassword) != "",
		SmtpFrom:              rt.SMTPFrom,
		AgentModel:            rt.AgentModel,
		AgentSecretMasked:     sitesettings.MaskSecret(rt.AgentSecret),
		AgentSecretSet:        strings.TrimSpace(rt.AgentSecret) != "",
		AiAnalyzeEndpoint:     rt.AiAnalyzeEndpoint,
		AiAnalyzeModel:        rt.AiAnalyzeModel,
		AiAnalyzeSecretMasked: sitesettings.MaskSecret(rt.AiAnalyzeSecret),
		AiAnalyzeSecretSet:    strings.TrimSpace(rt.AiAnalyzeSecret) != "",
		FooterIcp:             row.FooterIcp,
	}, nil
}

func (s *SiteService) UpdateConfig(ctx context.Context, req *site.UpdateConfigReq) (*site.UpdateConfigRes, error) {
	if !auth.VerifySiteAdmin(ctx) {
		return &site.UpdateConfigRes{Code: 1, Message: "仅站点管理员可修改站点配置"}, nil
	}
	row, err := s.ensureRow(ctx)
	if err != nil {
		return nil, errors.InternalServer("site config", err.Error())
	}

	// 管理端表单整页保存：非密钥字段直接覆盖
	title := strings.TrimSpace(req.SiteTitle)
	if title == "" {
		title = row.SiteTitle
		if title == "" {
			title = "GoAlgo"
		}
	}
	port := int(req.SmtpPort)
	if port <= 0 {
		port = 465
	}
	updates := map[string]interface{}{
		"site_title":          title,
		"site_logo":           strings.TrimSpace(req.SiteLogo),
		"favicon":             strings.TrimSpace(req.Favicon),
		"footer_icp":          strings.TrimSpace(req.FooterIcp),
		"smtp_host":           strings.TrimSpace(req.SmtpHost),
		"smtp_port":           port,
		"smtp_username":       strings.TrimSpace(req.SmtpUsername),
		"smtp_from":           strings.TrimSpace(req.SmtpFrom),
		"agent_model":         strings.TrimSpace(req.AgentModel),
		"ai_analyze_endpoint": strings.TrimSpace(req.AiAnalyzeEndpoint),
		"ai_analyze_model":    strings.TrimSpace(req.AiAnalyzeModel),
	}

	if req.ClearSmtpPassword {
		updates["smtp_password"] = ""
	} else if isRealSecret(req.SmtpPassword) {
		updates["smtp_password"] = req.SmtpPassword
	}
	if req.ClearAgentSecret {
		updates["agent_secret"] = ""
	} else if isRealSecret(req.AgentSecret) {
		updates["agent_secret"] = req.AgentSecret
	}
	if req.ClearAiAnalyzeSecret {
		updates["ai_analyze_secret"] = ""
	} else if isRealSecret(req.AiAnalyzeSecret) {
		updates["ai_analyze_secret"] = req.AiAnalyzeSecret
	}

	if e := s.data.DB.WithContext(ctx).Model(&model.SiteConfig{}).Where("id = ?", 1).Updates(updates).Error; e != nil {
		return nil, errors.InternalServer("site config update", e.Error())
	}
	if e := s.data.DB.WithContext(ctx).First(row, 1).Error; e != nil {
		return nil, errors.InternalServer("site config", e.Error())
	}
	s.publish(ctx, row)

	return &site.UpdateConfigRes{
		Code:      0,
		Message:   "success",
		SiteTitle: row.SiteTitle,
		SiteLogo:  row.SiteLogo,
		Favicon:   row.Favicon,
		FooterIcp: row.FooterIcp,
	}, nil
}

func isRealSecret(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	if s == "••••••••" || s == "********" || s == "****" {
		return false
	}
	return true
}

func (s *SiteService) TestEmail(ctx context.Context, req *site.TestEmailReq) (*site.TestEmailRes, error) {
	if !auth.VerifySiteAdmin(ctx) {
		return &site.TestEmailRes{Code: 1, Message: "仅站点管理员可测试邮件", Success: false}, nil
	}
	to := strings.TrimSpace(req.To)
	if to == "" {
		return &site.TestEmailRes{Code: 1, Message: "请填写收件人邮箱", Success: false}, nil
	}

	row, err := s.ensureRow(ctx)
	if err != nil {
		return nil, errors.InternalServer("site config", err.Error())
	}
	rt := s.effectiveRuntime(row)

	host := strings.TrimSpace(req.SmtpHost)
	if host == "" {
		host = rt.SMTPHost
	}
	port := int(req.SmtpPort)
	if port <= 0 {
		port = rt.SMTPPort
	}
	if port <= 0 {
		port = 465
	}
	user := strings.TrimSpace(req.SmtpUsername)
	if user == "" {
		user = rt.SMTPUsername
	}
	pass := req.SmtpPassword
	if !isRealSecret(pass) {
		pass = rt.SMTPPassword
	}
	from := strings.TrimSpace(req.SmtpFrom)
	if from == "" {
		from = rt.SMTPFrom
	}
	if from == "" {
		from = user
	}

	sender := mail.NewSender(&conf.SMTP{
		Host: host, Port: int32(port), Username: user, Password: pass, From: from,
	})
	if !sender.Configured() {
		return &site.TestEmailRes{Code: 1, Message: "SMTP 未配置完整（需要主机）", Success: false}, nil
	}
	title := rt.SiteTitle
	if title == "" {
		title = "GoAlgo"
	}
	subject := fmt.Sprintf("【%s】邮件配置测试", title)
	body := fmt.Sprintf(`<div style="font-family:sans-serif;line-height:1.6">
<p>你好，</p>
<p>这是一封来自 <b>%s</b> 的测试邮件。</p>
<p>若你收到此信，说明 SMTP 配置可用。</p>
<p style="color:#888">发送时间：%s</p>
</div>`, title, time.Now().Format("2006-01-02 15:04:05"))
	if err := sender.Send(to, subject, body); err != nil {
		log.Errorf("test email: %v", err)
		return &site.TestEmailRes{Code: 1, Message: "发送失败：" + err.Error(), Success: false}, nil
	}
	return &site.TestEmailRes{Code: 0, Message: "测试邮件已发送", Success: true}, nil
}
