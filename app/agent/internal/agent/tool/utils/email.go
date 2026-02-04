package utils

import (
	"crypto/tls"
	"encoding/json"
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
	"gopkg.in/gomail.v2"
)

// SendEmailParams 邮件发送参数
type SendEmailParams struct {
	To      string `json:"to"`      // 收件人邮箱地址
	Subject string `json:"subject"` // 邮件标题
	Body    string `json:"body"`    // 邮件内容(支持HTML)
}

// SendEmail 邮件发送工具
type SendEmail struct {
	smtpHost     string
	smtpPort     int
	smtpUsername string
	smtpPassword string
	smtpFrom     string
}

// NewSendEmail 创建邮件发送工具实例
func NewSendEmail(host string, port int, username, password, from string) *SendEmail {
	return &SendEmail{
		smtpHost:     host,
		smtpPort:     port,
		smtpUsername: username,
		smtpPassword: password,
		smtpFrom:     from,
	}
}

// Description 返回工具描述供AI调用
func (e *SendEmail) Description() *model.Tool {
	return &model.Tool{
		Type: model.ToolTypeFunction,
		Function: &model.FunctionDefinition{
			Name:        "send_email",
			Description: "发送HTML格式邮件给指定收件人",
			Parameters: map[string]interface{}{
				"type": "object",
				"properties": map[string]interface{}{
					"to": map[string]interface{}{
						"type":        "string",
						"description": "收件人邮箱地址，例如：user@example.com",
					},
					"subject": map[string]interface{}{
						"type":        "string",
						"description": "邮件标题",
					},
					"body": map[string]interface{}{
						"type":        "string",
						"description": "邮件内容，支持HTML格式",
					},
				},
				"required": []string{"to", "subject", "body"},
			},
		},
	}
}

// AiInterface AI调用接口
func (e *SendEmail) AiInterface(jsonStr string) string {
	params := SendEmailParams{}
	if err := json.Unmarshal([]byte(jsonStr), &params); err != nil {
		log.Errorf("邮件参数解析失败: %v", err)
		return "邮件发送失败：参数格式错误"
	}

	if params.To == "" {
		return "邮件发送失败：收件人地址不能为空"
	}
	if params.Subject == "" {
		return "邮件发送失败：邮件标题不能为空"
	}
	if params.Body == "" {
		return "邮件发送失败：邮件内容不能为空"
	}

	if err := e.Handle(params.To, params.Subject, params.Body); err != nil {
		log.Errorf("邮件发送失败: %v", err)
		return fmt.Sprintf("邮件发送失败：%v", err)
	}
	return "邮件发送成功"
}

// Handle 执行邮件发送
func (e *SendEmail) Handle(to, subject, body string) error {
	if e.smtpHost == "" {
		return fmt.Errorf("SMTP服务器未配置")
	}

	m := gomail.NewMessage()
	m.SetHeader("From", e.smtpFrom)
	m.SetHeader("To", to)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", body)

	d := gomail.NewDialer(e.smtpHost, e.smtpPort, e.smtpUsername, e.smtpPassword)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}

	if err := d.DialAndSend(m); err != nil {
		return fmt.Errorf("发送邮件失败: %w", err)
	}

	return nil
}
