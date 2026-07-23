package utils

import (
	"encoding/json"
	"fmt"

	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/mail"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
)

// SendEmailParams 邮件发送参数
type SendEmailParams struct {
	To      string `json:"to"`      // 收件人邮箱地址
	Subject string `json:"subject"` // 邮件标题
	Body    string `json:"body"`    // 邮件内容(支持HTML)
}

// SendEmail 邮件发送工具（委托 common/mail，与全站 SMTP 行为一致）
type SendEmail struct {
	sender *mail.Sender
}

// NewSendEmail 创建邮件发送工具实例
func NewSendEmail(host string, port int, username, password, from string) *SendEmail {
	return &SendEmail{
		sender: mail.NewSender(&conf.SMTP{
			Host:     host,
			Port:     int32(port),
			Username: username,
			Password: password,
			From:     from,
		}),
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
	if e == nil || e.sender == nil || !e.sender.Configured() {
		return fmt.Errorf("SMTP服务器未配置")
	}
	// 片段则套统一品牌壳，完整报告文档原样发送
	if !mail.IsFullHTMLDocument(body) {
		body = mail.Wrap(mail.LayoutOpts{Brand: mail.DefaultBrand, Title: subject}, body)
	}
	return e.sender.Send(to, subject, body)
}
