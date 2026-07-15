package mail

import (
	"crypto/tls"
	"fmt"

	"cwxu-algo/app/common/conf"

	"gopkg.in/gomail.v2"
)

// Sender SMTP 邮件发送
type Sender struct {
	host     string
	port     int
	username string
	password string
	from     string
}

// NewSender 从 conf.SMTP 创建发送器；smtp 为 nil 或未配置时仍返回可用对象，Send 会报错
func NewSender(smtp *conf.SMTP) *Sender {
	if smtp == nil {
		return &Sender{}
	}
	return &Sender{
		host:     smtp.Host,
		port:     int(smtp.Port),
		username: smtp.Username,
		password: smtp.Password,
		from:     smtp.From,
	}
}

// Configured 是否已配置 SMTP
func (s *Sender) Configured() bool {
	return s != nil && s.host != ""
}

// Send 发送 HTML 邮件
func (s *Sender) Send(to, subject, body string) error {
	if s == nil || s.host == "" {
		return fmt.Errorf("SMTP服务器未配置")
	}
	if to == "" {
		return fmt.Errorf("收件人地址不能为空")
	}
	m := gomail.NewMessage()
	from := s.from
	if from == "" {
		from = s.username
	}
	m.SetHeader("From", from)
	m.SetHeader("To", to)
	m.SetHeader("Subject", subject)
	m.SetBody("text/html", body)

	d := gomail.NewDialer(s.host, s.port, s.username, s.password)
	d.TLSConfig = &tls.Config{InsecureSkipVerify: true}

	if err := d.DialAndSend(m); err != nil {
		return fmt.Errorf("发送邮件失败: %w", err)
	}
	return nil
}
