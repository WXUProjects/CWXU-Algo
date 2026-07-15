package service

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

	pb "cwxu-algo/api/user/v1/auth"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/mail"
	"cwxu-algo/app/common/sitesettings"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

const (
	codeTTL          = 10 * time.Minute
	codeCooldown      = 60 * time.Second
	purposeRegister  = "register"
	purposeReset     = "reset"
	codeKeyPrefix    = "auth:code:"
	cooldownPrefix   = "auth:code:cd:"
)

var emailRe = regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)

type AuthService struct {
	pb.UnimplementedAuthServer
	db       *gorm.DB
	rdb      *redis.Client
	yamlSMTP *conf.SMTP
}

func NewAuthService(d *data.Data, smtp *conf.SMTP) *AuthService {
	return &AuthService{db: d.DB, rdb: d.RDB, yamlSMTP: smtp}
}

func (s *AuthService) runtime(ctx context.Context) *sitesettings.Runtime {
	rt := sitesettings.LoadPreferDB(ctx, s.db, s.rdb)
	return rt.MergeFallback(s.yamlSMTP, nil, nil)
}

func (s *AuthService) mailSender(ctx context.Context) *mail.Sender {
	return s.runtime(ctx).MailSender()
}

func (s *AuthService) siteTitle(ctx context.Context) string {
	t := strings.TrimSpace(s.runtime(ctx).SiteTitle)
	if t == "" {
		return "GoAlgo"
	}
	return t
}

func (s *AuthService) Login(ctx context.Context, req *pb.LoginReq) (*pb.LoginRes, error) {
	res := &pb.LoginRes{}
	account := strings.TrimSpace(req.Username)
	if account == "" || req.Password == "" {
		res.Success = false
		res.Message = "请输入账号和密码"
		return res, nil
	}

	u := &model.User{}
	var r *gorm.DB
	if strings.Contains(account, "@") {
		r = s.db.Where("LOWER(email) = ? AND password = ?", strings.ToLower(account), req.Password).First(&u)
	} else {
		r = s.db.Where("username = ? AND password = ?", account, req.Password).First(&u)
	}
	if errors.Is(r.Error, gorm.ErrRecordNotFound) {
		res.Success = false
		res.Message = "用户名或密码错误"
		return res, nil
	}
	if r.Error != nil {
		res.Success = false
		res.Message = "登录失败，请稍后重试"
		return res, nil
	}
	token, err := IssueJWT(s.db, u)
	if err != nil {
		res.Success = false
		res.Message = "身份校验成功，但是jwt生成失败了." + err.Error()
		return res, nil
	}
	res.Success = true
	res.Message = "登录成功"
	res.JwtToken = token
	return res, nil
}

func (s *AuthService) Register(ctx context.Context, req *pb.RegisterReq) (res *pb.RegisterRes, err error) {
	res = &pb.RegisterRes{Success: true, Message: "注册成功"}

	username := strings.TrimSpace(req.Username)
	email := strings.ToLower(strings.TrimSpace(req.Email))
	name := strings.TrimSpace(req.Name)
	code := strings.TrimSpace(req.Code)

	if username == "" || req.Password == "" || name == "" || email == "" {
		res.Success = false
		res.Message = "请填写所有必填项"
		return res, nil
	}
	if !emailRe.MatchString(email) {
		res.Success = false
		res.Message = "请输入有效邮箱"
		return res, nil
	}
	if code == "" {
		res.Success = false
		res.Message = "请输入邮箱验证码"
		return res, nil
	}
	if !s.verifyCode(ctx, purposeRegister, email, code) {
		res.Success = false
		res.Message = "验证码错误或已过期"
		return res, nil
	}

	var count int64
	if countErr := s.db.Model(&model.User{}).Where("username = ?", username).Count(&count).Error; countErr != nil {
		res.Success = false
		res.Message = "注册失败，请稍后重试"
		return res, nil
	}
	if count >= 1 {
		res.Success = false
		res.Message = "用户名已经存在"
		return
	}
	if countErr := s.db.Model(&model.User{}).Where("LOWER(email) = ?", email).Count(&count).Error; countErr != nil {
		res.Success = false
		res.Message = "注册失败，请稍后重试"
		return res, nil
	}
	if count >= 1 {
		res.Success = false
		res.Message = "该邮箱已被注册"
		return
	}

	var public model.Org
	if e := s.db.Where("slug = ?", model.PublicOrgSlug).First(&public).Error; e != nil {
		res.Success = false
		res.Message = "系统未就绪，请稍后重试"
		return
	}
	// 公共域仅统计「只属于公共域」的用户；注册占用公共域席位
	if msg := seatFullMessage(s.db, &public); msg != "" {
		res.Success = false
		res.Message = msg
		return
	}

	// 公共域默认分组
	var defG model.Group
	defGID := uint(0)
	if e := s.db.Where("org_id = ? AND name IN ?", public.ID, []string{model.DefaultGroupName, "未分组"}).
		Order("id ASC").First(&defG).Error; e == nil {
		defGID = defG.ID
		if defG.Name != nil && *defG.Name == "未分组" {
			n := model.DefaultGroupName
			_ = s.db.Model(&defG).Updates(map[string]interface{}{"name": n, "describe": model.DefaultGroupDesc}).Error
		}
	} else {
		n := model.DefaultGroupName
		defG = model.Group{Name: &n, Describe: model.DefaultGroupDesc, OrgID: public.ID}
		if s.db.Create(&defG).Error == nil {
			defGID = defG.ID
		}
	}

	newUser := &model.User{
		Username:           username,
		Password:           req.Password,
		Name:               name,
		Email:              email,
		GroupId:            int64(defGID),
		RoleID:             0,
		IsSiteAdmin:        false,
		CurrentOrgID:       public.ID,
		EmailEnabled:       false,
		EmailWeeklyEnabled: false,
	}
	if r := s.db.Create(&newUser); r.Error != nil {
		res.Success = false
		res.Message = r.Error.Error()
		return
	}
	var memGid *uint
	if defGID > 0 {
		memGid = &defGID
	}
	_ = s.db.Create(&model.OrgMember{
		OrgID:          public.ID,
		UserID:         newUser.ID,
		Role:           model.OrgRoleMember,
		GroupID:        memGid,
		OrgDisplayName: name,
		JoinedAt:       time.Now(),
	}).Error

	s.consumeCode(ctx, purposeRegister, email)
	return
}

// SendCode 发送邮箱验证码（register | reset）
func (s *AuthService) SendCode(ctx context.Context, req *pb.SendCodeReq) (*pb.SendCodeRes, error) {
	res := &pb.SendCodeRes{}
	email := strings.ToLower(strings.TrimSpace(req.Email))
	purpose := strings.TrimSpace(strings.ToLower(req.Purpose))
	if purpose == "" {
		purpose = purposeRegister
	}
	if purpose != purposeRegister && purpose != purposeReset {
		res.Success = false
		res.Message = "无效的验证用途"
		return res, nil
	}
	if !emailRe.MatchString(email) {
		res.Success = false
		res.Message = "请输入有效邮箱"
		return res, nil
	}
	sender := s.mailSender(ctx)
	if sender == nil || !sender.Configured() {
		res.Success = false
		res.Message = "邮件服务未配置，请联系管理员"
		return res, nil
	}

	var count int64
	_ = s.db.Model(&model.User{}).Where("LOWER(email) = ?", email).Count(&count)
	if purpose == purposeRegister && count >= 1 {
		res.Success = false
		res.Message = "该邮箱已被注册"
		return res, nil
	}
	if purpose == purposeReset && count == 0 {
		// 防枚举：统一成功文案，但不发信
		res.Success = true
		res.Message = "若该邮箱已注册，验证码将很快送达"
		return res, nil
	}

	cdKey := cooldownPrefix + purpose + ":" + email
	if n, err := s.rdb.Exists(ctx, cdKey).Result(); err == nil && n > 0 {
		res.Success = false
		res.Message = "发送过于频繁，请稍后再试"
		return res, nil
	}

	code, err := genDigits(6)
	if err != nil {
		res.Success = false
		res.Message = "生成验证码失败"
		return res, nil
	}

	codeKey := codeKeyPrefix + purpose + ":" + email
	if err := s.rdb.Set(ctx, codeKey, code, codeTTL).Err(); err != nil {
		log.Errorf("写入验证码失败: %v", err)
		res.Success = false
		res.Message = "服务繁忙，请稍后重试"
		return res, nil
	}
	_ = s.rdb.Set(ctx, cdKey, "1", codeCooldown).Err()

	subject, body := codeMailContent(purpose, code, s.siteTitle(ctx))
	if err := sender.Send(email, subject, body); err != nil {
		log.Errorf("发送验证码邮件失败: %v", err)
		_ = s.rdb.Del(ctx, codeKey).Err()
		res.Success = false
		res.Message = "邮件发送失败，请稍后重试"
		return res, nil
	}

	res.Success = true
	res.Message = "验证码已发送，请查收邮箱"
	return res, nil
}

// ChangePassword 登录态修改密码（校验旧密码）
func (s *AuthService) ChangePassword(ctx context.Context, req *pb.ChangePasswordReq) (*pb.ChangePasswordRes, error) {
	res := &pb.ChangePasswordRes{}
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		res.Success = false
		res.Message = "请先登录"
		return res, nil
	}
	oldPwd := strings.TrimSpace(req.OldPassword)
	newPwd := strings.TrimSpace(req.NewPassword)
	if oldPwd == "" || newPwd == "" {
		res.Success = false
		res.Message = "请填写当前密码和新密码"
		return res, nil
	}
	if oldPwd == newPwd {
		res.Success = false
		res.Message = "新密码不能与当前密码相同"
		return res, nil
	}
	var u model.User
	if err := s.db.First(&u, pd.UserID).Error; err != nil {
		res.Success = false
		res.Message = "用户不存在"
		return res, nil
	}
	if u.Password != oldPwd {
		res.Success = false
		res.Message = "当前密码不正确"
		return res, nil
	}
	if err := s.db.Model(&u).Update("password", newPwd).Error; err != nil {
		res.Success = false
		res.Message = "修改失败，请稍后重试"
		return res, nil
	}
	res.Success = true
	res.Message = "密码已更新"
	return res, nil
}

// ResetPassword 邮箱验证码重置密码
func (s *AuthService) ResetPassword(ctx context.Context, req *pb.ResetPasswordReq) (*pb.ResetPasswordRes, error) {
	res := &pb.ResetPasswordRes{}
	email := strings.ToLower(strings.TrimSpace(req.Email))
	code := strings.TrimSpace(req.Code)
	if !emailRe.MatchString(email) {
		res.Success = false
		res.Message = "请输入有效邮箱"
		return res, nil
	}
	if code == "" || req.Password == "" {
		res.Success = false
		res.Message = "请填写验证码和新密码"
		return res, nil
	}
	if !s.verifyCode(ctx, purposeReset, email, code) {
		res.Success = false
		res.Message = "验证码错误或已过期"
		return res, nil
	}

	var u model.User
	if err := s.db.Where("LOWER(email) = ?", email).First(&u).Error; err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			res.Success = false
			res.Message = "该邮箱未注册"
			return res, nil
		}
		res.Success = false
		res.Message = "重置失败，请稍后重试"
		return res, nil
	}
	if err := s.db.Model(&u).Update("password", req.Password).Error; err != nil {
		res.Success = false
		res.Message = "重置失败，请稍后重试"
		return res, nil
	}
	s.consumeCode(ctx, purposeReset, email)
	res.Success = true
	res.Message = "密码已重置，请使用新密码登录"
	return res, nil
}

// Refresh 根据当前 JWT 用户从 DB 重签 token（角色/组织变更后 F5 即可同步）
func (s *AuthService) Refresh(ctx context.Context, _ *pb.RefreshReq) (*pb.LoginRes, error) {
	res := &pb.LoginRes{}
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		res.Success = false
		res.Message = "请先登录"
		return res, nil
	}
	var u model.User
	if err := s.db.First(&u, pd.UserID).Error; err != nil {
		res.Success = false
		res.Message = "用户不存在"
		return res, nil
	}
	token, err := IssueJWT(s.db, &u)
	if err != nil {
		res.Success = false
		res.Message = "jwt 生成失败: " + err.Error()
		return res, nil
	}
	res.Success = true
	res.Message = "已刷新"
	res.JwtToken = token
	return res, nil
}

func (s *AuthService) verifyCode(ctx context.Context, purpose, email, code string) bool {
	if s.rdb == nil || code == "" {
		return false
	}
	key := codeKeyPrefix + purpose + ":" + email
	stored, err := s.rdb.Get(ctx, key).Result()
	if err != nil {
		return false
	}
	return stored == code
}

func (s *AuthService) consumeCode(ctx context.Context, purpose, email string) {
	if s.rdb == nil {
		return
	}
	_ = s.rdb.Del(ctx, codeKeyPrefix+purpose+":"+email).Err()
}

func genDigits(n int) (string, error) {
	var b strings.Builder
	b.Grow(n)
	for i := 0; i < n; i++ {
		v, err := rand.Int(rand.Reader, big.NewInt(10))
		if err != nil {
			return "", err
		}
		b.WriteByte(byte('0' + v.Int64()))
	}
	return b.String(), nil
}

func codeMailContent(purpose, code, brand string) (subject, body string) {
	if brand == "" {
		brand = "GoAlgo"
	}
	switch purpose {
	case purposeReset:
		subject = fmt.Sprintf("【%s】密码重置验证码", brand)
		body = fmt.Sprintf(`<div style="font-family:sans-serif;line-height:1.6">
<p>你好，</p>
<p>你正在重置 %s 账号密码，验证码为：</p>
<p style="font-size:24px;font-weight:bold;letter-spacing:4px">%s</p>
<p>验证码 %d 分钟内有效。如非本人操作，请忽略本邮件。</p>
<p style="color:#888">%s</p>
</div>`, brand, code, int(codeTTL.Minutes()), brand)
	default:
		subject = fmt.Sprintf("【%s】注册验证码", brand)
		body = fmt.Sprintf(`<div style="font-family:sans-serif;line-height:1.6">
<p>你好，</p>
<p>你正在注册 %s 账号，验证码为：</p>
<p style="font-size:24px;font-weight:bold;letter-spacing:4px">%s</p>
<p>验证码 %d 分钟内有效。如非本人操作，请忽略本邮件。</p>
<p style="color:#888">%s</p>
</div>`, brand, code, int(codeTTL.Minutes()), brand)
	}
	return
}
