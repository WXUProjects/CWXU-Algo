package service

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"regexp"
	"strings"
	"time"

	spiderpb "cwxu-algo/api/core/v1/spider"
	pb "cwxu-algo/api/user/v1/auth"
	"cwxu-algo/app/common/conf"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/mail"
	"cwxu-algo/app/common/sitesettings"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/common/utils/clientip"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/dal"
	"cwxu-algo/app/user/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/redis/go-redis/v9"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	codeTTL            = 10 * time.Minute
	codeCooldown       = 60 * time.Second
	purposeRegister    = "register"
	purposeReset       = "reset"
	codeKeyPrefix      = "auth:code:"
	cooldownPrefix     = "auth:code:cd:"
	codeAttemptPrefix  = "auth:code:attempt:"
	loginAttemptPrefix = "auth:login:attempt:"
	maxCodeAttempts    = 5
	maxLoginAttempts   = 8
	maxAccountAttempts = 30
)

var emailRe = regexp.MustCompile(`^[^\s@]+@[^\s@]+\.[^\s@]+$`)
var clientPasswordRe = regexp.MustCompile(`^[a-fA-F0-9]{64}$`)
var usernameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{3,64}$`)

type AuthService struct {
	pb.UnimplementedAuthServer
	db         *gorm.DB
	rdb        *redis.Client
	yamlSMTP   *conf.SMTP
	reg        *discovery.Register
	profileDal *dal.ProfileDal
}

func NewAuthService(d *data.Data, smtp *conf.SMTP, reg *discovery.Register) *AuthService {
	return &AuthService{
		db:         d.DB,
		rdb:        d.RDB,
		yamlSMTP:   smtp,
		reg:        reg,
		profileDal: dal.NewProfileDal(d),
	}
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
	if account == "" || !clientPasswordRe.MatchString(req.Password) {
		res.Success = false
		res.Message = "请输入账号和密码"
		return res, nil
	}

	accountDigest := sha256.Sum256([]byte(strings.ToLower(account)))
	digest := hex.EncodeToString(accountDigest[:])
	accountAttemptKey := loginAttemptPrefix + "account:" + digest
	pairAttemptKey := loginAttemptPrefix + "pair:" + digest + ":" + clientip.FromContext(ctx)
	allowed, limitErr := s.allowLoginAttempt(ctx, pairAttemptKey, maxLoginAttempts)
	if limitErr == nil && allowed {
		allowed, limitErr = s.allowLoginAttempt(ctx, accountAttemptKey, maxAccountAttempts)
	}
	if limitErr != nil {
		res.Message = "登录服务暂不可用，请稍后重试"
		return res, nil
	}
	if !allowed {
		res.Message = "尝试次数过多，请 15 分钟后再试"
		return res, nil
	}

	u := &model.User{}
	var r *gorm.DB
	if strings.Contains(account, "@") {
		r = s.db.Where("LOWER(email) = ?", strings.ToLower(account)).First(&u)
	} else {
		r = s.db.Where("username = ?", account).First(&u)
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
	if !passwordMatches(u.Password, req.Password) {
		res.Message = "用户名或密码错误"
		return res, nil
	}
	// Legacy rows stored the replayable client SHA256 directly. Upgrade in place
	// after a successful login without forcing a password reset.
	if clientPasswordRe.MatchString(u.Password) {
		if hashed, hashErr := hashClientPassword(req.Password); hashErr == nil {
			_ = s.db.Model(u).Update("password", hashed).Error
		}
	}
	_ = s.rdb.Del(ctx, pairAttemptKey, accountAttemptKey).Err()

	// 休眠判定须在 touch last_login 之前
	wasDormant := false
	if s.profileDal != nil {
		wasDormant = s.profileDal.IsUserDormant(ctx, u)
	}
	now := time.Now()
	if err := s.db.Model(u).Update("last_login_at", now).Error; err != nil {
		log.Warnf("login touch last_login user=%d: %v", u.ID, err)
	} else {
		u.LastLoginAt = &now
	}

	token, err := IssueJWT(s.db, u)
	if err != nil {
		res.Success = false
		log.Errorf("issue login jwt: %v", err)
		res.Message = "登录暂时不可用，请稍后重试"
		return res, nil
	}
	res.Success = true
	res.Message = "登录成功"
	res.JwtToken = token
	res.WasDormant = wasDormant
	if wasDormant {
		syncStarted := s.enqueueWakeSpider(int64(u.ID))
		res.SyncStarted = syncStarted
		res.Message = "欢迎回来！检测到你一段时间未登录，正在全量同步 OJ 数据，请稍候刷新查看。"
	}
	setSessionCookie(ctx, token)
	return res, nil
}

// enqueueWakeSpider 休眠唤醒：全量入队爬虫（异步 gRPC）
func (s *AuthService) enqueueWakeSpider(userID int64) bool {
	if userID <= 0 || s.reg == nil {
		return false
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
		defer cancel()
		conn, err := grpc.DialInsecure(
			ctx,
			grpc.WithEndpoint("discovery:///core-data"),
			grpc.WithDiscovery(s.reg.Reg.(registry.Discovery)),
			grpc.WithTimeout(12*time.Second),
		)
		if err != nil {
			log.Warnf("wake spider dial user=%d: %v", userID, err)
			return
		}
		defer conn.Close()
		cli := spiderpb.NewSpiderClient(conn)
		res, err := cli.EnqueueUserSpider(ctx, &spiderpb.EnqueueUserSpiderReq{
			UserId:  userID,
			NeedAll: true,
		})
		if err != nil {
			log.Warnf("wake spider enqueue user=%d: %v", userID, err)
			return
		}
		if res != nil && res.Code != 0 {
			log.Warnf("wake spider enqueue user=%d code=%d msg=%s", userID, res.Code, res.Message)
			return
		}
		log.Infof("wake spider enqueued user=%d published=%d", userID, res.GetPublished())
	}()
	return true
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
	if !usernameRe.MatchString(username) {
		res.Success = false
		res.Message = "用户名需为 3–64 位字母、数字、下划线或短横线"
		return res, nil
	}
	if len([]rune(name)) > 64 || len(email) > 320 {
		res.Success = false
		res.Message = "姓名或邮箱过长"
		return res, nil
	}
	if !clientPasswordRe.MatchString(req.Password) {
		res.Success = false
		res.Message = "密码格式无效"
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

	hashedPassword, hashErr := hashClientPassword(req.Password)
	if hashErr != nil {
		res.Success = false
		res.Message = "注册失败，请稍后重试"
		return res, nil
	}
	err = s.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var public model.Org
		if e := tx.Clauses(clause.Locking{Strength: "UPDATE"}).Where("slug = ?", model.PublicOrgSlug).First(&public).Error; e != nil {
			return e
		}
		if msg := seatFullMessage(tx, &public); msg != "" {
			return fmt.Errorf("seat:%s", msg)
		}
		var defG model.Group
		if e := tx.Where("org_id = ? AND name IN ?", public.ID, []string{model.DefaultGroupName, "未分组"}).
			Order("id ASC").First(&defG).Error; e != nil {
			if !errors.Is(e, gorm.ErrRecordNotFound) {
				return e
			}
			n := model.DefaultGroupName
			defG = model.Group{Name: &n, Describe: model.DefaultGroupDesc, OrgID: public.ID}
			if e = tx.Create(&defG).Error; e != nil {
				return e
			}
		}
		newUser := &model.User{
			Username: username, Password: hashedPassword, Name: name, Email: email,
			GroupId: int64(defG.ID), RoleID: 0, CurrentOrgID: public.ID,
		}
		if e := tx.Create(newUser).Error; e != nil {
			return e
		}
		groupID := defG.ID
		return tx.Create(&model.OrgMember{
			OrgID: public.ID, UserID: newUser.ID, Role: model.OrgRoleMember,
			GroupID: &groupID, OrgDisplayName: name, JoinedAt: time.Now(),
		}).Error
	})
	if err != nil {
		res.Success = false
		if strings.HasPrefix(err.Error(), "seat:") {
			res.Message = strings.TrimPrefix(err.Error(), "seat:")
		} else if isUniqueViolation(err) {
			res.Message = "用户名或邮箱已被注册"
		} else {
			log.Errorf("register transaction failed: %v", err)
			res.Message = "注册失败，请稍后重试"
		}
		return res, nil
	}

	return
}

// SendCode 发送邮箱验证码（register | reset | change_email）
func (s *AuthService) SendCode(ctx context.Context, req *pb.SendCodeReq) (*pb.SendCodeRes, error) {
	res := &pb.SendCodeRes{}
	email := strings.ToLower(strings.TrimSpace(req.Email))
	purpose := strings.TrimSpace(strings.ToLower(req.Purpose))
	if purpose == "" {
		purpose = purposeRegister
	}
	if purpose != purposeRegister && purpose != purposeReset && purpose != purposeChangeEmail {
		res.Success = false
		res.Message = "无效的验证用途"
		return res, nil
	}
	if !emailRe.MatchString(email) {
		res.Success = false
		res.Message = "请输入有效邮箱"
		return res, nil
	}
	// 绑定/修改邮箱须登录
	if purpose == purposeChangeEmail {
		pd := auth.GetCurrentUser(ctx)
		if pd == nil || pd.UserID == 0 {
			res.Success = false
			res.Message = "请先登录后再绑定邮箱"
			return res, nil
		}
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
	if purpose == purposeChangeEmail {
		pd := auth.GetCurrentUser(ctx)
		if count >= 1 {
			var owner model.User
			if err := s.db.Select("id").Where("LOWER(email) = ?", email).First(&owner).Error; err == nil && pd != nil {
				if owner.ID == pd.UserID {
					res.Success = false
					res.Message = "这已是你当前绑定的邮箱"
					return res, nil
				}
				res.Success = false
				res.Message = "该邮箱已被其他账号使用"
				return res, nil
			}
		}
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
	if !clientPasswordRe.MatchString(oldPwd) || !clientPasswordRe.MatchString(newPwd) {
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
	if !passwordMatches(u.Password, oldPwd) {
		res.Success = false
		res.Message = "当前密码不正确"
		return res, nil
	}
	hashed, err := hashClientPassword(newPwd)
	if err != nil || s.db.Model(&u).Update("password", hashed).Error != nil {
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
	if code == "" || !clientPasswordRe.MatchString(req.Password) {
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
	hashed, err := hashClientPassword(req.Password)
	if err != nil || s.db.Model(&u).Update("password", hashed).Error != nil {
		res.Success = false
		res.Message = "重置失败，请稍后重试"
		return res, nil
	}
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
		log.Errorf("issue refresh jwt: %v", err)
		res.Message = "刷新登录状态失败，请稍后重试"
		return res, nil
	}
	res.Success = true
	res.Message = "已刷新"
	res.JwtToken = token
	setSessionCookie(ctx, token)
	return res, nil
}

func (s *AuthService) verifyCode(ctx context.Context, purpose, email, code string) bool {
	return VerifyEmailCode(ctx, s.rdb, purpose, email, code)
}

func (s *AuthService) allowLoginAttempt(ctx context.Context, key string, maximum int64) (bool, error) {
	if s.rdb == nil {
		return false, fmt.Errorf("redis unavailable")
	}
	n, err := s.rdb.Incr(ctx, key).Result()
	if err != nil {
		return false, err
	}
	if n == 1 {
		if err := s.rdb.Expire(ctx, key, 15*time.Minute).Err(); err != nil {
			return false, err
		}
	}
	return n <= maximum, nil
}

func hashClientPassword(clientHash string) (string, error) {
	if !clientPasswordRe.MatchString(clientHash) {
		return "", fmt.Errorf("invalid client password hash")
	}
	b, err := bcrypt.GenerateFromPassword([]byte(strings.ToLower(clientHash)), bcrypt.DefaultCost)
	return string(b), err
}

func passwordMatches(stored, supplied string) bool {
	if !clientPasswordRe.MatchString(supplied) {
		return false
	}
	supplied = strings.ToLower(supplied)
	if clientPasswordRe.MatchString(stored) {
		return subtle.ConstantTimeCompare([]byte(strings.ToLower(stored)), []byte(supplied)) == 1
	}
	return bcrypt.CompareHashAndPassword([]byte(stored), []byte(supplied)) == nil
}

func isUniqueViolation(err error) bool {
	if err == nil {
		return false
	}
	s := strings.ToLower(err.Error())
	return strings.Contains(s, "duplicate key") || strings.Contains(s, "unique constraint")
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
	case purposeChangeEmail:
		subject = fmt.Sprintf("【%s】绑定邮箱验证码", brand)
		body = fmt.Sprintf(`<div style="font-family:sans-serif;line-height:1.6">
<p>你好，</p>
<p>你正在为 %s 账号绑定或更换此邮箱，验证码为：</p>
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
