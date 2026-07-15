package service

import (
	"context"
	"errors"
	"strings"
	"time"

	pb "cwxu-algo/api/user/v1/auth"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"

	"gorm.io/gorm"
)

type AuthService struct {
	pb.UnimplementedAuthServer
	db *gorm.DB
}

func NewAuthService(d *data.Data) *AuthService {
	return &AuthService{db: d.DB}
}

func (s *AuthService) Login(ctx context.Context, req *pb.LoginReq) (*pb.LoginRes, error) {
	res := &pb.LoginRes{}
	u := &model.User{}
	r := s.db.Where("username = ? and password = ?", req.Username, req.Password).First(&u)
	if errors.Is(r.Error, gorm.ErrRecordNotFound) {
		res.Success = false
		res.Message = "用户名或密码错误"
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
	var count int64
	if countErr := s.db.Model(&model.User{}).Where("username = ?", req.Username).Count(&count).Error; countErr != nil {
		res.Success = false
		res.Message = "注册失败，请稍后重试"
		return res, nil
	}
	if count >= 1 {
		res.Success = false
		res.Message = "用户名已经存在"
		return
	}

	var public model.Org
	if e := s.db.Where("slug = ?", model.PublicOrgSlug).First(&public).Error; e != nil {
		res.Success = false
		res.Message = "系统未就绪，请稍后重试"
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
		Username:     req.Username,
		Password:     req.Password,
		Name:         req.Name,
		Email:        req.Email,
		GroupId:      int64(defGID),
		RoleID:       0,
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
		OrgDisplayName: strings.TrimSpace(req.Name),
		JoinedAt:       time.Now(),
	}).Error
	return
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
