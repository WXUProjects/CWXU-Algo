package service

import (
	"context"
	"time"

	pb "cwxu-algo/api/user/v1/auth"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"

	"errors"

	"gorm.io/gorm"
)

type AuthService struct {
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

	newUser := &model.User{
		Username:     req.Username,
		Password:     req.Password,
		Name:         req.Name,
		Email:        req.Email,
		GroupId:      0,
		RoleID:       0,
		IsSiteAdmin:  false,
		CurrentOrgID: public.ID,
		EmailEnabled: true,
	}
	if r := s.db.Create(&newUser); r.Error != nil {
		res.Success = false
		res.Message = r.Error.Error()
		return
	}
	_ = s.db.Create(&model.OrgMember{
		OrgID:    public.ID,
		UserID:   newUser.ID,
		Role:     model.OrgRoleMember,
		JoinedAt: time.Now(),
	}).Error
	return
}
