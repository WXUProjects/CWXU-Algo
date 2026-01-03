package service

import (
	"context"
	_const "cwxu-algo/app/common/const"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/gorm/model"
	"errors"
	"os/user"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/golang-jwt/jwt/v5"

	pb "cwxu-algo/api/user/v1"

	"gorm.io/gorm"
)

type AuthService struct {
	db *gorm.DB
}

func NewAuthService(d *data.Data) *AuthService {
	return &AuthService{
		db: d.DB,
	}
}

// Login 登录
func (s *AuthService) Login(ctx context.Context, req *pb.LoginReq) (*pb.LoginRes, error) {
	res := &pb.LoginRes{}
	// 做校验
	u := &model.User{}
	r := s.db.Where("username = ? and password = ?", req.Username, req.Password).First(&u)
	if errors.Is(r.Error, gorm.ErrRecordNotFound) {
		res.Success = false
		res.Message = "用户名或密码错误"
		return res, nil
	}
	// 制作jwt token
	expire := time.Now().Add(12 * time.Hour) // 过期时间12小时
	jwtToken, err := jwt.NewWithClaims(jwt.SigningMethodHS256, jwt.MapClaims{
		"username": u.Username,
		"name":     u.Name,
		"email":    u.Email,
		"roleIds":  u.RoleIDs,
		"exp":      expire,
		"nbf":      time.Now(),
	}).SignedString([]byte(_const.JWTSecret))
	if err != nil {
		res.Success = false
		res.Message = "身份校验成功，但是jwt生成失败了." + err.Error()
		return res, nil
	}
	res.Success = true
	res.Message = "登录成功"
	res.JwtToken = jwtToken
	log.Info(u.Model.CreatedAt)
	return res, nil
}

// Register 注册
func (s *AuthService) Register(ctx context.Context, req *pb.RegisterReq) (res *pb.RegisterRes, err error) {
	res = &pb.RegisterRes{
		Success: true,
		Message: "注册成功",
	}
	// 是否已经用户名
	users := make([]user.User, 0)
	s.db.Where("username = ?", req.Username).Find(&users)
	if len(users) >= 1 {
		res.Success = false
		res.Message = "用户名已经存在"
		return
	}
	// 尝试去注册
	newUser := &model.User{
		Username: &req.Username,
		Password: &req.Password,
		Avatar:   nil,
		Name:     &req.Name,
		Email:    &req.Email,
		GroupId:  &req.GroupId,
		RoleIDs:  nil,
	}
	r := s.db.Create(&newUser)
	if r.Error != nil {
		res.Success = false
		res.Message = r.Error.Error()
	}
	return
}
