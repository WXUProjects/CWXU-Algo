package service

import (
	"context"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/data/dal"
	"cwxu-algo/app/user/internal/data/model"

	"github.com/go-kratos/kratos/v2/errors"
	"gorm.io/gorm"
)

var (
	UpdateForbidden = errors.Forbidden("禁止访问", "您无权更新该用户资料")
)

type ProfileService struct {
	profileDal *dal.ProfileDal
}

func (p *ProfileService) Update(ctx context.Context, req *profile.UpdateReq) (*profile.UpdateRes, error) {
	// 校验JWT
	if !auth.VerifyById(ctx, uint(req.UserId)) {
		return nil, UpdateForbidden
	}
	// 构建 User
	pro := model.User{
		Model:  gorm.Model{ID: uint(req.UserId)},
		Avatar: req.Avatar,
		Name:   req.Name,
		Email:  req.Email,
	}
	err := p.profileDal.Update(ctx, pro)
	if err == nil {
		res := &profile.UpdateRes{
			Code:    0,
			Message: "更新成功",
		}
		return res, nil
	}
	return nil, errors.InternalServer("内部错误", err.Error())
}

func (p *ProfileService) GetById(ctx context.Context, req *profile.GetByIdReq) (*profile.GetByIdRes, error) {
	pf, err := p.profileDal.GetById(ctx, req.UserId)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	return &profile.GetByIdRes{
		UserId:   uint64(pf.ID),
		Username: pf.Username,
		Name:     pf.Name,
		Email:    pf.Email,
		Avatar:   pf.Avatar,
		GroupId:  pf.GroupId,
	}, nil
}

func NewProfileService(profileDal *dal.ProfileDal) *ProfileService {
	return &ProfileService{
		profileDal: profileDal,
	}
}
