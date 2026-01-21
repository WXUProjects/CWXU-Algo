package service

import (
	"context"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/user/internal/data/dal"
)

type ProfileService struct {
	profileDal *dal.ProfileDal
}

func (p *ProfileService) GetById(ctx context.Context, req *profile.GetByIdReq) (*profile.GetByIdRes, error) {
	pf, err := p.profileDal.GetProfileById(ctx, req.UserId)
	if err != nil {
		return nil, err
	}
	return &profile.GetByIdRes{
		UserId:   uint64(pf.ID),
		Username: pf.Username,
		Name:     pf.Name,
		Email:    pf.Email,
		Avatar:   pf.Avatar,
		GroupId:  pf.GroupId,
	}, err
}

func NewProfileService(profileDal *dal.ProfileDal) *ProfileService {
	return &ProfileService{
		profileDal: profileDal,
	}
}
