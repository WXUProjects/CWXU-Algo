package biz

import (
	"context"
	"cwxu-algo/app/user/internal/data/dal"
	"cwxu-algo/app/user/internal/data/model"
)

type ProfileUseCase struct {
	profileDal *dal.ProfileDal
}

func NewProfileUseCase(profileDal *dal.ProfileDal) *ProfileUseCase {
	return &ProfileUseCase{
		profileDal: profileDal,
	}
}

func (uc *ProfileUseCase) GetList(ctx context.Context, pageSize, pageNum int64) ([]model.User, error) {
	return uc.profileDal.GetList(ctx, pageSize, pageNum)
}
