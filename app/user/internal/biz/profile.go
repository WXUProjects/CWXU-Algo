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

func (uc *ProfileUseCase) GetList(ctx context.Context, pageSize, pageNum int64, keyword string, dormantOnly bool) ([]model.User, int64, error) {
	return uc.profileDal.GetList(ctx, pageSize, pageNum, keyword, dormantOnly)
}

func (uc *ProfileUseCase) GetUserIdsByGroup(ctx context.Context, groupId int64) ([]int64, error) {
	return uc.profileDal.GetUserIdsByGroup(ctx, groupId)
}

func (uc *ProfileUseCase) GetByIds(ctx context.Context, userIds []int64) ([]dal.UserProfile, error) {
	return uc.profileDal.GetByIds(ctx, userIds)
}
