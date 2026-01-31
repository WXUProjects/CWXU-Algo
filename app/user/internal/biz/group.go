package biz

import (
	"context"
	"cwxu-algo/app/user/internal/data/dal"
	"cwxu-algo/app/user/internal/data/model"
)

type GroupUseCase struct {
	groupDal *dal.GroupDal
}

func NewGroupUseCase(groupDal *dal.GroupDal) *GroupUseCase {
	return &GroupUseCase{
		groupDal: groupDal,
	}
}

func (uc *GroupUseCase) Create(ctx context.Context, name, describe string) (int64, error) {
	return uc.groupDal.Create(ctx, name, describe)
}

func (uc *GroupUseCase) Delete(ctx context.Context, id int64) error {
	return uc.groupDal.Delete(ctx, id)
}

func (uc *GroupUseCase) Get(ctx context.Context, id int64) (*model.Group, error) {
	return uc.groupDal.Get(ctx, id)
}

func (uc *GroupUseCase) GetWithUsers(ctx context.Context, id int64) (*model.Group, []model.User, error) {
	return uc.groupDal.GetWithUsers(ctx, id)
}

func (uc *GroupUseCase) List(ctx context.Context, page, size int64) ([]model.Group, int64, error) {
	return uc.groupDal.List(ctx, page, size)
}

func (uc *GroupUseCase) Update(ctx context.Context, id int64, name, describe string) error {
	return uc.groupDal.Update(ctx, id, name, describe)
}