package service

import (
	"context"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/user/internal/data"
	"fmt"

	"github.com/redis/go-redis/v9"
)

type ProfileService struct {
	RDB *redis.Client
}

func (p ProfileService) GetById(ctx context.Context, req *profile.GetByIdReq) (*profile.GetByIdRes, error) {
	p.RDB.Set(ctx, fmt.Sprintf("profile:%d", req.UserId), "TestProfile", 0)
	return &profile.GetByIdRes{
		UserId:   0,
		Username: "",
		Name:     "",
		Email:    "",
		GroupId:  0,
	}, nil
}

func NewProfileService(data *data.Data) *ProfileService {
	return &ProfileService{
		RDB: data.RDB,
	}
}
