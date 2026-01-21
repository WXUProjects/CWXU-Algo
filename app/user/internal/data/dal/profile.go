package dal

import (
	"context"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/gorm/model"
	"errors"
	"fmt"

	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

type ProfileDal struct {
	db  *gorm.DB
	rdb *redis.Client
}

func NewProfileDal(data *data.Data) *ProfileDal {
	return &ProfileDal{db: data.DB, rdb: data.RDB}
}

func (d *ProfileDal) GetProfileById(ctx context.Context, userId int64) (*model.User, error) {
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	profile, _, err := GetCacheDal[model.User](ctx, d.rdb, cacheKey, func(data *model.User) error {
		err := d.db.Where("id = ?", userId).First(data).Error
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return fmt.Errorf("没有找到相关用户信息")
		} else if err != nil {
			return fmt.Errorf("未知错误 %s", err.Error())
		}
		return nil
	})
	return profile, err
}
