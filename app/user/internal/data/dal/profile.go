package dal

import (
	"context"
	data2 "cwxu-algo/app/common/data"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"
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

// GetById 根据Id获取用户详细信息
func (d *ProfileDal) GetById(ctx context.Context, userId int64) (*model.User, error) {
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	profile, _, err := data2.GetCacheDal[model.User](ctx, d.rdb, cacheKey, func(data *model.User) error {
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

// Update 更新用户信息
func (d *ProfileDal) Update(ctx context.Context, profile model.User) error {
	cacheKey := fmt.Sprintf("user:%d:profile", profile.ID)
	err := data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		d.db.Model(&model.User{}).Where("id = ?", profile.ID).Updates(map[string]interface{}{
			"avatar": profile.Avatar,
			"email":  profile.Email,
			"name":   profile.Name,
		})
		return nil
	})
	return err
}

func (d *ProfileDal) GetList(ctx context.Context, pageSize, pageNum int64) ([]model.User, int64, error) {
	var list []model.User
	err := d.db.Select("id", "username", "name", "groupId", "avatar").
		Limit(int(pageSize)).Offset(int(pageNum-1) * int(pageSize)).
		Find(&list).Error
	var total int64
	err = d.db.Model(&model.User{}).Count(&total).Error
	if err != nil {
		return nil, 0, err
	}
	return list, (total + pageSize - 1) / pageSize, nil
}
