package dal

import (
	"context"
	"cwxu-algo/app/common/utils"
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

func (d *ProfileDal) GetProfileById(userId int64) (*model.User, error) {
	ctx := context.Background()
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	// 尝试去查找 key
	res := d.rdb.Get(ctx, cacheKey)
	rVal, err := res.Result()
	profile := model.User{}
	if err != nil {
		err := d.db.Where("id = ?", userId).First(&profile).Error
		if err != nil {
			return nil, fmt.Errorf("没有找到对应用户信息")
		}
		b, err := utils.GobEncoder(&profile)
		if err != nil {
			return nil, errors.New("gob编码失败")
		}
		d.rdb.Set(ctx, cacheKey, b, 0)
		return &profile, nil
	}
	err = utils.GobDecoder([]byte(rVal), &profile)
	if err != nil {
		err = fmt.Errorf("缓存解析出错 %s", err.Error())
		return nil, err
	}
	return &profile, nil
}
