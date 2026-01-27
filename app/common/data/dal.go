package data

import (
	"context"
	"cwxu-algo/app/common/utils"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// GetCacheDal 通用带Redis缓存的Dal查询操作层
//
// 泛型参数:
//   - T any 一般要传入一个model结构体
//
// 参数:
//   - ctx context 上下文
//   - rdb *redis.Client Redis客户端
//   - cacheKey string Redis缓存键
//   - dbFunc 数据库兜底策略查询
//
// 返回值:
//   - *T 返回的查询结果
//   - bool 是否命中缓存
//   - error 错误信息
func GetCacheDal[T any](
	ctx context.Context,
	rdb *redis.Client,
	cacheKey string,
	dbFunc func(data *T) error,
) (*T, bool, error) {
	// 尝试去查 Redis
	res := rdb.Get(ctx, cacheKey)
	rVal, err := res.Result()
	var data T
	if err != nil {
		// 降级回数据库
		err := dbFunc(&data)
		if err != nil {
			return nil, false, err
		}
		b, err := utils.GobEncoder(&data)
		if err != nil {
			return nil, false, errors.New("gob编码失败")
		}
		rdb.Set(ctx, cacheKey, b, 48*time.Hour)
		return &data, false, nil
	}
	err = utils.GobDecoder([]byte(rVal), &data)
	if err != nil {
		err = fmt.Errorf("缓存解析出错 %s", err.Error())
		return nil, false, err
	}
	return &data, true, nil
}

// UpdateCacheDal 通用带Redis缓存的Dal更新操作层
//
// 参数:
//   - ctx context 上下文
//   - rdb *redis.Client Redis客户端
//   - cacheKey string Redis缓存键
//   - dbFunc 数据库更新策略
//
// 返回值:
//   - error 错误信息
func UpdateCacheDal(
	ctx context.Context,
	rdb *redis.Client,
	cacheKey string,
	dbFunc func() error,
) error {
	err := dbFunc()
	if err != nil {
		return err
	}
	// 更新成功后把Redis缓存删了
	_ = rdb.Del(ctx, cacheKey)
	return nil
}
