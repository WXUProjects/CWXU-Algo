package dal

import (
	"context"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"
	"errors"
	"fmt"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

type GroupDal struct {
	db *gorm.DB
}

func NewGroupDal(data *data.Data) *GroupDal {
	return &GroupDal{db: data.DB}
}

func (d *GroupDal) Create(ctx context.Context, name, describe string) (int64, error) {
	group := model.Group{
		Name:     &name,
		Describe: describe,
	}
	if err := d.db.WithContext(ctx).Create(&group).Error; err != nil {
		return 0, fmt.Errorf("创建组失败: %w", err)
	}
	return int64(group.ID), nil
}

func (d *GroupDal) Delete(ctx context.Context, id int64) error {
	result := d.db.WithContext(ctx).Delete(&model.Group{}, id)
	if result.Error != nil {
		return fmt.Errorf("删除组失败: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("组不存在")
	}
	if err := d.db.WithContext(ctx).Model(&model.User{}).Where("group_id = ?", id).Update("group_id", 0).Error; err != nil {
		return fmt.Errorf("重置用户组ID失败: %w", err)
	}
	return nil
}

func (d *GroupDal) Get(ctx context.Context, id int64) (*model.Group, error) {
	var group model.Group
	err := d.db.WithContext(ctx).First(&group, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("组不存在")
	}
	if err != nil {
		return nil, fmt.Errorf("查询组失败: %w", err)
	}
	return &group, nil
}

func (d *GroupDal) GetWithUsers(ctx context.Context, id int64) (*model.Group, []model.User, error) {
	var group model.Group
	err := d.db.WithContext(ctx).Preload("Users").First(&group, id).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil, fmt.Errorf("组不存在")
	}
	if err != nil {
		return nil, nil, fmt.Errorf("查询组失败: %w", err)
	}
	if id == 0 {
		_ = d.db.WithContext(ctx).Model(&model.User{}).Where("group_id = ?", id).Find(&group.Users)
		return &group, group.Users, nil
	}
	log.Info("group: %v", group)
	return &group, group.Users, nil
}

func (d *GroupDal) List(ctx context.Context, page, size int64) ([]model.Group, int64, error) {
	var list []model.Group
	var total int64

	if err := d.db.WithContext(ctx).Model(&model.Group{}).Count(&total).Error; err != nil {
		return nil, 0, fmt.Errorf("查询组总数失败: %w", err)
	}

	offset := (page - 1) * size
	if err := d.db.WithContext(ctx).
		Order("id DESC").
		Limit(int(size)).
		Offset(int(offset)).
		Find(&list).Error; err != nil {
		return nil, 0, fmt.Errorf("查询组列表失败: %w", err)
	}

	return list, total, nil
}

func (d *GroupDal) Update(ctx context.Context, id int64, name, describe string) error {
	updates := map[string]interface{}{}
	if name != "" {
		updates["name"] = name
	}
	if describe != "" {
		updates["describe"] = describe
	}

	result := d.db.WithContext(ctx).Model(&model.Group{}).Where("id = ?", id).Updates(updates)
	if result.Error != nil {
		return fmt.Errorf("更新组失败: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("组不存在")
	}
	return nil
}
