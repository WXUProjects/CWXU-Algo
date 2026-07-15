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

// GetByName 根据姓名模糊查询用户信息
func (d *ProfileDal) GetByName(ctx context.Context, name string) ([]*model.User, error) {
	var userList []*model.User
	err := d.db.Select("id, name").Where("name LIKE ?", "%"+name+"%").Limit(5).Find(&userList).Error
	if err != nil {
		return nil, err
	}
	return userList, nil
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
	err := d.db.WithContext(ctx).
		Select("id", "username", "name", "group_id", "avatar", "role_id", "is_site_admin").
		Order("id").
		Limit(int(pageSize)).Offset(int(pageNum-1) * int(pageSize)).
		Find(&list).Error
	if err != nil {
		return nil, 0, err
	}
	var total int64
	if err = d.db.WithContext(ctx).Model(&model.User{}).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// OrgBrief 用户所属组织摘要（列表 Badge）
type OrgBrief struct {
	OrgID uint
	Name  string
	Role  string
}

// GetOrgBriefsByUserIDs 批量查询用户所属组织
func (d *ProfileDal) GetOrgBriefsByUserIDs(ctx context.Context, userIDs []uint) (map[uint][]OrgBrief, error) {
	out := make(map[uint][]OrgBrief)
	if len(userIDs) == 0 {
		return out, nil
	}
	type row struct {
		UserID  uint
		OrgID   uint
		Name    string
		Role    string
		IsSystem bool
	}
	var rows []row
	err := d.db.WithContext(ctx).
		Table("org_members AS m").
		Select("m.user_id AS user_id, m.org_id AS org_id, o.name AS name, m.role AS role, o.is_system AS is_system").
		Joins("JOIN orgs o ON o.id = m.org_id AND o.deleted_at IS NULL").
		Where("m.user_id IN ? AND m.deleted_at IS NULL", userIDs).
		Order("o.is_system DESC, o.id ASC").
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		out[r.UserID] = append(out[r.UserID], OrgBrief{
			OrgID: r.OrgID,
			Name:  r.Name,
			Role:  r.Role,
		})
	}
	return out, nil
}

func (d *ProfileDal) MoveGroup(ctx context.Context, userId uint64, groupId int64) error {
	result := d.db.WithContext(ctx).Model(&model.User{}).Where("id = ?", userId).Update("group_id", groupId)
	if result.Error != nil {
		return fmt.Errorf("移动用户组失败: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("用户不存在")
	}
	return nil
}

// SetEmailEnabled 设置用户邮件发送开关
func (d *ProfileDal) SetEmailEnabled(ctx context.Context, userId int64, enabled bool) error {
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	return data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		return d.db.Model(&model.User{}).Where("id = ?", userId).Update("email_enabled", enabled).Error
	})
}

// GetEmailEnabled 获取用户邮件发送开关
func (d *ProfileDal) GetEmailEnabled(ctx context.Context, userId int64) (bool, error) {
	var user model.User
	err := d.db.Select("email_enabled").Where("id = ?", userId).First(&user).Error
	if err != nil {
		return true, err
	}
	return user.EmailEnabled, nil
}

// PublicOrgID 公共域 id
func (d *ProfileDal) PublicOrgID(ctx context.Context) (uint, error) {
	var o model.Org
	if err := d.db.WithContext(ctx).Where("slug = ?", model.PublicOrgSlug).First(&o).Error; err != nil {
		return 0, err
	}
	return o.ID, nil
}

// GetUserIdsByOrg 组织成员 userId 列表
func (d *ProfileDal) GetUserIdsByOrg(ctx context.Context, orgID uint) ([]int64, error) {
	var ids []int64
	err := d.db.WithContext(ctx).Model(&model.OrgMember{}).
		Where("org_id = ?", orgID).
		Pluck("user_id", &ids).Error
	return ids, err
}

// GetListByOrg 分页列出组织成员用户
func (d *ProfileDal) GetListByOrg(ctx context.Context, orgID uint, pageSize, pageNum int64) ([]model.User, int64, error) {
	var total int64
	sub := d.db.WithContext(ctx).Model(&model.OrgMember{}).Select("user_id").Where("org_id = ?", orgID)
	if err := d.db.WithContext(ctx).Model(&model.User{}).Where("id IN (?)", sub).Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []model.User
	err := d.db.WithContext(ctx).
		Select("id", "username", "name", "group_id", "avatar", "role_id", "is_site_admin").
		Where("id IN (?)", sub).
		Order("id").
		Limit(int(pageSize)).Offset(int(pageNum-1)*int(pageSize)).
		Find(&list).Error
	return list, total, err
}

// GetUserIdsByGroup 根据组ID获取用户ID列表
func (d *ProfileDal) GetUserIdsByGroup(ctx context.Context, groupId int64) ([]int64, error) {
	var ids []int64
	err := d.db.Model(&model.User{}).
		Where("group_id = ?", groupId).
		Pluck("id", &ids).Error
	return ids, err
}

// UserProfile 用户简要信息（供批量查询用）
type UserProfile struct {
	ID     uint
	Name   string
	Avatar string
}

// GetByIds 批量获取用户简要信息
func (d *ProfileDal) GetByIds(ctx context.Context, userIds []int64) ([]UserProfile, error) {
	var profiles []UserProfile
	err := d.db.Model(&model.User{}).
		Select("id, name, avatar").
		Where("id IN ?", userIds).
		Find(&profiles).Error
	return profiles, err
}

// SetRoleId 设置用户角色ID
func (d *ProfileDal) SetRoleId(ctx context.Context, userId int64, roleId int) error {
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	return data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		return d.db.Model(&model.User{}).Where("id = ?", userId).Update("role_id", roleId).Error
	})
}

// Delete 软删除用户，并清理 profile 缓存
func (d *ProfileDal) Delete(ctx context.Context, userId int64) error {
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	return data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		result := d.db.WithContext(ctx).Delete(&model.User{}, userId)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return fmt.Errorf("用户不存在")
		}
		return nil
	})
}
