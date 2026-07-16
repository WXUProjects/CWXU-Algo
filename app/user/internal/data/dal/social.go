package dal

import (
	"context"
	"fmt"
	"strings"

	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

type SocialDal struct {
	db *gorm.DB
}

func NewSocialDal(data *data.Data) *SocialDal {
	return &SocialDal{db: data.DB}
}

// Follow 关注；不可关注自己
func (d *SocialDal) Follow(ctx context.Context, followerID, followeeID uint) error {
	if followerID == 0 || followeeID == 0 || followerID == followeeID {
		return fmt.Errorf("无效的关注目标")
	}
	var n int64
	if err := d.db.WithContext(ctx).Model(&model.User{}).Where("id = ?", followeeID).Count(&n).Error; err != nil {
		return err
	}
	if n == 0 {
		return fmt.Errorf("用户不存在")
	}
	f := model.UserFollow{FollowerID: followerID, FolloweeID: followeeID}
	return d.db.WithContext(ctx).Clauses(clause.OnConflict{DoNothing: true}).Create(&f).Error
}

// Unfollow 取消关注
func (d *SocialDal) Unfollow(ctx context.Context, followerID, followeeID uint) error {
	return d.db.WithContext(ctx).
		Where("follower_id = ? AND followee_id = ?", followerID, followeeID).
		Delete(&model.UserFollow{}).Error
}

// IsFollowing 是否已关注
func (d *SocialDal) IsFollowing(ctx context.Context, followerID, followeeID uint) bool {
	if followerID == 0 || followeeID == 0 {
		return false
	}
	var n int64
	_ = d.db.WithContext(ctx).Model(&model.UserFollow{}).
		Where("follower_id = ? AND followee_id = ?", followerID, followeeID).
		Count(&n).Error
	return n > 0
}

// Counts 关注数 / 粉丝数
func (d *SocialDal) Counts(ctx context.Context, userID uint) (following, followers int64, err error) {
	if userID == 0 {
		return 0, 0, nil
	}
	err = d.db.WithContext(ctx).Model(&model.UserFollow{}).
		Where("follower_id = ?", userID).Count(&following).Error
	if err != nil {
		return
	}
	err = d.db.WithContext(ctx).Model(&model.UserFollow{}).
		Where("followee_id = ?", userID).Count(&followers).Error
	return
}

// FollowingIDs 关注的 userId 列表（不分页，供过滤）
func (d *SocialDal) FollowingIDs(ctx context.Context, userID uint) ([]int64, error) {
	if userID == 0 {
		return []int64{}, nil
	}
	var ids []int64
	err := d.db.WithContext(ctx).Model(&model.UserFollow{}).
		Where("follower_id = ?", userID).
		Order("id DESC").
		Pluck("followee_id", &ids).Error
	if ids == nil {
		ids = []int64{}
	}
	return ids, err
}

// SocialUser 列表项
type SocialUser struct {
	UserID   uint
	Username string
	Name     string
	Avatar   string
}

// ListFollowing 关注列表
func (d *SocialDal) ListFollowing(ctx context.Context, userID uint, page, pageSize int) ([]SocialUser, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 50 {
		pageSize = 20
	}
	var total int64
	q := d.db.WithContext(ctx).Model(&model.UserFollow{}).Where("follower_id = ?", userID)
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var rows []SocialUser
	err := d.db.WithContext(ctx).Table("user_follows f").
		Select("u.id AS user_id, u.username, u.name, u.avatar").
		Joins("JOIN users u ON u.id = f.followee_id").
		Where("f.follower_id = ?", userID).
		Order("f.id DESC").
		Offset((page - 1) * pageSize).Limit(pageSize).
		Scan(&rows).Error
	if rows == nil {
		rows = []SocialUser{}
	}
	return rows, total, err
}

// ListFollowers 粉丝列表
func (d *SocialDal) ListFollowers(ctx context.Context, userID uint, page, pageSize int) ([]SocialUser, int64, error) {
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 50 {
		pageSize = 20
	}
	var total int64
	q := d.db.WithContext(ctx).Model(&model.UserFollow{}).Where("followee_id = ?", userID)
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var rows []SocialUser
	err := d.db.WithContext(ctx).Table("user_follows f").
		Select("u.id AS user_id, u.username, u.name, u.avatar").
		Joins("JOIN users u ON u.id = f.follower_id").
		Where("f.followee_id = ?", userID).
		Order("f.id DESC").
		Offset((page - 1) * pageSize).Limit(pageSize).
		Scan(&rows).Error
	if rows == nil {
		rows = []SocialUser{}
	}
	return rows, total, err
}

// SearchUsers 搜索用户（用户名 / 昵称）。
// 公共域可见性：未配置隐私 或 allow_public_profile=true 的用户才出现在搜索结果中。
// （私人域成员互搜由调用方另做组织过滤时再放宽；当前全站搜索遵循公共域规则。）
func (d *SocialDal) SearchUsers(ctx context.Context, keyword string, page, pageSize int) ([]SocialUser, int64, error) {
	kw := strings.TrimSpace(keyword)
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 50 {
		pageSize = 20
	}
	// 空关键词不扫全表，避免枚举
	if kw == "" {
		return []SocialUser{}, 0, nil
	}
	q := d.db.WithContext(ctx).Model(&model.User{}).
		Where("(privacy_configured = false OR allow_public_profile = true)").
		Where("username ILIKE ? OR name ILIKE ?", "%"+kw+"%", "%"+kw+"%")
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var rows []SocialUser
	err := q.Select("id AS user_id, username, name, avatar").
		Order("id ASC").
		Offset((page - 1) * pageSize).Limit(pageSize).
		Scan(&rows).Error
	if rows == nil {
		rows = []SocialUser{}
	}
	return rows, total, err
}

// GetPrivacy 读取隐私字段
func (d *SocialDal) GetPrivacy(ctx context.Context, userID uint) (configured, allowProfile, allowFeed bool, err error) {
	var u model.User
	err = d.db.WithContext(ctx).Select(
		"privacy_configured", "allow_public_profile", "allow_public_feed",
	).First(&u, userID).Error
	if err != nil {
		return false, true, true, err
	}
	// GORM default:true 对已有行可能为 false 零值；未配置时按产品默认 true
	if !u.PrivacyConfigured {
		return false, true, true, nil
	}
	return true, u.AllowPublicProfile, u.AllowPublicFeed, nil
}

// UpdatePrivacy 保存隐私并标记已配置
func (d *SocialDal) UpdatePrivacy(ctx context.Context, userID uint, allowProfile, allowFeed bool) error {
	return d.db.WithContext(ctx).Model(&model.User{}).Where("id = ?", userID).Updates(map[string]interface{}{
		"privacy_configured":   true,
		"allow_public_profile": allowProfile,
		"allow_public_feed":    allowFeed,
	}).Error
}

// FilterPublicFeedUserIDs 保留允许出现在公共域动态的用户（未配置=默认允许）
func (d *SocialDal) FilterPublicFeedUserIDs(ctx context.Context, userIDs []int64) ([]int64, error) {
	if len(userIDs) == 0 {
		return []int64{}, nil
	}
	type row struct {
		ID                 uint
		PrivacyConfigured  bool
		AllowPublicFeed    bool
	}
	var rows []row
	err := d.db.WithContext(ctx).Model(&model.User{}).
		Select("id, privacy_configured, allow_public_feed").
		Where("id IN ?", userIDs).
		Find(&rows).Error
	if err != nil {
		return nil, err
	}
	out := make([]int64, 0, len(rows))
	for _, r := range rows {
		if !r.PrivacyConfigured || r.AllowPublicFeed {
			out = append(out, int64(r.ID))
		}
	}
	return out, nil
}

// CanViewPublicProfile 公共域下是否允许查看资料（未配置=默认允许）
func (d *SocialDal) CanViewPublicProfile(ctx context.Context, userID uint) (bool, error) {
	configured, allowProfile, _, err := d.GetPrivacy(ctx, userID)
	if err != nil {
		return false, err
	}
	if !configured {
		return true, nil
	}
	return allowProfile, nil
}

// GetByUsername 精确用户名
func (d *SocialDal) GetByUsername(ctx context.Context, username string) (*model.User, error) {
	username = strings.TrimSpace(username)
	if username == "" {
		return nil, gorm.ErrRecordNotFound
	}
	var u model.User
	err := d.db.WithContext(ctx).Where("username = ?", username).First(&u).Error
	if err != nil {
		return nil, err
	}
	return &u, nil
}

// IsPublicOrg 当前 org 是否公共域
func (d *SocialDal) IsPublicOrg(ctx context.Context, orgID uint) bool {
	if orgID == 0 {
		return true // 无组织上下文视作公共域
	}
	var o model.Org
	if err := d.db.WithContext(ctx).Select("id, slug, is_system").First(&o, orgID).Error; err != nil {
		return true
	}
	return o.IsSystem || o.Slug == model.PublicOrgSlug
}
