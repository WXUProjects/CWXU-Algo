package dal

import (
	"context"
	data2 "cwxu-algo/app/common/data"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"
	"errors"
	"fmt"
	"strings"

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

// GetByName 按姓名或用户名模糊查询
func (d *ProfileDal) GetByName(ctx context.Context, name string) ([]*model.User, error) {
	var userList []*model.User
	kw := strings.TrimSpace(name)
	if kw == "" {
		return userList, nil
	}
	like := "%" + kw + "%"
	err := d.db.WithContext(ctx).
		Select("id, name, username").
		Where("name LIKE ? OR username LIKE ?", like, like).
		Limit(15).
		Find(&userList).Error
	if err != nil {
		return nil, err
	}
	return userList, nil
}

// RDB 供验证码等跨层使用
func (d *ProfileDal) RDB() *redis.Client {
	if d == nil {
		return nil
	}
	return d.rdb
}

// EmailTakenByOther 邮箱是否被其他用户占用
func (d *ProfileDal) EmailTakenByOther(ctx context.Context, email string, selfID uint) (bool, error) {
	email = strings.ToLower(strings.TrimSpace(email))
	if email == "" {
		return false, nil
	}
	var n int64
	err := d.db.WithContext(ctx).Model(&model.User{}).
		Where("LOWER(email) = ? AND id <> ?", email, selfID).
		Count(&n).Error
	return n > 0, err
}

// InvalidateProfileCache 删除资料缓存（组织内称呼等旁路更新后调用）
func (d *ProfileDal) InvalidateProfileCache(ctx context.Context, userID uint) {
	if d == nil || d.rdb == nil || userID == 0 {
		return
	}
	_ = d.rdb.Del(ctx, fmt.Sprintf("user:%d:profile", userID)).Err()
}

// UpdateAvatarEmail 更新头像；emailChanged 时同时写邮箱。不再改 name（昵称走组织内称呼）。
func (d *ProfileDal) UpdateAvatarEmail(ctx context.Context, profile model.User, emailChanged bool) error {
	cacheKey := fmt.Sprintf("user:%d:profile", profile.ID)
	return data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		updates := map[string]interface{}{
			"avatar": profile.Avatar,
		}
		if emailChanged {
			updates["email"] = strings.ToLower(strings.TrimSpace(profile.Email))
		}
		return d.db.WithContext(ctx).Model(&model.User{}).Where("id = ?", profile.ID).Updates(updates).Error
	})
}

// Update 兼容旧调用：头像+邮箱+昵称（管理端等）；新编辑资料走 UpdateAvatarEmail
func (d *ProfileDal) Update(ctx context.Context, profile model.User) error {
	cacheKey := fmt.Sprintf("user:%d:profile", profile.ID)
	err := data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		if err := d.db.WithContext(ctx).Model(&model.User{}).Where("id = ?", profile.ID).Updates(map[string]interface{}{
			"avatar": profile.Avatar,
			"email":  profile.Email,
			"name":   profile.Name,
		}).Error; err != nil {
			return err
		}
		name := strings.TrimSpace(profile.Name)
		if name == "" {
			return nil
		}
		var publicID uint
		if err := d.db.WithContext(ctx).Model(&model.Org{}).
			Select("id").Where("slug = ?", model.PublicOrgSlug).
			Scan(&publicID).Error; err != nil || publicID == 0 {
			return nil
		}
		_ = d.db.WithContext(ctx).Model(&model.OrgMember{}).
			Where("org_id = ? AND user_id = ?", publicID, profile.ID).
			Update("org_display_name", name).Error
		return nil
	})
	return err
}

// OrgDisplayNamesByUserIDs 批量取某组织内的组织内名称
func (d *ProfileDal) OrgDisplayNamesByUserIDs(ctx context.Context, orgID uint, userIDs []uint) (map[uint]string, error) {
	out := make(map[uint]string)
	if orgID == 0 || len(userIDs) == 0 {
		return out, nil
	}
	type row struct {
		UserID         uint
		OrgDisplayName string
	}
	var rows []row
	err := d.db.WithContext(ctx).Model(&model.OrgMember{}).
		Select("user_id, org_display_name").
		Where("org_id = ? AND user_id IN ?", orgID, userIDs).
		Find(&rows).Error
	if err != nil {
		return out, err
	}
	for _, r := range rows {
		out[r.UserID] = strings.TrimSpace(r.OrgDisplayName)
	}
	return out, nil
}

func (d *ProfileDal) GetList(ctx context.Context, pageSize, pageNum int64) ([]model.User, int64, error) {
	var list []model.User
	err := d.db.WithContext(ctx).
		Select("id", "username", "name", "group_id", "avatar", "role_id", "is_site_admin",
			"email_enabled", "email_weekly_enabled").
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

// ResolveDefaultGroupID 组织默认分组 id（无则创建）
func (d *ProfileDal) ResolveDefaultGroupID(ctx context.Context, orgID uint) (uint, error) {
	if orgID == 0 {
		return 0, fmt.Errorf("orgID 无效")
	}
	var g model.Group
	err := d.db.WithContext(ctx).
		Where("org_id = ? AND name IN ?", orgID, []string{model.DefaultGroupName, "未分组"}).
		Order("id ASC").
		First(&g).Error
	if err == nil {
		if g.Name != nil && *g.Name == "未分组" {
			n := model.DefaultGroupName
			_ = d.db.WithContext(ctx).Model(&g).Updates(map[string]interface{}{
				"name": n, "describe": model.DefaultGroupDesc,
			}).Error
		}
		return g.ID, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, err
	}
	n := model.DefaultGroupName
	g = model.Group{Name: &n, Describe: model.DefaultGroupDesc, OrgID: orgID}
	if err := d.db.WithContext(ctx).Create(&g).Error; err != nil {
		return 0, err
	}
	return g.ID, nil
}

// GetGroupNamesByIDs 批量查分组名（仅未删除）
func (d *ProfileDal) GetGroupNamesByIDs(ctx context.Context, groupIDs []int64) (map[int64]string, error) {
	out := make(map[int64]string)
	if len(groupIDs) == 0 {
		return out, nil
	}
	uniq := make([]int64, 0, len(groupIDs))
	seen := map[int64]struct{}{}
	for _, id := range groupIDs {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, id)
	}
	if len(uniq) == 0 {
		return out, nil
	}
	type row struct {
		ID   int64
		Name string
	}
	var rows []row
	err := d.db.WithContext(ctx).
		Table("groups").
		Select("id, name").
		Where("id IN ?", uniq).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	for _, r := range rows {
		name := r.Name
		if name == "未分组" {
			name = model.DefaultGroupName
		}
		out[r.ID] = name
	}
	return out, nil
}

// GetOrgBriefsByUserIDs 批量查询用户所属组织
func (d *ProfileDal) GetOrgBriefsByUserIDs(ctx context.Context, userIDs []uint) (map[uint][]OrgBrief, error) {
	out := make(map[uint][]OrgBrief)
	if len(userIDs) == 0 {
		return out, nil
	}
	type row struct {
		UserID   uint
		OrgID    uint
		Name     string
		Role     string
		IsSystem bool
	}
	var rows []row
	err := d.db.WithContext(ctx).
		Table("org_members AS m").
		Select("m.user_id AS user_id, m.org_id AS org_id, o.name AS name, m.role AS role, o.is_system AS is_system").
		Joins("JOIN orgs o ON o.id = m.org_id").
		Where("m.user_id IN ?", userIDs).
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

func (d *ProfileDal) MoveGroup(ctx context.Context, userID uint64, groupID int64, orgID uint) error {
	result := d.db.WithContext(ctx).Model(&model.OrgMember{}).
		Where("user_id = ? AND org_id = ?", userID, orgID).
		Update("group_id", groupID)
	if result.Error != nil {
		return fmt.Errorf("移动用户组失败: %w", result.Error)
	}
	if result.RowsAffected == 0 {
		return fmt.Errorf("用户不属于当前组织")
	}
	return nil
}

// GroupBelongsToOrg verifies the tenant boundary before assigning a member.
func (d *ProfileDal) GroupBelongsToOrg(ctx context.Context, groupID int64, orgID uint) bool {
	if groupID <= 0 || orgID == 0 {
		return false
	}
	var n int64
	_ = d.db.WithContext(ctx).Model(&model.Group{}).
		Where("id = ? AND org_id = ?", groupID, orgID).Count(&n).Error
	return n == 1
}

// SetEmailEnabled 设置用户日报邮件开关
func (d *ProfileDal) SetEmailEnabled(ctx context.Context, userId int64, enabled bool) error {
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	return data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		return d.db.Model(&model.User{}).Where("id = ?", userId).Update("email_enabled", enabled).Error
	})
}

// SetEmailWeeklyEnabled 设置用户周报邮件开关
func (d *ProfileDal) SetEmailWeeklyEnabled(ctx context.Context, userId int64, enabled bool) error {
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	return data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		return d.db.Model(&model.User{}).Where("id = ?", userId).Update("email_weekly_enabled", enabled).Error
	})
}

// GetEmailEnabled 获取用户日报邮件开关（失败默认关）
func (d *ProfileDal) GetEmailEnabled(ctx context.Context, userId int64) (bool, error) {
	var user model.User
	err := d.db.Select("email_enabled, email_weekly_enabled").Where("id = ?", userId).First(&user).Error
	if err != nil {
		return false, err
	}
	return user.EmailEnabled, nil
}

// UserHasOrgDailyEmailGrant 是否有任一组织授权日报邮件
func (d *ProfileDal) UserHasOrgDailyEmailGrant(ctx context.Context, userID int64) bool {
	var n int64
	_ = d.db.WithContext(ctx).Table("org_members AS m").
		Joins("JOIN orgs o ON o.id = m.org_id").
		Where("m.user_id = ? AND o.status = ? AND o.enable_ai_email = ?",
			userID, model.OrgStatusActive, true).
		Count(&n)
	return n > 0
}

// UserHasOrgWeeklyEmailGrant 是否在授权周报的组织中担任 staff 角色
func (d *ProfileDal) UserHasOrgWeeklyEmailGrant(ctx context.Context, userID int64) bool {
	var n int64
	_ = d.db.WithContext(ctx).Table("org_members AS m").
		Joins("JOIN orgs o ON o.id = m.org_id").
		Where(`m.user_id = ? AND o.status = ?
			AND o.enable_ai_weekly_email = ? AND m.role IN ?`,
			userID, model.OrgStatusActive, true,
			[]string{model.OrgRoleCoach, model.OrgRoleCaptain, model.OrgRoleOrgAdmin}).
		Count(&n)
	return n > 0
}

// StaffOrgIDsForWeekly 用户可收周报的组织（staff + 组织周报开）
func (d *ProfileDal) StaffOrgIDsForWeekly(ctx context.Context, userID int64) ([]uint, error) {
	var ids []uint
	err := d.db.WithContext(ctx).Table("org_members AS m").
		Joins("JOIN orgs o ON o.id = m.org_id").
		Where(`m.user_id = ? AND o.status = ?
			AND o.enable_ai_weekly_email = ? AND m.role IN ?`,
			userID, model.OrgStatusActive, true,
			[]string{model.OrgRoleCoach, model.OrgRoleCaptain, model.OrgRoleOrgAdmin}).
		Pluck("m.org_id", &ids).Error
	return ids, err
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

// GetNonPublicOrgUserIds 至少属于一个非公共域/非系统组织的用户
func (d *ProfileDal) GetNonPublicOrgUserIds(ctx context.Context) ([]int64, error) {
	var ids []int64
	err := d.db.WithContext(ctx).
		Table("org_members AS m").
		Joins("JOIN orgs o ON o.id = m.org_id").
		Where("o.slug <> ? AND COALESCE(o.is_system, false) = false", model.PublicOrgSlug).
		Distinct().
		Pluck("m.user_id", &ids).Error
	return ids, err
}

// GetProblemPipelineUserIds 题面爬取 / AI 资格用户：
// 默认=非公共域组织成员；个人 *bool 覆盖 true 强制开、false 强制关、null 跟默认。
func (d *ProfileDal) GetProblemPipelineUserIds(ctx context.Context) (fetchIDs, aiIDs []int64, err error) {
	orgIDs, err := d.GetNonPublicOrgUserIds(ctx)
	if err != nil {
		return nil, nil, err
	}
	orgSet := make(map[int64]struct{}, len(orgIDs))
	for _, id := range orgIDs {
		orgSet[id] = struct{}{}
	}

	type overrideRow struct {
		ID                  int64
		ProblemFetchEnabled *bool
		ProblemAIEnabled    *bool
	}
	var rows []overrideRow
	// 只拉有覆盖的用户 + 组织用户（组织用户也需读覆盖）
	// 简化：拉全部有覆盖的，再与 org 合并
	if err = d.db.WithContext(ctx).Model(&model.User{}).
		Select("id, problem_fetch_enabled, problem_ai_enabled").
		Where("problem_fetch_enabled IS NOT NULL OR problem_ai_enabled IS NOT NULL").
		Find(&rows).Error; err != nil {
		return nil, nil, err
	}
	fetchOff := map[int64]struct{}{}
	fetchOn := map[int64]struct{}{}
	aiOff := map[int64]struct{}{}
	aiOn := map[int64]struct{}{}
	for _, r := range rows {
		if r.ProblemFetchEnabled != nil {
			if *r.ProblemFetchEnabled {
				fetchOn[r.ID] = struct{}{}
			} else {
				fetchOff[r.ID] = struct{}{}
			}
		}
		if r.ProblemAIEnabled != nil {
			if *r.ProblemAIEnabled {
				aiOn[r.ID] = struct{}{}
			} else {
				aiOff[r.ID] = struct{}{}
			}
		}
	}

	fetchSet := make(map[int64]struct{}, len(orgIDs)+len(fetchOn))
	aiSet := make(map[int64]struct{}, len(orgIDs)+len(aiOn))
	for id := range orgSet {
		if _, off := fetchOff[id]; !off {
			fetchSet[id] = struct{}{}
		}
		if _, off := aiOff[id]; !off {
			aiSet[id] = struct{}{}
		}
	}
	for id := range fetchOn {
		fetchSet[id] = struct{}{}
	}
	for id := range aiOn {
		aiSet[id] = struct{}{}
	}
	fetchIDs = make([]int64, 0, len(fetchSet))
	for id := range fetchSet {
		fetchIDs = append(fetchIDs, id)
	}
	aiIDs = make([]int64, 0, len(aiSet))
	for id := range aiSet {
		aiIDs = append(aiIDs, id)
	}
	return fetchIDs, aiIDs, nil
}

// SetProblemPipeline 设置题面爬取/AI 覆盖（强制 true/false）
func (d *ProfileDal) SetProblemPipeline(ctx context.Context, userID int64, kind string, enabled bool) error {
	col := "problem_fetch_enabled"
	if kind == "ai" {
		col = "problem_ai_enabled"
	}
	cacheKey := fmt.Sprintf("user:%d:profile", userID)
	return data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		return d.db.WithContext(ctx).Model(&model.User{}).
			Where("id = ?", userID).
			Update(col, enabled).Error
	})
}

// EffectiveProblemPipeline 计算列表展示用有效开关（覆盖优先，否则是否非公共域组织）
func EffectiveProblemPipeline(override *bool, isNonPublicOrg bool) bool {
	if override != nil {
		return *override
	}
	return isNonPublicOrg
}

// UserSyncPolicy 一人多组织聚合后的定时策略
type UserSyncPolicy struct {
	UserID               int64
	EnableSpider         bool
	EnableAISummary      bool
	EnableAIEmail        bool // 组织授权日报（任一）
	EnableAIWeeklyEmail  bool // 组织授权周报且本人为 staff
	IsOrgStaff           bool // coach/captain/org_admin 任一
	EmailEnabled         bool // 个人日报偏好
	EmailWeeklyEnabled   bool // 个人周报偏好
	SpiderIntervalMin    int
	AISummaryIntervalMin int
}

// GetSyncPolicies 对每个用户：取其所属 active 组织，开关=任一开启，间隔=开启组织中的 MIN
func (d *ProfileDal) GetSyncPolicies(ctx context.Context, userIDs []int64) ([]UserSyncPolicy, error) {
	if len(userIDs) == 0 {
		return nil, nil
	}
	type row struct {
		UserID               int64
		Role                 string
		EnableSpider         bool
		EnableAISummary      bool
		EnableAIEmail        bool
		EnableAIWeeklyEmail  bool
		SpiderIntervalMin    int
		AISummaryIntervalMin int
	}
	var rows []row
	err := d.db.WithContext(ctx).
		Table("org_members AS m").
		Select(`m.user_id AS user_id, m.role AS role,
			o.enable_spider AS enable_spider,
			o.enable_ai_summary AS enable_ai_summary,
			o.enable_ai_email AS enable_ai_email,
			o.enable_ai_weekly_email AS enable_ai_weekly_email,
			o.spider_interval_min AS spider_interval_min,
			o.ai_summary_interval_min AS ai_summary_interval_min`).
		Joins("JOIN orgs o ON o.id = m.org_id").
		Where("m.user_id IN ? AND o.status = ?", userIDs, model.OrgStatusActive).
		Scan(&rows).Error
	if err != nil {
		return nil, err
	}

	type acc struct {
		spiderOn  bool
		aiOn      bool
		emailOn   bool
		weeklyOn  bool
		staff     bool
		spiderMin int
		aiMin     int
	}
	byUser := make(map[int64]*acc)
	for _, r := range rows {
		a := byUser[r.UserID]
		if a == nil {
			a = &acc{spiderMin: 0, aiMin: 0}
			byUser[r.UserID] = a
		}
		isStaff := r.Role == model.OrgRoleCoach || r.Role == model.OrgRoleCaptain || r.Role == model.OrgRoleOrgAdmin
		if isStaff {
			a.staff = true
		}
		if r.EnableSpider {
			a.spiderOn = true
			iv := r.SpiderIntervalMin
			if iv <= 0 {
				iv = 60
			}
			if a.spiderMin == 0 || iv < a.spiderMin {
				a.spiderMin = iv
			}
		}
		if r.EnableAISummary {
			a.aiOn = true
			iv := r.AISummaryIntervalMin
			if iv <= 0 {
				iv = 180
			}
			if a.aiMin == 0 || iv < a.aiMin {
				a.aiMin = iv
			}
		}
		if r.EnableAIEmail {
			a.emailOn = true
		}
		if r.EnableAIWeeklyEmail && isStaff {
			a.weeklyOn = true
		}
	}

	// 个人邮件偏好
	type pref struct {
		ID                 int64
		EmailEnabled       bool
		EmailWeeklyEnabled bool
	}
	var prefs []pref
	_ = d.db.WithContext(ctx).Model(&model.User{}).
		Select("id, email_enabled, email_weekly_enabled").
		Where("id IN ?", userIDs).
		Scan(&prefs).Error
	prefMap := make(map[int64]pref, len(prefs))
	for _, p := range prefs {
		prefMap[p.ID] = p
	}

	out := make([]UserSyncPolicy, 0, len(userIDs))
	for _, uid := range userIDs {
		a := byUser[uid]
		pr := prefMap[uid]
		if a == nil {
			out = append(out, UserSyncPolicy{
				UserID:             uid,
				EmailEnabled:       pr.EmailEnabled,
				EmailWeeklyEnabled: pr.EmailWeeklyEnabled,
			})
			continue
		}
		sp := 60
		if a.spiderMin > 0 {
			sp = a.spiderMin
		}
		ai := 180
		if a.aiMin > 0 {
			ai = a.aiMin
		}
		out = append(out, UserSyncPolicy{
			UserID:               uid,
			EnableSpider:         a.spiderOn,
			EnableAISummary:      a.aiOn,
			EnableAIEmail:        a.emailOn,
			EnableAIWeeklyEmail:  a.weeklyOn,
			IsOrgStaff:           a.staff,
			EmailEnabled:         pr.EmailEnabled,
			EmailWeeklyEnabled:   pr.EmailWeeklyEnabled,
			SpiderIntervalMin:    sp,
			AISummaryIntervalMin: ai,
		})
	}
	return out, nil
}

// GetListByOrg 分页列出组织成员用户
// total 与列表一致：仅统计仍存在于 users 表的成员（忽略孤儿 org_members）
func (d *ProfileDal) GetListByOrg(ctx context.Context, orgID uint, pageSize, pageNum int64) ([]model.User, int64, error) {
	var total int64
	if err := d.db.WithContext(ctx).
		Table("org_members AS m").
		Joins("JOIN users AS u ON u.id = m.user_id").
		Where("m.org_id = ?", orgID).
		Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []model.User
	err := d.db.WithContext(ctx).
		Table("users AS u").
		Select("u.id, u.username, u.name, COALESCE(m.group_id, 0) AS group_id, u.avatar, u.role_id, u.is_site_admin, u.email_enabled, u.email_weekly_enabled").
		Joins("JOIN org_members AS m ON m.user_id = u.id AND m.org_id = ?", orgID).
		Order("u.id").
		Limit(int(pageSize)).Offset(int(pageNum-1) * int(pageSize)).
		Find(&list).Error
	return list, total, err
}

// IsMemberOfOrg 用户是否为某组织成员
func (d *ProfileDal) IsMemberOfOrg(ctx context.Context, userID int64, orgID uint) bool {
	if userID <= 0 || orgID == 0 {
		return false
	}
	var n int64
	_ = d.db.WithContext(ctx).Model(&model.OrgMember{}).
		Where("user_id = ? AND org_id = ?", userID, orgID).
		Count(&n)
	return n > 0
}

// BatchEmailGrants 批量查询日报/周报组织授权（任一组织满足即 true）
func (d *ProfileDal) BatchEmailGrants(ctx context.Context, userIDs []int64) (daily map[int64]bool, weekly map[int64]bool) {
	daily = map[int64]bool{}
	weekly = map[int64]bool{}
	if len(userIDs) == 0 {
		return daily, weekly
	}
	var dailyIDs []int64
	_ = d.db.WithContext(ctx).Table("org_members AS m").
		Joins("JOIN orgs o ON o.id = m.org_id").
		Where("m.user_id IN ? AND o.status = ? AND o.enable_ai_email = ?",
			userIDs, model.OrgStatusActive, true).
		Distinct("m.user_id").
		Pluck("m.user_id", &dailyIDs)
	for _, id := range dailyIDs {
		daily[id] = true
	}
	var weeklyIDs []int64
	_ = d.db.WithContext(ctx).Table("org_members AS m").
		Joins("JOIN orgs o ON o.id = m.org_id").
		Where(`m.user_id IN ? AND o.status = ?
			AND o.enable_ai_weekly_email = ? AND m.role IN ?`,
			userIDs, model.OrgStatusActive, true,
			[]string{model.OrgRoleCoach, model.OrgRoleCaptain, model.OrgRoleOrgAdmin}).
		Distinct("m.user_id").
		Pluck("m.user_id", &weeklyIDs)
	for _, id := range weeklyIDs {
		weekly[id] = true
	}
	return daily, weekly
}

// GetUserIdsByGroup 根据组ID获取用户ID列表
func (d *ProfileDal) GetUserIdsByGroup(ctx context.Context, groupId int64) ([]int64, error) {
	var ids []int64
	err := d.db.WithContext(ctx).Model(&model.OrgMember{}).
		Where("group_id = ?", groupId).
		Pluck("user_id", &ids).Error
	return ids, err
}

func (d *ProfileDal) GroupIDForOrg(ctx context.Context, userID int64, orgID uint) int64 {
	if userID <= 0 || orgID == 0 {
		return 0
	}
	var row struct{ GroupID *uint }
	if err := d.db.WithContext(ctx).Model(&model.OrgMember{}).
		Select("group_id").Where("user_id = ? AND org_id = ?", userID, orgID).
		Scan(&row).Error; err != nil || row.GroupID == nil {
		return 0
	}
	return int64(*row.GroupID)
}

// UserProfile 用户简要信息（供批量查询用）
type UserProfile struct {
	ID       uint
	Name     string // 展示名（调用方按组织解析后写入）
	Username string
	Avatar   string
}

// GetByIds 批量获取用户简要信息（原始 users 字段，Name=全局昵称）
func (d *ProfileDal) GetByIds(ctx context.Context, userIds []int64) ([]UserProfile, error) {
	if len(userIds) == 0 {
		return nil, nil
	}
	var profiles []UserProfile
	err := d.db.WithContext(ctx).Model(&model.User{}).
		Select("id, name, username, avatar").
		Where("id IN ?", userIds).
		Find(&profiles).Error
	return profiles, err
}

// GetByIdsForOrg 批量展示名：组织内名称 → username（不用全局昵称）
func (d *ProfileDal) GetByIdsForOrg(ctx context.Context, orgID uint, userIds []int64) ([]UserProfile, error) {
	profiles, err := d.GetByIds(ctx, userIds)
	if err != nil || len(profiles) == 0 {
		return profiles, err
	}
	if orgID == 0 {
		if pub, e := d.PublicOrgID(ctx); e == nil {
			orgID = pub
		}
	}
	uids := make([]uint, 0, len(profiles))
	for _, p := range profiles {
		uids = append(uids, p.ID)
	}
	displayMap, _ := d.OrgDisplayNamesByUserIDs(ctx, orgID, uids)
	for i := range profiles {
		if dname := displayMap[profiles[i].ID]; dname != "" {
			profiles[i].Name = dname
		} else if profiles[i].Username != "" {
			profiles[i].Name = profiles[i].Username
		}
	}
	return profiles, nil
}

// SetRoleId 设置用户角色ID
func (d *ProfileDal) SetRoleId(ctx context.Context, userId int64, roleId int) error {
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	return data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		return d.db.Model(&model.User{}).Where("id = ?", userId).Update("role_id", roleId).Error
	})
}

// Delete 删除用户：清空本库关联数据后硬删除用户行，并清理 profile 缓存
func (d *ProfileDal) Delete(ctx context.Context, userId int64) error {
	cacheKey := fmt.Sprintf("user:%d:profile", userId)
	return data2.UpdateCacheDal(ctx, d.rdb, cacheKey, func() error {
		return d.db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			uid := uint(userId)
			if err := tx.Where("follower_id = ? OR followee_id = ?", uid, uid).
				Delete(&model.UserFollow{}).Error; err != nil {
				return err
			}
			if err := tx.Where("user_id = ?", uid).Delete(&model.OrgMember{}).Error; err != nil {
				return err
			}
			if err := tx.Where("user_id = ?", uid).Delete(&model.OrgJoinRequest{}).Error; err != nil {
				return err
			}
			if err := tx.Where("user_id = ?", uid).Delete(&model.Paste{}).Error; err != nil {
				return err
			}
			result := tx.Delete(&model.User{}, userId)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				return fmt.Errorf("用户不存在")
			}
			return nil
		})
	})
}
