package service

import (
	"context"
	"cwxu-algo/api/core/v1/spider"
	"cwxu-algo/api/core/v1/submit_log"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/permission"
	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/biz"
	"cwxu-algo/app/user/internal/data/dal"
	"cwxu-algo/app/user/internal/data/model"
	"strconv"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	grpc2 "google.golang.org/grpc"
	"gorm.io/gorm"
)

var (
	UpdateForbidden = errors.Forbidden("禁止访问", "您无权更新该用户资料")
	InternalServer  = errors.InternalServer("内部错误", "内部错误")
)

type ProfileService struct {
	profile.UnimplementedProfileServer
	reg            *discovery.Register
	profileDal     *dal.ProfileDal
	profileUseCase *biz.ProfileUseCase
}

func (p *ProfileService) GetByName(ctx context.Context, req *profile.GetByNameReq) (*profile.GetByNameRes, error) {
	userList, err := p.profileDal.GetByName(ctx, req.Name)
	if err != nil {
		return nil, errors.InternalServer("内部错误", "查询时出错")
	}
	res := &profile.GetByNameRes{List: make([]*profile.GetByNameRes_UserList, 0)}
	for _, v := range userList {
		t := &profile.GetByNameRes_UserList{
			UserId:   int64(v.ID),
			Name:     v.Name,
			Username: v.Username,
		}
		res.List = append(res.List, t)
	}
	return res, nil
}

func (p *ProfileService) MoveGroup(ctx context.Context, req *profile.MoveGroupReq) (*profile.MoveGroupRes, error) {
	// 组织 staff（教练/队长/团队管理员）或站点管理员
	if !auth.VerifyStaff(ctx) {
		return nil, errors.Forbidden("权限不足", "需要教练、队长、团队管理员或站点管理员权限")
	}
	gid := req.GroupId
	// groupId=0 表示归入当前组织默认分组（不再使用「未分组」）
	if gid <= 0 {
		orgID := uint(0)
		if pd := auth.GetCurrentUser(ctx); pd != nil {
			orgID = pd.OrgID
		}
		if orgID == 0 {
			if id, e := p.profileDal.PublicOrgID(ctx); e == nil {
				orgID = id
			}
		}
		if orgID > 0 {
			// 通过 groupDal 路径不便，直接写默认：由 seed/list 保证存在
			// 使用 profileDal 的 db 查默认分组
			if def, e := p.profileDal.ResolveDefaultGroupID(ctx, orgID); e == nil && def > 0 {
				gid = int64(def)
			}
		}
	}
	err := p.profileDal.MoveGroup(ctx, req.UserId, gid)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	return &profile.MoveGroupRes{
		Code:    0,
		Message: "移动成功",
	}, nil
}

func (p *ProfileService) coreDataRPC() (*grpc2.ClientConn, error) {
	return grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///core-data"),
		grpc.WithDiscovery(p.reg.Reg.(registry.Discovery)),
		grpc.WithTimeout(20*time.Second),
	)
}

func (p *ProfileService) GetList(ctx context.Context, req *profile.GetListReq) (*profile.GetListRes, error) {
	pageSize, pageNum := req.PageSize, req.PageNum
	if pageSize < 1 {
		pageSize = 20
	}
	if pageNum < 1 {
		pageNum = 1
	}
	var pf []model.User
	var total int64
	var err error
	// scope=org：当前组织；scope=site：全站（仅站管）；空：兼容（站管全站/否则组织）
	scope := req.Scope
	useSite := false
	if scope == "site" {
		if !auth.VerifySiteAdmin(ctx) {
			return nil, errors.Forbidden("权限不足", "仅站点管理员可查看全站用户")
		}
		useSite = true
	} else if scope == "org" {
		useSite = false
	} else {
		// 兼容旧客户端
		useSite = auth.VerifySiteAdmin(ctx)
	}
	if useSite {
		pf, total, err = p.profileUseCase.GetList(ctx, pageSize, pageNum)
	} else {
		orgID := uint(0)
		if pd := auth.GetCurrentUser(ctx); pd != nil {
			orgID = pd.OrgID
		}
		if orgID == 0 {
			orgID, _ = p.profileDal.PublicOrgID(ctx)
		}
		pf, total, err = p.profileDal.GetListByOrg(ctx, orgID, pageSize, pageNum)
	}
	if err != nil {
		return nil, InternalServer
	}
	ids := make([]int64, 0)
	for _, v := range pf {
		ids = append(ids, int64(v.ID))
	}
	// 获取 最后一次 提交时间
	conn, err := p.coreDataRPC()
	if err != nil {
		return nil, InternalServer
	}
	defer conn.Close()
	sb := submit_log.NewSubmitClient(conn)
	sp, err := sb.LastSubmitTime(ctx, &submit_log.LastSubmitTimeReq{UserIds: ids})
	if err != nil {
		log.Info(err.Error())
		return nil, InternalServer
	}

	var timeMap map[int64]int64
	err = utils.GobDecoder(sp.TimeMap, &timeMap)
	if err != nil {
		log.Info(err.Error())
		return nil, InternalServer
	}

	uids := make([]uint, 0, len(pf))
	gids := make([]int64, 0, len(pf))
	for _, v := range pf {
		uids = append(uids, v.ID)
		if v.GroupId > 0 {
			gids = append(gids, v.GroupId)
		}
	}
	orgMap, _ := p.profileDal.GetOrgBriefsByUserIDs(ctx, uids)
	groupNames, _ := p.profileDal.GetGroupNamesByIDs(ctx, gids)

	// 组织视图：列表名称优先组织内称呼
	displayByUID := map[uint]string{}
	if !useSite {
		orgID := uint(0)
		if pd := auth.GetCurrentUser(ctx); pd != nil {
			orgID = pd.OrgID
		}
		if orgID == 0 {
			orgID, _ = p.profileDal.PublicOrgID(ctx)
		}
		if m, e := p.profileDal.OrgDisplayNamesByUserIDs(ctx, orgID, uids); e == nil {
			displayByUID = m
		}
	}

	dailyGrant, weeklyGrant := p.profileDal.BatchEmailGrants(ctx, ids)

	res := &profile.GetListRes{
		List:  make([]*profile.GetListRes_List, 0),
		Total: total,
	}
	for _, v := range pf {
		var t string
		if ts, ok := timeMap[int64(v.ID)]; ok {
			t = strconv.Itoa(int(ts))
		}
		gName := ""
		if v.GroupId > 0 {
			if n, ok := groupNames[v.GroupId]; ok {
				gName = n
			} else {
				// 分组已删：显示默认分组，避免「分组11」
				gName = "默认分组"
			}
		} else {
			gName = "默认分组"
		}
		displayName := v.Name
		if !useSite {
			if d := displayByUID[v.ID]; d != "" {
				displayName = d
			} else if v.Username != "" {
				displayName = v.Username
			}
		}
		uid := int64(v.ID)
		item := &profile.GetListRes_List{
			UserId:                  uint64(v.ID),
			Username:                v.Username,
			Name:                    displayName,
			Avatar:                  v.Avatar,
			GroupId:                 v.GroupId,
			RoleId:                  int32(v.RoleID),
			LastSubmit:              t,
			IsSiteAdmin:             v.IsSiteAdmin,
			GroupName:               gName,
			EmailEnabled:            v.EmailEnabled,
			EmailWeeklyEnabled:      v.EmailWeeklyEnabled,
			EmailAllowedByOrg:       dailyGrant[uid],
			EmailWeeklyAllowedByOrg: weeklyGrant[uid],
		}
		if briefs := orgMap[v.ID]; len(briefs) > 0 {
			item.Orgs = make([]*profile.GetListRes_OrgBrief, 0, len(briefs))
			for _, b := range briefs {
				item.Orgs = append(item.Orgs, &profile.GetListRes_OrgBrief{
					OrgId: uint64(b.OrgID),
					Name:  b.Name,
					Role:  b.Role,
				})
			}
		}
		res.List = append(res.List, item)
	}
	return res, nil
}

func (p *ProfileService) Update(ctx context.Context, req *profile.UpdateReq) (*profile.UpdateRes, error) {
	// 校验 JWT：只能修改自己，或者管理员可以修改任何人
	if !auth.VerifySelfOrAbove(ctx, uint(req.UserId)) {
		return nil, UpdateForbidden
	}
	// 构建 User
	pro := model.User{
		Model:  gorm.Model{ID: uint(req.UserId)},
		Avatar: req.Avatar,
		Name:   req.Name,
		Email:  req.Email,
	}
	err := p.profileDal.Update(ctx, pro)
	if err == nil {
		res := &profile.UpdateRes{
			Code:    0,
			Message: "更新成功",
		}
		return res, nil
	}
	return nil, errors.InternalServer("内部错误", err.Error())
}

func (p *ProfileService) GetById(ctx context.Context, req *profile.GetByIdReq) (*profile.GetByIdRes, error) {
	pf, err := p.profileDal.GetById(ctx, req.UserId)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	// 获取 platform spider 信息
	conn, err := p.coreDataRPC()
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	defer conn.Close()
	s := spider.NewSpiderClient(conn)
	sp, err := s.GetSpider(ctx, &spider.GetSpiderReq{UserId: req.UserId})
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	spiders := make([]*profile.GetByIdRes_Spiders, 0)
	for _, v := range sp.Data {
		spiders = append(spiders, &profile.GetByIdRes_Spiders{
			Platform: v.Platform,
			Username: v.Username,
		})
	}
	dailyGrant := p.profileDal.UserHasOrgDailyEmailGrant(ctx, req.UserId)
	weeklyGrant := p.profileDal.UserHasOrgWeeklyEmailGrant(ctx, req.UserId)
	// 无组织授权时对外展示为关（并尽量写回，避免脏状态）
	emailOn := pf.EmailEnabled && dailyGrant
	weeklyOn := pf.EmailWeeklyEnabled && weeklyGrant
	if pf.EmailEnabled && !dailyGrant {
		_ = p.profileDal.SetEmailEnabled(ctx, req.UserId, false)
	}
	if pf.EmailWeeklyEnabled && !weeklyGrant {
		_ = p.profileDal.SetEmailWeeklyEnabled(ctx, req.UserId, false)
	}
	// 当前组织视图：展示组织内名称（空则 username）
	displayName := strings.TrimSpace(pf.Name)
	if pd := auth.GetCurrentUser(ctx); pd != nil && pd.OrgID > 0 {
		if m, e := p.profileDal.OrgDisplayNamesByUserIDs(ctx, pd.OrgID, []uint{pf.ID}); e == nil {
			if d := m[pf.ID]; d != "" {
				displayName = d
			} else if pf.Username != "" {
				displayName = pf.Username
			}
		}
	} else if displayName == "" {
		displayName = pf.Username
	}
	return &profile.GetByIdRes{
		UserId:                 uint64(pf.ID),
		Username:               pf.Username,
		Name:                   displayName,
		Email:                  pf.Email,
		Avatar:                 pf.Avatar,
		GroupId:                pf.GroupId,
		Spiders:                spiders,
		EmailEnabled:           emailOn,
		EmailWeeklyEnabled:     weeklyOn,
		EmailAllowedByOrg:      dailyGrant,
		EmailWeeklyAllowedByOrg: weeklyGrant,
		RoleId:                 int32(pf.RoleID),
	}, nil
}

func NewProfileService(profileDal *dal.ProfileDal, reg *discovery.Register, profileUseCase *biz.ProfileUseCase) *ProfileService {
	return &ProfileService{
		profileDal:     profileDal,
		reg:            reg,
		profileUseCase: profileUseCase,
	}
}

// GetUserIdsByGroup 根据组ID获取用户ID列表
func (p *ProfileService) GetUserIdsByGroup(ctx context.Context, req *profile.GetUserIdsByGroupReq) (*profile.GetUserIdsByGroupRes, error) {
	ids, err := p.profileUseCase.GetUserIdsByGroup(ctx, req.GroupId)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	return &profile.GetUserIdsByGroupRes{
		UserIds: ids,
	}, nil
}

// GetSyncPolicies 定时任务用：多组织 MIN 间隔 + 开关任一开启
func (p *ProfileService) GetSyncPolicies(ctx context.Context, req *profile.GetSyncPoliciesReq) (*profile.GetSyncPoliciesRes, error) {
	ids := req.UserIds
	if len(ids) == 0 {
		return &profile.GetSyncPoliciesRes{Policies: nil}, nil
	}
	// 去重
	seen := make(map[int64]struct{}, len(ids))
	uniq := make([]int64, 0, len(ids))
	for _, id := range ids {
		if id <= 0 {
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		uniq = append(uniq, id)
	}
	list, err := p.profileDal.GetSyncPolicies(ctx, uniq)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	res := &profile.GetSyncPoliciesRes{Policies: make([]*profile.UserSyncPolicy, 0, len(list))}
	for _, v := range list {
		res.Policies = append(res.Policies, &profile.UserSyncPolicy{
			UserId:               v.UserID,
			EnableSpider:         v.EnableSpider,
			EnableAiSummary:      v.EnableAISummary,
			EnableAiEmail:        v.EnableAIEmail,
			EnableAiWeeklyEmail:  v.EnableAIWeeklyEmail,
			IsOrgStaff:           v.IsOrgStaff,
			EmailEnabled:         v.EmailEnabled,
			EmailWeeklyEnabled:   v.EmailWeeklyEnabled,
			SpiderIntervalMin:    int32(v.SpiderIntervalMin),
			AiSummaryIntervalMin: int32(v.AISummaryIntervalMin),
		})
	}
	return res, nil
}

// GetUserIdsByOrg 组织成员 ID（数据隔离）
func (p *ProfileService) GetUserIdsByOrg(ctx context.Context, req *profile.GetUserIdsByOrgReq) (*profile.GetUserIdsByOrgRes, error) {
	orgID := uint(req.OrgId)
	if orgID == 0 {
		if pd := auth.GetCurrentUser(ctx); pd != nil && pd.OrgID > 0 {
			orgID = pd.OrgID
		}
	}
	if orgID == 0 {
		// 无 org 时回落公共域
		id, err := p.profileDal.PublicOrgID(ctx)
		if err == nil {
			orgID = id
		}
	}
	if orgID == 0 {
		return &profile.GetUserIdsByOrgRes{UserIds: []int64{}, OrgId: 0}, nil
	}
	ids, err := p.profileDal.GetUserIdsByOrg(ctx, orgID)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	return &profile.GetUserIdsByOrgRes{UserIds: ids, OrgId: int64(orgID)}, nil
}

// GetByIds 批量获取用户展示名（当前组织 / 指定 org 的组织内名称）
func (p *ProfileService) GetByIds(ctx context.Context, req *profile.GetByIdsReq) (*profile.GetByIdsRes, error) {
	orgID := uint(req.OrgId)
	if orgID == 0 {
		if pd := auth.GetCurrentUser(ctx); pd != nil && pd.OrgID > 0 {
			orgID = pd.OrgID
		}
	}
	if orgID == 0 {
		orgID, _ = p.profileDal.PublicOrgID(ctx)
	}
	profiles, err := p.profileDal.GetByIdsForOrg(ctx, orgID, req.UserIds)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	list := make([]*profile.GetByIdsRes_UserProfile, 0, len(profiles))
	for _, v := range profiles {
		list = append(list, &profile.GetByIdsRes_UserProfile{
			UserId: int64(v.ID),
			Name:   v.Name,
			Avatar: v.Avatar,
		})
	}
	return &profile.GetByIdsRes{Profiles: list}, nil
}

// Delete 管理员删除用户（软删除）
func (p *ProfileService) Delete(ctx context.Context, req *profile.DeleteReq) (*profile.DeleteRes, error) {
	if !auth.VerifyAdmin(ctx) {
		return nil, errors.Forbidden("权限不足", "仅管理员可删除用户")
	}
	if req.UserId <= 0 {
		return nil, errors.BadRequest("参数错误", "用户ID无效")
	}
	callerId := int64(auth.GetCurrentUserId(ctx))
	if callerId == req.UserId {
		return nil, errors.Forbidden("权限不足", "不能删除自己")
	}
	target, err := p.profileDal.GetById(ctx, req.UserId)
	if err != nil {
		return nil, errors.BadRequest("参数错误", "用户不存在")
	}
	// 禁止删除管理员账号
	if target.RoleID == permission.RoleAdmin {
		return nil, errors.Forbidden("权限不足", "不能删除管理员账号")
	}
	if err := p.profileDal.Delete(ctx, req.UserId); err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	return &profile.DeleteRes{
		Code:    0,
		Message: "删除成功",
	}, nil
}

// canManageEmailPrefs 本人、站点管理员，或当前组织 staff 管理本组织成员
func (p *ProfileService) canManageEmailPrefs(ctx context.Context, targetUserID int64) bool {
	if auth.VerifySelfOrAbove(ctx, uint(targetUserID)) {
		return true
	}
	if !auth.VerifyStaff(ctx) {
		return false
	}
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.OrgID == 0 {
		return false
	}
	return p.profileDal.IsMemberOfOrg(ctx, targetUserID, pd.OrgID)
}

// SetEmailEnabled 设置用户日报/周报邮件开关（kind=daily|weekly）
func (p *ProfileService) SetEmailEnabled(ctx context.Context, req *profile.SetEmailEnabledReq) (*profile.SetEmailEnabledRes, error) {
	if !p.canManageEmailPrefs(ctx, req.UserId) {
		return nil, UpdateForbidden
	}
	kind := req.Kind
	if kind == "" {
		kind = "daily"
	}
	if req.Enabled {
		if kind == "weekly" {
			if !p.profileDal.UserHasOrgWeeklyEmailGrant(ctx, req.UserId) {
				return &profile.SetEmailEnabledRes{
					Code:    1,
					Message: "无法开启周报：需在组织中为教练/队长/团队管理员，且组织已开通周报",
				}, nil
			}
		} else {
			if !p.profileDal.UserHasOrgDailyEmailGrant(ctx, req.UserId) {
				return &profile.SetEmailEnabledRes{
					Code:    1,
					Message: "无法开启日报：该用户所在组织未开通日报邮件权限",
				}, nil
			}
		}
	}
	var err error
	if kind == "weekly" {
		err = p.profileDal.SetEmailWeeklyEnabled(ctx, req.UserId, req.Enabled)
	} else {
		err = p.profileDal.SetEmailEnabled(ctx, req.UserId, req.Enabled)
	}
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	return &profile.SetEmailEnabledRes{
		Code:    0,
		Message: "设置成功",
	}, nil
}
