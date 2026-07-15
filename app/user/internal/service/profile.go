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
			UserId: int64(v.ID),
			Name:   v.Name,
		}
		res.List = append(res.List, t)
	}
	return res, nil
}

func (p *ProfileService) MoveGroup(ctx context.Context, req *profile.MoveGroupReq) (*profile.MoveGroupRes, error) {
	// 管理员 / 教练 / 队长
	if !auth.VerifyMinRole(ctx, permission.RoleCoach) {
		return nil, errors.Forbidden("权限不足", "需要教练、队长或管理员权限")
	}
	err := p.profileDal.MoveGroup(ctx, req.UserId, req.GroupId)
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
	// 非站点管理员：仅当前组织成员；站点管理员默认全站
	if !auth.VerifySiteAdmin(ctx) {
		orgID := uint(0)
		if pd := auth.GetCurrentUser(ctx); pd != nil {
			orgID = pd.OrgID
		}
		if orgID == 0 {
			orgID, _ = p.profileDal.PublicOrgID(ctx)
		}
		pf, total, err = p.profileDal.GetListByOrg(ctx, orgID, pageSize, pageNum)
	} else {
		pf, total, err = p.profileUseCase.GetList(ctx, pageSize, pageNum)
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

	res := &profile.GetListRes{
		List:  make([]*profile.GetListRes_List, 0),
		Total: total,
	}
	for _, v := range pf {
		var t string
		if v, ok := timeMap[int64(v.ID)]; ok {
			t = strconv.Itoa(int(v))
		}
		res.List = append(res.List, &profile.GetListRes_List{
			UserId:     uint64(v.ID),
			Username:   v.Username,
			Name:       v.Name,
			Avatar:     v.Avatar,
			GroupId:    v.GroupId,
			RoleId:     int32(v.RoleID),
			LastSubmit: t,
		})
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
	return &profile.GetByIdRes{
		UserId:       uint64(pf.ID),
		Username:     pf.Username,
		Name:         pf.Name,
		Email:        pf.Email,
		Avatar:       pf.Avatar,
		GroupId:      pf.GroupId,
		Spiders:      spiders,
		EmailEnabled: pf.EmailEnabled,
		RoleId:       int32(pf.RoleID),
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

// GetByIds 批量获取用户简要信息（供排行榜等场景使用）
func (p *ProfileService) GetByIds(ctx context.Context, req *profile.GetByIdsReq) (*profile.GetByIdsRes, error) {
	profiles, err := p.profileUseCase.GetByIds(ctx, req.UserIds)
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

// SetEmailEnabled 设置用户邮件发送开关
func (p *ProfileService) SetEmailEnabled(ctx context.Context, req *profile.SetEmailEnabledReq) (*profile.SetEmailEnabledRes, error) {
	if !auth.VerifySelfOrAbove(ctx, uint(req.UserId)) {
		return nil, UpdateForbidden
	}
	err := p.profileDal.SetEmailEnabled(ctx, req.UserId, req.Enabled)
	if err != nil {
		return nil, errors.InternalServer("内部错误", err.Error())
	}
	return &profile.SetEmailEnabledRes{
		Code:    0,
		Message: "设置成功",
	}, nil
}
