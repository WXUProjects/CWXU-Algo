package service

import (
	"context"
	"cwxu-algo/api/core/v1/spider"
	"cwxu-algo/api/core/v1/submit_log"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
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
	if !auth.VerifyAdmin(ctx) {
		return nil, errors.Forbidden("权限不足", "需要管理员权限操作")
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
	pf, total, err := p.profileUseCase.GetList(ctx, req.PageSize, req.PageNum)
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
			LastSubmit: t,
		})
	}
	return res, nil
}

func (p *ProfileService) Update(ctx context.Context, req *profile.UpdateReq) (*profile.UpdateRes, error) {
	// 校验JWT
	if !auth.VerifyById(ctx, uint(req.UserId)) {
		// 再次校验是不是管理员
		if !auth.VerifyAdmin(ctx) {
			return nil, UpdateForbidden
		}
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
		UserId:   uint64(pf.ID),
		Username: pf.Username,
		Name:     pf.Name,
		Email:    pf.Email,
		Avatar:   pf.Avatar,
		GroupId:  pf.GroupId,
		Spiders:  spiders,
	}, nil
}

func NewProfileService(profileDal *dal.ProfileDal, reg *discovery.Register, profileUseCase *biz.ProfileUseCase) *ProfileService {
	return &ProfileService{
		profileDal:     profileDal,
		reg:            reg,
		profileUseCase: profileUseCase,
	}
}
