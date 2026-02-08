package service

import (
	"context"
	"cwxu-algo/api/core/v1/submit_log"
	"cwxu-algo/api/user/v1/group"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/biz"
	"cwxu-algo/app/user/internal/data/dal"
	"strconv"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	grpc2 "google.golang.org/grpc"
)

type GroupService struct {
	group.UnimplementedGroupServer
	reg          *discovery.Register
	groupUseCase *biz.GroupUseCase
	groupDal     *dal.GroupDal
}

func (g *GroupService) coreDataRPC() (*grpc2.ClientConn, error) {
	return grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///core-data"),
		grpc.WithDiscovery(g.reg.Reg.(registry.Discovery)),
		grpc.WithTimeout(20*time.Second),
	)
}

func (g *GroupService) Create(ctx context.Context, request *group.CreateRequest) (*group.CreateReply, error) {
	if !auth.VerifyAdmin(ctx) {
		return nil, errors.Forbidden("权限不足", "需要管理员权限操作")
	}
	if request.Name == "" {
		return nil, errors.BadRequest("参数错误", "组名称不能为空")
	}
	id, err := g.groupUseCase.Create(ctx, request.Name, request.Describe)
	if err != nil {
		return nil, errors.InternalServer("创建失败", err.Error())
	}
	return &group.CreateReply{
		Id:      id,
		Message: "创建成功",
	}, nil
}

func (g *GroupService) Delete(ctx context.Context, request *group.DeleteRequest) (*group.DeleteReply, error) {
	if !auth.VerifyAdmin(ctx) {
		return nil, errors.Forbidden("权限不足", "需要管理员权限操作")
	}
	if request.Id == 0 {
		return nil, errors.BadRequest("参数错误", "组ID不能为空")
	}
	err := g.groupUseCase.Delete(ctx, request.Id)
	if err != nil {
		return nil, errors.InternalServer("删除失败", err.Error())
	}
	return &group.DeleteReply{Success: true}, nil
}

func (g *GroupService) Get(ctx context.Context, request *group.GetRequest) (*group.GetReply, error) {
	groupModel, users, err := g.groupUseCase.GetWithUsers(ctx, request.Id)
	if err != nil {
		return nil, errors.InternalServer("查询失败", err.Error())
	}

	name := ""
	if groupModel.Name != nil {
		name = *groupModel.Name
	}

	reply := &group.GetReply{
		Id:       int64(groupModel.ID),
		Name:     name,
		Describe: groupModel.Describe,
		Users:    make([]*group.User, 0),
	}

	if len(users) > 0 {
		userIds := make([]int64, 0, len(users))
		for _, u := range users {
			userIds = append(userIds, int64(u.ID))
		}

		conn, err := g.coreDataRPC()
		if err != nil {
			log.Info(err.Error())
		} else {
			defer conn.Close()
			sb := submit_log.NewSubmitClient(conn)
			sp, err := sb.LastSubmitTime(ctx, &submit_log.LastSubmitTimeReq{UserIds: userIds})
			if err == nil {
				var timeMap map[int64]int64
				if err := utils.GobDecoder(sp.TimeMap, &timeMap); err == nil {
					for _, u := range users {
						lastSubmit := ""
						if t, ok := timeMap[int64(u.ID)]; ok {
							lastSubmit = strconv.Itoa(int(t))
						}
						reply.Users = append(reply.Users, &group.User{
							UserId:     uint64(u.ID),
							Username:   u.Username,
							Name:       u.Name,
							GroupId:    u.GroupId,
							Avatar:     u.Avatar,
							LastSubmit: lastSubmit,
						})
					}
				}
			}
		}
	}

	return reply, nil
}

func (g *GroupService) List(ctx context.Context, request *group.ListRequest) (*group.ListReply, error) {
	page := request.Page
	if page < 1 {
		page = 1
	}
	size := request.Size
	if size < 1 {
		size = 10
	}
	list, total, err := g.groupUseCase.List(ctx, page, size)
	if err != nil {
		return nil, errors.InternalServer("查询失败", err.Error())
	}
	reply := &group.ListReply{List: make([]*group.GetReply, 0), Total: total}
	for _, g := range list {
		name := ""
		if g.Name != nil {
			name = *g.Name
		}
		reply.List = append(reply.List, &group.GetReply{
			Id:       int64(g.ID),
			Name:     name,
			Describe: g.Describe,
		})
	}
	return reply, nil
}

func (g *GroupService) Update(ctx context.Context, request *group.UpdateRequest) (*group.UpdateReply, error) {
	if !auth.VerifyAdmin(ctx) {
		return nil, errors.Forbidden("权限不足", "需要管理员权限操作")
	}
	if request.Id == 0 {
		return nil, errors.BadRequest("参数错误", "组ID不能为空")
	}
	if request.Name == "" && request.Describe == "" {
		return nil, errors.BadRequest("参数错误", "至少更新一个字段")
	}
	err := g.groupUseCase.Update(ctx, request.Id, request.Name, request.Describe)
	if err != nil {
		return nil, errors.InternalServer("更新失败", err.Error())
	}
	return &group.UpdateReply{Success: true}, nil
}

func NewGroupService(reg *discovery.Register, groupUseCase *biz.GroupUseCase, groupDal *dal.GroupDal) *GroupService {
	return &GroupService{
		reg:          reg,
		groupUseCase: groupUseCase,
		groupDal:     groupDal,
	}
}
