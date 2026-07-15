package service

import (
	"context"
	"cwxu-algo/api/user/v1/role"
	"cwxu-algo/app/common/permission"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/data/dal"

	"github.com/go-kratos/kratos/v2/errors"
)

type RoleService struct {
	role.UnimplementedRoleServer
	profileDal *dal.ProfileDal
}

// List 获取所有角色列表（所有人可访问）
func (r *RoleService) List(ctx context.Context, req *role.ListReq) (*role.ListRes, error) {
	return &role.ListRes{
		Roles: []*role.RoleInfo{
			{RoleId: int32(permission.RoleUser), Name: permission.RoleName[permission.RoleUser]},
			{RoleId: int32(permission.RoleAdmin), Name: permission.RoleName[permission.RoleAdmin]},
			{RoleId: int32(permission.RoleCoach), Name: permission.RoleName[permission.RoleCoach]},
			{RoleId: int32(permission.RoleCaptain), Name: permission.RoleName[permission.RoleCaptain]},
		},
	}, nil
}

// SetUserRole 设置用户角色
// 权限规则：仅管理员可操作；不能将用户提升到比自己更高或同级权限
func (r *RoleService) SetUserRole(ctx context.Context, req *role.SetUserRoleReq) (*role.SetUserRoleRes, error) {
	// 1. 仅管理员可设置角色（查数据库，不信任JWT）
	caller, err := r.profileDal.GetById(ctx, int64(auth.GetCurrentUserId(ctx)))
	if err != nil || caller.RoleID != permission.RoleAdmin {
		return nil, errors.Forbidden("权限不足", "仅管理员可设置用户角色")
	}
	// 2. 校验角色值合法性
	if !permission.IsValid(int(req.RoleId)) {
		return nil, errors.BadRequest("参数错误", "无效的角色ID")
	}
	// 3. 获取目标用户信息
	targetUser, err := r.profileDal.GetById(ctx, req.UserId)
	if err != nil {
		return nil, errors.InternalServer("内部错误", "用户不存在")
	}
	// 4. 禁止将自己降级（管理员不能把自己的角色改成非管理员）
	callerId := auth.GetCurrentUserId(ctx)
	if int64(callerId) == req.UserId && targetUser.RoleID == permission.RoleAdmin {
		return nil, errors.Forbidden("权限不足", "无法修改自己的管理员角色")
	}
	// 5. 不能将用户设置为管理员（已删除，管理员可授权）
	// if req.RoleId == permission.RoleAdmin {
	// 	return nil, errors.Forbidden("权限不足", "无法授予管理员角色")
	// }
	// 6. 执行更新
	err = r.profileDal.SetRoleId(ctx, req.UserId, int(req.RoleId))
	if err != nil {
		return nil, errors.InternalServer("内部错误", "更新角色失败")
	}
	return &role.SetUserRoleRes{
		Code:    0,
		Message: "设置成功",
	}, nil
}

func NewRoleService(profileDal *dal.ProfileDal) *RoleService {
	return &RoleService{profileDal: profileDal}
}
