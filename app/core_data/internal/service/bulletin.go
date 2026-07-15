package service

import (
	"context"
	"cwxu-algo/api/core/v1/bulletin"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/errors"
	"gorm.io/gorm"
)

type BulletinService struct {
	bulletin.UnimplementedBulletinServer
	bulletinDal *dal.BulletinDal
}

func NewBulletinService(bulletinDal *dal.BulletinDal) *BulletinService {
	return &BulletinService{bulletinDal: bulletinDal}
}

// modelToProto 将 GORM 模型转换为 proto 消息
func (s *BulletinService) modelToProto(m *model.Bulletin) *bulletin.BulletinInfo {
	return &bulletin.BulletinInfo{
		Id:         m.ID,
		Title:      m.Title,
		Content:    m.Content,
		AuthorId:   m.AuthorID,
		AuthorName: m.AuthorName,
		IsPinned:   m.IsPinned,
		CreatedAt:  m.CreatedAt.Unix(),
		UpdatedAt:  m.UpdatedAt.Unix(),
	}
}

func canManageBulletin(ctx context.Context, user *auth.JwtPayload, m *model.Bulletin) bool {
	if user == nil {
		return false
	}
	if auth.VerifySiteAdmin(ctx) {
		return true
	}
	// 组织公告：当前组织管理员且 org 匹配
	if m.Scope == model.BulletinScopeOrg && m.OrgID != nil {
		return auth.VerifyOrgAdmin(ctx) && user.OrgID == *m.OrgID
	}
	// 全站公告仅站点管理员
	return false
}

// Create 创建公告：站点管理员可发全站；组织管理员默认发当前组织公告
func (s *BulletinService) Create(ctx context.Context, req *bulletin.CreateBulletinReq) (*bulletin.CreateBulletinRes, error) {
	user := auth.GetCurrentUser(ctx)
	if user == nil {
		return &bulletin.CreateBulletinRes{Code: 1, Message: "未获取到用户信息"}, nil
	}

	scope := model.BulletinScopeOrg
	var orgID *uint
	if auth.VerifySiteAdmin(ctx) {
		// 站点管理员默认全站；若 JWT 有 org 且非仅站点场景，仍发全站（规格：全站仅站点管理员）
		scope = model.BulletinScopeSite
		orgID = nil
	} else if auth.VerifyOrgAdmin(ctx) && user.OrgID > 0 {
		scope = model.BulletinScopeOrg
		oid := user.OrgID
		orgID = &oid
	} else {
		return &bulletin.CreateBulletinRes{
			Code:    1,
			Message: "权限不足，仅站点管理员或团队管理员可发布公告",
		}, nil
	}

	if req.Title == "" {
		return &bulletin.CreateBulletinRes{Code: 2, Message: "标题不能为空"}, nil
	}
	if req.Content == "" {
		return &bulletin.CreateBulletinRes{Code: 3, Message: "内容不能为空"}, nil
	}

	m := &model.Bulletin{
		Title:      req.Title,
		Content:    req.Content,
		AuthorID:   int64(user.UserID),
		AuthorName: user.Name,
		IsPinned:   req.IsPinned,
		Scope:      scope,
		OrgID:      orgID,
	}
	if err := s.bulletinDal.Create(m); err != nil {
		return nil, errors.InternalServer("创建失败", err.Error())
	}

	return &bulletin.CreateBulletinRes{
		Code:    0,
		Message: "success",
		Data:    s.modelToProto(m),
	}, nil
}

// Update 更新公告
func (s *BulletinService) Update(ctx context.Context, req *bulletin.UpdateBulletinReq) (*bulletin.UpdateBulletinRes, error) {
	user := auth.GetCurrentUser(ctx)
	existing, err := s.bulletinDal.GetById(req.Id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return &bulletin.UpdateBulletinRes{Code: 2, Message: "公告不存在"}, nil
		}
		return nil, errors.InternalServer("查询失败", err.Error())
	}
	if !canManageBulletin(ctx, user, existing) {
		return &bulletin.UpdateBulletinRes{
			Code:    1,
			Message: "权限不足",
		}, nil
	}

	// 构建更新字段（管理员/教练均可改任意字段，不校验作者）
	updates := make(map[string]interface{})
	if req.Title != "" {
		updates["title"] = req.Title
	}
	if req.Content != "" {
		updates["content"] = req.Content
	}
	updates["is_pinned"] = req.IsPinned

	if len(updates) == 0 {
		return &bulletin.UpdateBulletinRes{
			Code:    3,
			Message: "无需更新的字段",
		}, nil
	}

	if err := s.bulletinDal.Update(req.Id, updates); err != nil {
		return nil, errors.InternalServer("更新失败", err.Error())
	}

	// 重新查询获取最新数据
	updated, err := s.bulletinDal.GetById(req.Id)
	if err != nil {
		return nil, errors.InternalServer("查询失败", err.Error())
	}

	return &bulletin.UpdateBulletinRes{
		Code:    0,
		Message: "success",
		Data:    s.modelToProto(updated),
	}, nil
}

// Delete 删除公告
func (s *BulletinService) Delete(ctx context.Context, req *bulletin.DeleteBulletinReq) (*bulletin.DeleteBulletinRes, error) {
	user := auth.GetCurrentUser(ctx)
	existing, err := s.bulletinDal.GetById(req.Id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return &bulletin.DeleteBulletinRes{Code: 2, Message: "公告不存在"}, nil
		}
		return nil, errors.InternalServer("查询失败", err.Error())
	}
	if !canManageBulletin(ctx, user, existing) {
		return &bulletin.DeleteBulletinRes{Code: 1, Message: "权限不足"}, nil
	}

	if err := s.bulletinDal.Delete(req.Id); err != nil {
		return nil, errors.InternalServer("删除失败", err.Error())
	}

	return &bulletin.DeleteBulletinRes{
		Code:    0,
		Message: "success",
	}, nil
}

// Get 获取公告详情（公开）
func (s *BulletinService) Get(ctx context.Context, req *bulletin.GetBulletinReq) (*bulletin.GetBulletinRes, error) {
	m, err := s.bulletinDal.GetById(req.Id)
	if err != nil {
		if err == gorm.ErrRecordNotFound {
			return &bulletin.GetBulletinRes{
				Code:    2,
				Message: "公告不存在",
			}, nil
		}
		return nil, errors.InternalServer("查询失败", err.Error())
	}

	return &bulletin.GetBulletinRes{
		Code:    0,
		Message: "success",
		Data:    s.modelToProto(m),
	}, nil
}

// List 分页获取公告列表（公开）
func (s *BulletinService) List(ctx context.Context, req *bulletin.ListBulletinReq) (*bulletin.ListBulletinRes, error) {
	page := req.Page
	if page < 1 {
		page = 1
	}
	pageSize := req.PageSize
	if pageSize < 1 {
		pageSize = 10
	}
	if pageSize > 50 {
		pageSize = 50
	}

	orgID := uint(0)
	if u := auth.GetCurrentUser(ctx); u != nil {
		orgID = u.OrgID
	}
	bulletins, total, err := s.bulletinDal.List(page, pageSize, orgID)
	if err != nil {
		return nil, errors.InternalServer("查询失败", err.Error())
	}

	data := make([]*bulletin.BulletinInfo, 0, len(bulletins))
	for i := range bulletins {
		data = append(data, s.modelToProto(&bulletins[i]))
	}

	return &bulletin.ListBulletinRes{
		Code:     0,
		Message:  "success",
		Data:     data,
		Total:    total,
		Page:     page,
		PageSize: pageSize,
	}, nil
}
