package service

import (
	"context"
	"cwxu-algo/api/user/v1/site"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"
	"strings"

	"github.com/go-kratos/kratos/v2/errors"
)

type SiteService struct {
	site.UnimplementedSiteServer
	data *data.Data
}

func NewSiteService(d *data.Data) *SiteService {
	return &SiteService{data: d}
}

func (s *SiteService) ensureRow(ctx context.Context) (*model.SiteConfig, error) {
	var row model.SiteConfig
	err := s.data.DB.WithContext(ctx).First(&row, 1).Error
	if err == nil {
		return &row, nil
	}
	row = model.SiteConfig{
		ID:        1,
		SiteTitle: "GoAlgo",
	}
	if e := s.data.DB.WithContext(ctx).Create(&row).Error; e != nil {
		// 并发创建时再读
		if e2 := s.data.DB.WithContext(ctx).First(&row, 1).Error; e2 == nil {
			return &row, nil
		}
		return nil, e
	}
	return &row, nil
}

func (s *SiteService) GetConfig(ctx context.Context, _ *site.GetConfigReq) (*site.GetConfigRes, error) {
	row, err := s.ensureRow(ctx)
	if err != nil {
		return nil, errors.InternalServer("site config", err.Error())
	}
	return &site.GetConfigRes{
		Code:      0,
		Message:   "success",
		SiteTitle: row.SiteTitle,
		SiteLogo:  row.SiteLogo,
		Favicon:   row.Favicon,
	}, nil
}

func (s *SiteService) UpdateConfig(ctx context.Context, req *site.UpdateConfigReq) (*site.UpdateConfigRes, error) {
	if !auth.VerifySiteAdmin(ctx) {
		return &site.UpdateConfigRes{Code: 1, Message: "仅站点管理员可修改站点配置"}, nil
	}
	row, err := s.ensureRow(ctx)
	if err != nil {
		return nil, errors.InternalServer("site config", err.Error())
	}
	updates := map[string]interface{}{}
	if t := strings.TrimSpace(req.SiteTitle); t != "" {
		updates["site_title"] = t
		row.SiteTitle = t
	}
	// logo / favicon 允许清空
	if req.SiteLogo != "" || req.SiteTitle != "" || req.Favicon != "" {
		// 有任意字段时：logo/favicon 按请求写入（可空串清掉）
	}
	if req.SiteLogo != row.SiteLogo {
		updates["site_logo"] = strings.TrimSpace(req.SiteLogo)
		row.SiteLogo = strings.TrimSpace(req.SiteLogo)
	}
	if req.Favicon != row.Favicon {
		updates["favicon"] = strings.TrimSpace(req.Favicon)
		row.Favicon = strings.TrimSpace(req.Favicon)
	}
	if len(updates) > 0 {
		if e := s.data.DB.WithContext(ctx).Model(&model.SiteConfig{}).Where("id = ?", 1).Updates(updates).Error; e != nil {
			return nil, errors.InternalServer("site config update", e.Error())
		}
	}
	return &site.UpdateConfigRes{
		Code:      0,
		Message:   "success",
		SiteTitle: row.SiteTitle,
		SiteLogo:  row.SiteLogo,
		Favicon:   row.Favicon,
	}, nil
}
