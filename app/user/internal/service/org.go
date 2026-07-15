package service

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"gorm.io/gorm"
)

type OrgService struct {
	db *gorm.DB
}

func NewOrgService(d *data.Data) *OrgService {
	return &OrgService{db: d.DB}
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSON(r *http.Request, dst interface{}) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 1<<20))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, dst)
}

func newInviteCode() string {
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	return strings.ToUpper(hex.EncodeToString(b))
}

func orgToMap(o *model.Org, includeInvite bool) map[string]interface{} {
	m := map[string]interface{}{
		"id":                   o.ID,
		"name":                 o.Name,
		"slug":                 o.Slug,
		"plan":                 o.Plan,
		"status":               o.Status,
		"isSystem":             o.IsSystem,
		"brandTitle":           o.BrandTitle,
		"brandLogo":            o.BrandLogo,
		"brandFavicon":         o.BrandFavicon,
		"joinMode":             o.JoinMode,
		"enableAiSummary":      o.EnableAISummary,
		"enableAiEmail":        o.EnableAIEmail,
		"enableSpider":         o.EnableSpider,
		"spiderIntervalMin":    o.SpiderIntervalMin,
		"aiSummaryIntervalMin": o.AISummaryIntervalMin,
		"aiEmailSchedule":      o.AIEmailSchedule,
	}
	if includeInvite {
		m["inviteCode"] = o.InviteCode
	}
	return m
}

func (s *OrgService) loadUser(id uint) (*model.User, error) {
	var u model.User
	if err := s.db.First(&u, id).Error; err != nil {
		return nil, err
	}
	return &u, nil
}

func (s *OrgService) isOrgAdminDB(userID, orgID uint) bool {
	var m model.OrgMember
	if err := s.db.Where("org_id = ? AND user_id = ? AND role = ?", orgID, userID, model.OrgRoleOrgAdmin).First(&m).Error; err != nil {
		return false
	}
	return true
}

func (s *OrgService) isMemberDB(userID, orgID uint) bool {
	var n int64
	s.db.Model(&model.OrgMember{}).Where("org_id = ? AND user_id = ?", orgID, userID).Count(&n)
	return n > 0
}

// ensureDefaultGroupID 组织默认分组 ID（无则创建）
func (s *OrgService) ensureDefaultGroupID(orgID uint) uint {
	var g model.Group
	if s.db.Where("org_id = ? AND name IN ?", orgID, []string{model.DefaultGroupName, "未分组"}).
		Order("id ASC").First(&g).Error == nil {
		if g.Name != nil && *g.Name == "未分组" {
			n := model.DefaultGroupName
			_ = s.db.Model(&g).Updates(map[string]interface{}{
				"name": n, "describe": model.DefaultGroupDesc,
			}).Error
		}
		return g.ID
	}
	n := model.DefaultGroupName
	g = model.Group{Name: &n, Describe: model.DefaultGroupDesc, OrgID: orgID}
	if s.db.Create(&g).Error != nil {
		return 0
	}
	return g.ID
}

// RegisterOrgRoutes HTTP 路由（与 upload 同模式）
func RegisterOrgRoutes(srv *khttp.Server, org *OrgService) {
	r := srv.Route("/")
	r.GET("/v1/user/org/list", org.handleList)
	r.GET("/v1/user/org/get", org.handleGet)
	r.POST("/v1/user/org/create", org.handleCreate)
	r.POST("/v1/user/org/update", org.handleUpdate)
	r.POST("/v1/user/org/delete", org.handleDelete)
	r.POST("/v1/user/org/switch", org.handleSwitch)
	r.POST("/v1/user/org/join", org.handleJoin)
	r.POST("/v1/user/org/leave", org.handleLeave)
	r.GET("/v1/user/org/members", org.handleMembers)
	r.POST("/v1/user/org/members/set-role", org.handleSetRole)
	r.POST("/v1/user/org/members/remove", org.handleRemoveMember)
	r.POST("/v1/user/org/members/add", org.handleAddMember)
	r.GET("/v1/user/org/member-ids", org.handleMemberIds)
	r.GET("/v1/user/org/invite", org.handleInviteGet)
	r.POST("/v1/user/org/invite/rotate", org.handleInviteRotate)
	r.GET("/v1/user/org/join-requests", org.handleJoinRequests)
	r.POST("/v1/user/org/join-requests/review", org.handleJoinReview)
	r.POST("/v1/user/platform/set-site-admin", org.handleSetSiteAdmin)
}

func (s *OrgService) handleList(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	q := ctx.Request().URL.Query()
	mine := q.Get("mine") != "0"

	var orgs []model.Org
	if pd.IsSiteAdmin && q.Get("all") == "1" {
		_ = s.db.Order("is_system DESC, id ASC").Find(&orgs).Error
	} else if mine {
		var mems []model.OrgMember
		_ = s.db.Where("user_id = ?", pd.UserID).Find(&mems).Error
		ids := make([]uint, 0, len(mems))
		roleMap := map[uint]string{}
		for _, m := range mems {
			ids = append(ids, m.OrgID)
			roleMap[m.OrgID] = m.Role
		}
		if len(ids) > 0 {
			_ = s.db.Where("id IN ?", ids).Order("is_system DESC, id ASC").Find(&orgs).Error
		}
		list := make([]map[string]interface{}, 0, len(orgs))
		for i := range orgs {
			item := orgToMap(&orgs[i], false)
			item["myRole"] = roleMap[orgs[i].ID]
			item["isCurrent"] = orgs[i].ID == pd.OrgID
			list = append(list, item)
		}
		writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "success", "list": list})
		return nil
	}

	list := make([]map[string]interface{}, 0, len(orgs))
	for i := range orgs {
		item := orgToMap(&orgs[i], pd.IsSiteAdmin)
		item["isCurrent"] = orgs[i].ID == pd.OrgID
		list = append(list, item)
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "success", "list": list})
	return nil
}

func (s *OrgService) handleGet(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	idStr := ctx.Request().URL.Query().Get("id")
	id64, _ := strconv.ParseUint(idStr, 10, 64)
	orgID := uint(id64)
	if orgID == 0 && pd != nil {
		orgID = pd.OrgID
	}
	if orgID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "缺少组织 id"})
		return nil
	}
	var o model.Org
	if err := s.db.First(&o, orgID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "组织不存在"})
		return nil
	}
	showInvite := pd != nil && (pd.IsSiteAdmin || s.isOrgAdminDB(pd.UserID, orgID))
	item := orgToMap(&o, showInvite)
	if pd != nil {
		var m model.OrgMember
		if s.db.Where("org_id = ? AND user_id = ?", orgID, pd.UserID).First(&m).Error == nil {
			item["myRole"] = m.Role
		}
		item["isCurrent"] = orgID == pd.OrgID
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "success", "data": item})
	return nil
}

func (s *OrgService) handleCreate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || !auth.VerifySiteAdmin(ctx) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "仅站点管理员可创建组织"})
		return nil
	}
	var req struct {
		Name           string `json:"name"`
		Slug           string `json:"slug"`
		AdminUserID    uint   `json:"adminUserId"`
		JoinMode       string `json:"joinMode"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	name := strings.TrimSpace(req.Name)
	if name == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "组织名称不能为空"})
		return nil
	}
	slug := strings.TrimSpace(req.Slug)
	if slug == "" {
		slug = "org-" + newInviteCode()
	}
	slug = strings.ToLower(slug)
	if slug == model.PublicOrgSlug {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "slug 保留给公共域"})
		return nil
	}
	joinMode := req.JoinMode
	if joinMode != model.OrgJoinReview {
		joinMode = model.OrgJoinAuto
	}
	o := model.Org{
		Name:                 name,
		Slug:                 slug,
		Plan:                 "team",
		Status:               model.OrgStatusActive,
		IsSystem:             false,
		JoinMode:             joinMode,
		InviteCode:           newInviteCode(),
		EnableAISummary:      true,
		EnableAIEmail:        true,
		EnableSpider:         true,
		SpiderIntervalMin:    60,
		AISummaryIntervalMin: 180,
		AIEmailSchedule:      "30 7 * * *",
	}
	if err := s.db.Create(&o).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "创建失败: " + err.Error()})
		return nil
	}
	// 新建组织自带「默认分组」
	defID := s.ensureDefaultGroupID(o.ID)
	adminUID := req.AdminUserID
	if adminUID == 0 {
		adminUID = pd.UserID
	}
	// 确保目标用户存在且加入组织为管理员，并挂到默认分组
	if u, err := s.loadUser(adminUID); err == nil {
		_ = u
		_ = s.db.Where("org_id = ? AND user_id = ?", o.ID, adminUID).Delete(&model.OrgMember{}).Error
		var gid *uint
		if defID > 0 {
			gid = &defID
			_ = s.db.Model(&model.User{}).Where("id = ?", adminUID).Update("group_id", defID).Error
		}
		_ = s.db.Create(&model.OrgMember{
			OrgID:    o.ID,
			UserID:   adminUID,
			Role:     model.OrgRoleOrgAdmin,
			GroupID:  gid,
			JoinedAt: time.Now(),
		}).Error
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "创建成功", "data": orgToMap(&o, true),
	})
	return nil
}

// handleDelete 站点管理员删除组织（软删除）；公共域不可删
func (s *OrgService) handleDelete(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || !auth.VerifySiteAdmin(ctx) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "仅站点管理员可删除组织"})
		return nil
	}
	var req struct {
		ID uint `json:"id"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil || req.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var o model.Org
	if s.db.First(&o, req.ID).Error != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "组织不存在"})
		return nil
	}
	if o.IsSystem || o.Slug == model.PublicOrgSlug {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "公共域不可删除"})
		return nil
	}

	var pub model.Org
	if s.db.Where("slug = ?", model.PublicOrgSlug).First(&pub).Error != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "公共域不存在，无法迁移用户"})
		return nil
	}

	err := s.db.Transaction(func(tx *gorm.DB) error {
		// 当前组织指向被删组织的用户 → 切回公共域
		if err := tx.Model(&model.User{}).
			Where("current_org_id = ?", o.ID).
			Update("current_org_id", pub.ID).Error; err != nil {
			return err
		}
		// 成员关系
		if err := tx.Where("org_id = ?", o.ID).Delete(&model.OrgMember{}).Error; err != nil {
			return err
		}
		// 加入申请
		if err := tx.Where("org_id = ?", o.ID).Delete(&model.OrgJoinRequest{}).Error; err != nil {
			return err
		}
		// 组织内分组
		if err := tx.Where("org_id = ?", o.ID).Delete(&model.Group{}).Error; err != nil {
			return err
		}
		// 组织本身（软删除）
		if err := tx.Delete(&o).Error; err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "删除失败: " + err.Error()})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已删除组织"})
	return nil
}

func (s *OrgService) handleUpdate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req struct {
		ID                   uint   `json:"id"`
		Name                 string `json:"name"`
		Status               string `json:"status"`
		BrandTitle           string `json:"brandTitle"`
		BrandLogo            string `json:"brandLogo"`
		BrandFavicon         string `json:"brandFavicon"`
		JoinMode             string `json:"joinMode"`
		EnableAISummary      *bool  `json:"enableAiSummary"`
		EnableAIEmail        *bool  `json:"enableAiEmail"`
		EnableSpider         *bool  `json:"enableSpider"`
		SpiderIntervalMin    *int   `json:"spiderIntervalMin"`
		AISummaryIntervalMin *int   `json:"aiSummaryIntervalMin"`
		AIEmailSchedule      string `json:"aiEmailSchedule"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil || req.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var o model.Org
	if err := s.db.First(&o, req.ID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "组织不存在"})
		return nil
	}
	siteAdmin := auth.VerifySiteAdmin(ctx)
	orgAdmin := siteAdmin || s.isOrgAdminDB(pd.UserID, req.ID)
	if !orgAdmin {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "权限不足"})
		return nil
	}

	updates := map[string]interface{}{}
	// 品牌：组织管理员可写（空串表示清空 logo）
	updates["brand_title"] = strings.TrimSpace(req.BrandTitle)
	updates["brand_logo"] = strings.TrimSpace(req.BrandLogo)
	updates["brand_favicon"] = strings.TrimSpace(req.BrandFavicon)

	if req.JoinMode == model.OrgJoinAuto || req.JoinMode == model.OrgJoinReview {
		updates["join_mode"] = req.JoinMode
	}
	if req.EnableAISummary != nil {
		updates["enable_ai_summary"] = *req.EnableAISummary
	}
	if req.EnableAIEmail != nil {
		updates["enable_ai_email"] = *req.EnableAIEmail
	}
	if req.EnableSpider != nil {
		updates["enable_spider"] = *req.EnableSpider
	}

	// 名称：公共域仅站点管理员可改
	if strings.TrimSpace(req.Name) != "" {
		if !o.IsSystem || siteAdmin {
			updates["name"] = strings.TrimSpace(req.Name)
		}
	}

	// 间隔 / 状态：仅站点管理员
	if siteAdmin {
		if req.Status == model.OrgStatusActive || req.Status == model.OrgStatusSuspended {
			if !o.IsSystem {
				updates["status"] = req.Status
			}
		}
		if req.SpiderIntervalMin != nil && *req.SpiderIntervalMin > 0 {
			updates["spider_interval_min"] = *req.SpiderIntervalMin
		}
		if req.AISummaryIntervalMin != nil && *req.AISummaryIntervalMin > 0 {
			updates["ai_summary_interval_min"] = *req.AISummaryIntervalMin
		}
		if strings.TrimSpace(req.AIEmailSchedule) != "" {
			updates["ai_email_schedule"] = strings.TrimSpace(req.AIEmailSchedule)
		}
	}

	if err := s.db.Model(&o).Updates(updates).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": err.Error()})
		return nil
	}
	_ = s.db.First(&o, req.ID)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success", "data": orgToMap(&o, siteAdmin || orgAdmin),
	})
	return nil
}

func (s *OrgService) handleSwitch(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req struct {
		OrgID uint `json:"orgId"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil || req.OrgID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	if !s.isMemberDB(pd.UserID, req.OrgID) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "你不是该组织成员"})
		return nil
	}
	u, err := s.loadUser(pd.UserID)
	if err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "用户不存在"})
		return nil
	}
	_ = s.db.Model(u).Update("current_org_id", req.OrgID).Error
	u.CurrentOrgID = req.OrgID
	token, err := IssueJWT(s.db, u)
	if err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "签发 token 失败"})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "已切换组织", "jwtToken": token, "orgId": req.OrgID,
	})
	return nil
}

func (s *OrgService) handleJoin(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req struct {
		InviteCode string `json:"inviteCode"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	code := strings.TrimSpace(strings.ToUpper(req.InviteCode))
	if code == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "请输入团队识别码"})
		return nil
	}
	var o model.Org
	if err := s.db.Where("UPPER(invite_code) = ? AND status = ?", code, model.OrgStatusActive).First(&o).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "团队识别码无效"})
		return nil
	}
	if s.isMemberDB(pd.UserID, o.ID) {
		writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "你已在该组织中", "data": orgToMap(&o, false)})
		return nil
	}
	if o.JoinMode == model.OrgJoinReview {
		var existing model.OrgJoinRequest
		err := s.db.Where("org_id = ? AND user_id = ? AND status = ?", o.ID, pd.UserID, model.JoinReqPending).First(&existing).Error
		if err == nil {
			writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "申请已提交，等待团队管理员审批"})
			return nil
		}
		_ = s.db.Create(&model.OrgJoinRequest{
			OrgID:    o.ID,
			UserID:   pd.UserID,
			Status:   model.JoinReqPending,
			CodeUsed: code,
		}).Error
		writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "申请已提交，等待团队管理员审批"})
		return nil
	}
	defID := s.ensureDefaultGroupID(o.ID)
	var gid *uint
	if defID > 0 {
		gid = &defID
		_ = s.db.Model(&model.User{}).Where("id = ?", pd.UserID).Update("group_id", defID).Error
	}
	_ = s.db.Create(&model.OrgMember{
		OrgID:    o.ID,
		UserID:   pd.UserID,
		Role:     model.OrgRoleMember,
		GroupID:  gid,
		JoinedAt: time.Now(),
	}).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "加入成功", "data": orgToMap(&o, false)})
	return nil
}

func (s *OrgService) handleLeave(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req struct {
		OrgID uint `json:"orgId"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil || req.OrgID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var o model.Org
	if err := s.db.First(&o, req.OrgID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "组织不存在"})
		return nil
	}
	if o.IsSystem || o.Slug == model.PublicOrgSlug {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "公共域不可退出"})
		return nil
	}
	if err := s.db.Where("org_id = ? AND user_id = ?", req.OrgID, pd.UserID).Delete(&model.OrgMember{}).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": err.Error()})
		return nil
	}
	// 若当前组织是离开的组织，切回公共域
	u, _ := s.loadUser(pd.UserID)
	token := ""
	if u != nil && u.CurrentOrgID == req.OrgID {
		var pub model.Org
		if s.db.Where("slug = ?", model.PublicOrgSlug).First(&pub).Error == nil {
			_ = s.db.Model(u).Update("current_org_id", pub.ID).Error
			u.CurrentOrgID = pub.ID
			token, _ = IssueJWT(s.db, u)
		}
	}
	resp := map[string]interface{}{"code": 0, "message": "已退出组织"}
	if token != "" {
		resp["jwtToken"] = token
	}
	writeJSON(ctx.Response(), 200, resp)
	return nil
}

func (s *OrgService) handleMembers(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	q := ctx.Request().URL.Query()
	id64, _ := strconv.ParseUint(q.Get("orgId"), 10, 64)
	orgID := uint(id64)
	if orgID == 0 {
		orgID = pd.OrgID
	}
	if !auth.VerifySiteAdmin(ctx) && !s.isMemberDB(pd.UserID, orgID) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "权限不足"})
		return nil
	}
	page, _ := strconv.Atoi(q.Get("page"))
	if page < 1 {
		page = 1
	}
	pageSize, _ := strconv.Atoi(q.Get("pageSize"))
	if pageSize < 1 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	keyword := strings.TrimSpace(q.Get("keyword"))

	type row struct {
		UserID   uint
		Username string
		Name     string
		Avatar   string
		Role     string
		GroupID  *uint
		JoinedAt time.Time
	}
	base := s.db.Table("org_members AS m").
		Select("m.user_id AS user_id, u.username AS username, u.name AS name, u.avatar AS avatar, m.role AS role, m.group_id AS group_id, m.joined_at AS joined_at").
		Joins("JOIN users u ON u.id = m.user_id AND u.deleted_at IS NULL").
		Where("m.org_id = ? AND m.deleted_at IS NULL", orgID)
	if keyword != "" {
		like := "%" + keyword + "%"
		base = base.Where("u.name LIKE ? OR u.username LIKE ?", like, like)
	}

	var total int64
	countQ := s.db.Table("org_members AS m").
		Joins("JOIN users u ON u.id = m.user_id AND u.deleted_at IS NULL").
		Where("m.org_id = ? AND m.deleted_at IS NULL", orgID)
	if keyword != "" {
		like := "%" + keyword + "%"
		countQ = countQ.Where("u.name LIKE ? OR u.username LIKE ?", like, like)
	}
	_ = countQ.Count(&total).Error

	var rows []row
	_ = base.Order("m.role DESC, m.id ASC").
		Offset((page - 1) * pageSize).
		Limit(pageSize).
		Scan(&rows).Error

	list := make([]map[string]interface{}, 0, len(rows))
	for _, r := range rows {
		list = append(list, map[string]interface{}{
			"userId":   r.UserID,
			"username": r.Username,
			"name":     r.Name,
			"avatar":   r.Avatar,
			"role":     r.Role,
			"groupId":  r.GroupID,
			"joinedAt": r.JoinedAt.Unix(),
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success", "list": list, "total": total, "page": page, "pageSize": pageSize,
	})
	return nil
}

func (s *OrgService) handleMemberIds(ctx khttp.Context) error {
	id64, _ := strconv.ParseUint(ctx.Request().URL.Query().Get("orgId"), 10, 64)
	orgID := uint(id64)
	pd := auth.GetCurrentUser(ctx)
	if orgID == 0 && pd != nil {
		orgID = pd.OrgID
	}
	if orgID == 0 {
		var pub model.Org
		if s.db.Where("slug = ?", model.PublicOrgSlug).First(&pub).Error == nil {
			orgID = pub.ID
		}
	}
	var ids []int64
	_ = s.db.Model(&model.OrgMember{}).Where("org_id = ?", orgID).Pluck("user_id", &ids)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success", "userIds": ids, "orgId": orgID,
	})
	return nil
}

// handleAddMember 站点管理员搜索加入：按 userId 或 username
func (s *OrgService) handleAddMember(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req struct {
		OrgID    uint   `json:"orgId"`
		UserID   uint   `json:"userId"`
		Username string `json:"username"`
		Role     string `json:"role"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil || req.OrgID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	// 站点管理员可操作任意 org；组织管理员仅本 org
	if !auth.VerifySiteAdmin(ctx) && !s.isOrgAdminDB(pd.UserID, req.OrgID) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "权限不足"})
		return nil
	}
	uid := req.UserID
	if uid == 0 && strings.TrimSpace(req.Username) != "" {
		var u model.User
		if s.db.Where("username = ?", strings.TrimSpace(req.Username)).First(&u).Error != nil {
			// 尝试按姓名模糊
			if s.db.Where("name LIKE ?", "%"+strings.TrimSpace(req.Username)+"%").First(&u).Error != nil {
				writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "用户不存在"})
				return nil
			}
		}
		uid = u.ID
	}
	if uid == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "请提供 userId 或 username"})
		return nil
	}
	role := req.Role
	if !model.ValidOrgRole(role) {
		role = model.OrgRoleMember
	}
	if s.isMemberDB(uid, req.OrgID) {
		writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "用户已在组织中", "userId": uid})
		return nil
	}
	defID := s.ensureDefaultGroupID(req.OrgID)
	var gid *uint
	if defID > 0 {
		gid = &defID
		_ = s.db.Model(&model.User{}).Where("id = ?", uid).Update("group_id", defID).Error
	}
	if err := s.db.Create(&model.OrgMember{
		OrgID: req.OrgID, UserID: uid, Role: role, GroupID: gid, JoinedAt: time.Now(),
	}).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": err.Error()})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已加入组织", "userId": uid})
	return nil
}

func (s *OrgService) handleSetRole(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req struct {
		OrgID  uint   `json:"orgId"`
		UserID uint   `json:"userId"`
		Role   string `json:"role"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil || req.OrgID == 0 || req.UserID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	if !model.ValidOrgRole(req.Role) {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "角色无效（member|coach|captain|org_admin）"})
		return nil
	}
	// 站点管理员可任命任意组织；组织管理员可任命本组织
	if !auth.VerifySiteAdmin(ctx) && !s.isOrgAdminDB(pd.UserID, req.OrgID) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "权限不足"})
		return nil
	}
	var m model.OrgMember
	if err := s.db.Where("org_id = ? AND user_id = ?", req.OrgID, req.UserID).First(&m).Error; err != nil {
		// 不在组织中则加入
		m = model.OrgMember{OrgID: req.OrgID, UserID: req.UserID, Role: req.Role, JoinedAt: time.Now()}
		if err := s.db.Create(&m).Error; err != nil {
			writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": err.Error()})
			return nil
		}
	} else {
		_ = s.db.Model(&m).Update("role", req.Role).Error
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已更新角色"})
	return nil
}

func (s *OrgService) handleRemoveMember(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req struct {
		OrgID  uint `json:"orgId"`
		UserID uint `json:"userId"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil || req.OrgID == 0 || req.UserID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var o model.Org
	if s.db.First(&o, req.OrgID).Error != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "组织不存在"})
		return nil
	}
	if o.IsSystem {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "不能将成员移出公共域"})
		return nil
	}
	if !auth.VerifySiteAdmin(ctx) && !s.isOrgAdminDB(pd.UserID, req.OrgID) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "权限不足"})
		return nil
	}
	_ = s.db.Where("org_id = ? AND user_id = ?", req.OrgID, req.UserID).Delete(&model.OrgMember{}).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已移除成员"})
	return nil
}

func (s *OrgService) handleInviteGet(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	id64, _ := strconv.ParseUint(ctx.Request().URL.Query().Get("orgId"), 10, 64)
	orgID := uint(id64)
	if orgID == 0 {
		orgID = pd.OrgID
	}
	if !auth.VerifySiteAdmin(ctx) && !s.isOrgAdminDB(pd.UserID, orgID) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "权限不足"})
		return nil
	}
	var o model.Org
	if s.db.First(&o, orgID).Error != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "组织不存在"})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success",
		"inviteCode": o.InviteCode, "joinMode": o.JoinMode, "orgId": o.ID,
	})
	return nil
}

func (s *OrgService) handleInviteRotate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req struct {
		OrgID uint `json:"orgId"`
	}
	_ = readJSON(ctx.Request(), &req)
	orgID := req.OrgID
	if orgID == 0 {
		orgID = pd.OrgID
	}
	if !auth.VerifySiteAdmin(ctx) && !s.isOrgAdminDB(pd.UserID, orgID) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "权限不足"})
		return nil
	}
	code := newInviteCode()
	if err := s.db.Model(&model.Org{}).Where("id = ?", orgID).Update("invite_code", code).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": err.Error()})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已更换团队识别码", "inviteCode": code})
	return nil
}

func (s *OrgService) handleJoinRequests(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	id64, _ := strconv.ParseUint(ctx.Request().URL.Query().Get("orgId"), 10, 64)
	orgID := uint(id64)
	if orgID == 0 {
		orgID = pd.OrgID
	}
	if !auth.VerifySiteAdmin(ctx) && !s.isOrgAdminDB(pd.UserID, orgID) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "权限不足"})
		return nil
	}
	var reqs []model.OrgJoinRequest
	_ = s.db.Where("org_id = ? AND status = ?", orgID, model.JoinReqPending).Order("id DESC").Find(&reqs).Error
	list := make([]map[string]interface{}, 0, len(reqs))
	for _, r := range reqs {
		var u model.User
		_ = s.db.First(&u, r.UserID)
		list = append(list, map[string]interface{}{
			"id": r.ID, "userId": r.UserID, "username": u.Username, "name": u.Name,
			"status": r.Status, "createdAt": r.CreatedAt.Unix(),
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "success", "list": list})
	return nil
}

func (s *OrgService) handleJoinReview(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req struct {
		ID     uint `json:"id"`
		Approve bool `json:"approve"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil || req.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var jr model.OrgJoinRequest
	if s.db.First(&jr, req.ID).Error != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "申请不存在"})
		return nil
	}
	if !auth.VerifySiteAdmin(ctx) && !s.isOrgAdminDB(pd.UserID, jr.OrgID) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "权限不足"})
		return nil
	}
	uid := pd.UserID
	if req.Approve {
		_ = s.db.Model(&jr).Updates(map[string]interface{}{
			"status": model.JoinReqApproved, "reviewed_by": uid,
		}).Error
		if !s.isMemberDB(jr.UserID, jr.OrgID) {
			defID := s.ensureDefaultGroupID(jr.OrgID)
			var gid *uint
			if defID > 0 {
				gid = &defID
				_ = s.db.Model(&model.User{}).Where("id = ?", jr.UserID).Update("group_id", defID).Error
			}
			_ = s.db.Create(&model.OrgMember{
				OrgID: jr.OrgID, UserID: jr.UserID, Role: model.OrgRoleMember, GroupID: gid, JoinedAt: time.Now(),
			}).Error
		}
		writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已通过"})
		return nil
	}
	_ = s.db.Model(&jr).Updates(map[string]interface{}{
		"status": model.JoinReqRejected, "reviewed_by": uid,
	}).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已拒绝"})
	return nil
}

func (s *OrgService) handleSetSiteAdmin(ctx khttp.Context) error {
	if !auth.VerifySiteAdmin(ctx) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "仅站点管理员可操作"})
		return nil
	}
	var req struct {
		UserID      uint `json:"userId"`
		IsSiteAdmin bool `json:"isSiteAdmin"`
	}
	if err := readJSON(ctx.Request(), &req); err != nil || req.UserID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	// 防止撤销最后一个站点管理员
	if !req.IsSiteAdmin {
		var n int64
		s.db.Model(&model.User{}).Where("is_site_admin = ?", true).Count(&n)
		var target model.User
		if s.db.First(&target, req.UserID).Error == nil && target.IsSiteAdmin && n <= 1 {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "不能撤销最后一位站点管理员"})
			return nil
		}
	}
	roleID := 0
	if req.IsSiteAdmin {
		roleID = 1
	}
	if err := s.db.Model(&model.User{}).Where("id = ?", req.UserID).Updates(map[string]interface{}{
		"is_site_admin": req.IsSiteAdmin,
		"role_id":       roleID,
	}).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": err.Error()})
		return nil
	}
	log.Infof("set site admin user=%d is=%v", req.UserID, req.IsSiteAdmin)
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已更新"})
	return nil
}
