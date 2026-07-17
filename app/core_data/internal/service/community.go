package service

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/notify"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/userrpc"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"gorm.io/gorm"
)

const (
	maxCommentRunes  = 2000
	maxSolutionRunes = 100000
	maxSolutionTitle = 120
	maxExcerptRunes  = 120
)

// CommunityService 题目评论 / 用户题解 / 发现动态 / 资料近期
type CommunityService struct {
	db  *gorm.DB
	udb *gorm.DB // optional: algo_user for notifications
	reg *registry.Registrar
}

func NewCommunityService(d *data.Data, reg *discovery.Register) *CommunityService {
	var r *registry.Registrar
	if reg != nil {
		r = &reg.Reg
	}
	return &CommunityService{db: d.DB, udb: d.UserDB, reg: r}
}

// RegisterCommunityRoutes 注册社区相关路由
func RegisterCommunityRoutes(srv *khttp.Server, s *CommunityService) {
	r := srv.Route("/")
	// 评论（全站）
	r.GET("/v1/core/problem/comment/list", s.handleCommentList)
	r.POST("/v1/core/problem/comment/create", s.handleCommentCreate)
	r.POST("/v1/core/problem/comment/delete", s.handleCommentDelete)
	// 用户题解（全站）
	r.GET("/v1/core/problem/solution/list", s.handleSolutionList)
	r.GET("/v1/core/problem/solution/get", s.handleSolutionGet)
	r.POST("/v1/core/problem/solution/create", s.handleSolutionCreate)
	r.POST("/v1/core/problem/solution/update", s.handleSolutionUpdate)
	r.POST("/v1/core/problem/solution/delete", s.handleSolutionDelete)
	// 发现流（组织隔离）
	r.GET("/v1/core/activity/feed", s.handleActivityFeed)
	// 资料页近期
	r.GET("/v1/core/user/recent-comments", s.handleUserRecentComments)
	r.GET("/v1/core/user/recent-solutions", s.handleUserRecentSolutions)
}

// ---------- comments ----------

func (s *CommunityService) handleCommentList(ctx khttp.Context) error {
	pid := queryUint(ctx, "problemId")
	if pid == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "缺少题目"})
		return nil
	}
	page, pageSize := pageParams(ctx, 1, 20, 50)
	q := s.db.Model(&model.ProblemComment{}).Where("problem_id = ?", pid)
	var total int64
	_ = q.Count(&total).Error
	var list []model.ProblemComment
	_ = q.Order("id desc").Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error
	users := s.batchUsers(ctx, userIDsFromComments(list))
	items := make([]map[string]interface{}, 0, len(list))
	for _, c := range list {
		u := users[c.UserID]
		items = append(items, map[string]interface{}{
			"id":        c.ID,
			"problemId": c.ProblemID,
			"userId":    c.UserID,
			"username":  u.username,
			"name":      u.name,
			"avatar":    u.avatar,
			"content":   c.Content,
			"createdAt": c.CreatedAt.Unix(),
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "list": items, "total": total, "page": page, "pageSize": pageSize,
	})
	return nil
}

func (s *CommunityService) handleCommentCreate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		ProblemID uint   `json:"problemId"`
		Content   string `json:"content"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.ProblemID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	content := strings.TrimSpace(strings.ReplaceAll(req.Content, "\r\n", "\n"))
	if content == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "评论不能为空"})
		return nil
	}
	if utf8.RuneCountInString(content) > maxCommentRunes {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "评论过长"})
		return nil
	}
	if !s.problemExists(req.ProblemID) {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题目不存在"})
		return nil
	}
	row := model.ProblemComment{
		ProblemID: req.ProblemID,
		UserID:    pd.UserID,
		Content:   content,
	}
	if err := s.db.Create(&row).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "发布失败"})
		return nil
	}
	// 发现流（组织隔离）
	if pd.OrgID > 0 {
		_ = s.db.Create(&model.ActivityFeed{
			OrgID:     pd.OrgID,
			UserID:    pd.UserID,
			Type:      model.ActivityTypeComment,
			RefID:     row.ID,
			ProblemID: req.ProblemID,
			Title:     excerpt(content, maxExcerptRunes),
			Excerpt:   excerpt(content, maxExcerptRunes),
		}).Error
	}
	// @ 通知
	s.emitMentions(ctx, pd.UserID, pd.Username, content, "comment", row.ID, req.ProblemID)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "已发布",
		"data": map[string]interface{}{
			"id": row.ID, "problemId": row.ProblemID, "userId": row.UserID,
			"content": row.Content, "createdAt": row.CreatedAt.Unix(),
		},
	})
	return nil
}

func (s *CommunityService) handleCommentDelete(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		ID uint `json:"id"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	var row model.ProblemComment
	if s.db.First(&row, req.ID).Error != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "评论不存在"})
		return nil
	}
	if row.UserID != pd.UserID && !auth.VerifySiteAdmin(ctx) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "只能删除自己的评论"})
		return nil
	}
	_ = s.db.Delete(&row).Error
	_ = s.db.Where("type = ? AND ref_id = ?", model.ActivityTypeComment, row.ID).Delete(&model.ActivityFeed{}).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{"success": true, "message": "已删除"})
	return nil
}

// ---------- solutions ----------

func (s *CommunityService) handleSolutionList(ctx khttp.Context) error {
	pid := queryUint(ctx, "problemId")
	if pid == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "缺少题目"})
		return nil
	}
	page, pageSize := pageParams(ctx, 1, 20, 50)
	q := s.db.Model(&model.ProblemUserSolution{}).Where("problem_id = ?", pid)
	var total int64
	_ = q.Count(&total).Error
	var list []model.ProblemUserSolution
	_ = q.Order("id desc").Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error
	users := s.batchUsers(ctx, userIDsFromSolutions(list))
	items := make([]map[string]interface{}, 0, len(list))
	for _, sol := range list {
		u := users[sol.UserID]
		items = append(items, map[string]interface{}{
			"id":        sol.ID,
			"problemId": sol.ProblemID,
			"userId":    sol.UserID,
			"username":  u.username,
			"name":      u.name,
			"avatar":    u.avatar,
			"title":     sol.Title,
			// 列表不回全文，减轻体积
			"excerpt":   excerpt(sol.ContentMD, maxExcerptRunes),
			"createdAt": sol.CreatedAt.Unix(),
			"updatedAt": sol.UpdatedAt.Unix(),
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "list": items, "total": total, "page": page, "pageSize": pageSize,
	})
	return nil
}

func (s *CommunityService) handleSolutionGet(ctx khttp.Context) error {
	id := queryUint(ctx, "id")
	if id == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "缺少 id"})
		return nil
	}
	var sol model.ProblemUserSolution
	if s.db.First(&sol, id).Error != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题解不存在"})
		return nil
	}
	users := s.batchUsers(ctx, []uint{sol.UserID})
	u := users[sol.UserID]
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok",
		"data": map[string]interface{}{
			"id": sol.ID, "problemId": sol.ProblemID, "userId": sol.UserID,
			"username": u.username, "name": u.name, "avatar": u.avatar,
			"title": sol.Title, "contentMd": sol.ContentMD,
			"createdAt": sol.CreatedAt.Unix(), "updatedAt": sol.UpdatedAt.Unix(),
		},
	})
	return nil
}

func (s *CommunityService) handleSolutionCreate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		ProblemID uint   `json:"problemId"`
		Title     string `json:"title"`
		ContentMD string `json:"contentMd"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.ProblemID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	title := strings.TrimSpace(req.Title)
	content := strings.TrimSpace(strings.ReplaceAll(req.ContentMD, "\r\n", "\n"))
	if title == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "请填写标题"})
		return nil
	}
	if utf8.RuneCountInString(title) > maxSolutionTitle {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "标题过长"})
		return nil
	}
	if content == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "题解内容不能为空"})
		return nil
	}
	if utf8.RuneCountInString(content) > maxSolutionRunes {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "题解过长"})
		return nil
	}
	if !s.problemExists(req.ProblemID) {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题目不存在"})
		return nil
	}
	row := model.ProblemUserSolution{
		ProblemID: req.ProblemID,
		UserID:    pd.UserID,
		Title:     title,
		ContentMD: content,
	}
	if err := s.db.Create(&row).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "发布失败"})
		return nil
	}
	if pd.OrgID > 0 {
		_ = s.db.Create(&model.ActivityFeed{
			OrgID:     pd.OrgID,
			UserID:    pd.UserID,
			Type:      model.ActivityTypeSolution,
			RefID:     row.ID,
			ProblemID: req.ProblemID,
			Title:     title,
			Excerpt:   excerpt(content, maxExcerptRunes),
		}).Error
	}
	s.emitMentions(ctx, pd.UserID, pd.Username, title+"\n"+content, "solution", row.ID, req.ProblemID)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "已发布",
		"data": map[string]interface{}{
			"id": row.ID, "problemId": row.ProblemID, "userId": row.UserID,
			"title": row.Title, "contentMd": row.ContentMD, "createdAt": row.CreatedAt.Unix(),
		},
	})
	return nil
}

func (s *CommunityService) handleSolutionUpdate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		ID        uint   `json:"id"`
		Title     string `json:"title"`
		ContentMD string `json:"contentMd"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	var row model.ProblemUserSolution
	if s.db.First(&row, req.ID).Error != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题解不存在"})
		return nil
	}
	if row.UserID != pd.UserID && !auth.VerifySiteAdmin(ctx) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "只能编辑自己的题解"})
		return nil
	}
	title := strings.TrimSpace(req.Title)
	content := strings.TrimSpace(strings.ReplaceAll(req.ContentMD, "\r\n", "\n"))
	if title == "" || content == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "标题和内容不能为空"})
		return nil
	}
	if utf8.RuneCountInString(title) > maxSolutionTitle || utf8.RuneCountInString(content) > maxSolutionRunes {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "内容过长"})
		return nil
	}
	_ = s.db.Model(&row).Updates(map[string]interface{}{
		"title": title, "content_md": content,
	}).Error
	_ = s.db.Model(&model.ActivityFeed{}).
		Where("type = ? AND ref_id = ?", model.ActivityTypeSolution, row.ID).
		Updates(map[string]interface{}{
			"title": title, "excerpt": excerpt(content, maxExcerptRunes),
		}).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{"success": true, "message": "已更新"})
	return nil
}

func (s *CommunityService) handleSolutionDelete(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		ID uint `json:"id"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	var row model.ProblemUserSolution
	if s.db.First(&row, req.ID).Error != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题解不存在"})
		return nil
	}
	if row.UserID != pd.UserID && !auth.VerifySiteAdmin(ctx) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "只能删除自己的题解"})
		return nil
	}
	_ = s.db.Delete(&row).Error
	_ = s.db.Where("type = ? AND ref_id = ?", model.ActivityTypeSolution, row.ID).Delete(&model.ActivityFeed{}).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{"success": true, "message": "已删除"})
	return nil
}

// ---------- activity feed (org-scoped) ----------

func (s *CommunityService) handleActivityFeed(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	orgID := uint(0)
	if pd != nil {
		orgID = pd.OrgID
	}
	// 允许 query 覆盖仅站管；普通用户强制当前组织
	if q := queryUint(ctx, "orgId"); q > 0 && pd != nil && auth.VerifySiteAdmin(ctx) {
		orgID = q
	}
	if orgID == 0 {
		writeJSON(ctx.Response(), 200, map[string]interface{}{
			"success": true, "message": "ok", "list": []interface{}{}, "total": 0, "page": 1, "pageSize": 20,
		})
		return nil
	}
	page, pageSize := pageParams(ctx, 1, 20, 50)
	typ := strings.TrimSpace(ctx.Query().Get("type")) // comment|solution|空=全部
	q := s.db.Model(&model.ActivityFeed{}).Where("org_id = ?", orgID)
	if typ == model.ActivityTypeComment || typ == model.ActivityTypeSolution {
		q = q.Where("type = ?", typ)
	}
	var total int64
	_ = q.Count(&total).Error
	var list []model.ActivityFeed
	_ = q.Order("id desc").Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error

	uids := make([]uint, 0, len(list))
	pids := make([]uint, 0, len(list))
	seenU, seenP := map[uint]struct{}{}, map[uint]struct{}{}
	for _, a := range list {
		if _, ok := seenU[a.UserID]; !ok {
			seenU[a.UserID] = struct{}{}
			uids = append(uids, a.UserID)
		}
		if _, ok := seenP[a.ProblemID]; !ok {
			seenP[a.ProblemID] = struct{}{}
			pids = append(pids, a.ProblemID)
		}
	}
	users := s.batchUsers(ctx, uids)
	probs := s.batchProblems(pids)
	items := make([]map[string]interface{}, 0, len(list))
	for _, a := range list {
		u := users[a.UserID]
		p := probs[a.ProblemID]
		items = append(items, map[string]interface{}{
			"id":            a.ID,
			"orgId":         a.OrgID,
			"userId":        a.UserID,
			"username":      u.username,
			"name":          u.name,
			"avatar":        u.avatar,
			"type":          a.Type,
			"refId":         a.RefID,
			"problemId":     a.ProblemID,
			"problemTitle":  p.title,
			"platform":      p.platform,
			"title":         a.Title,
			"excerpt":       a.Excerpt,
			"createdAt":     a.CreatedAt.Unix(),
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "list": items, "total": total, "page": page, "pageSize": pageSize,
	})
	return nil
}

// ---------- profile recent ----------

func (s *CommunityService) handleUserRecentComments(ctx khttp.Context) error {
	uid := queryUint(ctx, "userId")
	if uid == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "缺少用户"})
		return nil
	}
	limit := queryInt(ctx, "limit", 10)
	if limit > 50 {
		limit = 50
	}
	var list []model.ProblemComment
	_ = s.db.Where("user_id = ?", uid).Order("id desc").Limit(limit).Find(&list).Error
	pids := make([]uint, 0, len(list))
	seen := map[uint]struct{}{}
	for _, c := range list {
		if _, ok := seen[c.ProblemID]; !ok {
			seen[c.ProblemID] = struct{}{}
			pids = append(pids, c.ProblemID)
		}
	}
	probs := s.batchProblems(pids)
	items := make([]map[string]interface{}, 0, len(list))
	for _, c := range list {
		p := probs[c.ProblemID]
		items = append(items, map[string]interface{}{
			"id": c.ID, "problemId": c.ProblemID, "problemTitle": p.title, "platform": p.platform,
			"content": c.Content, "createdAt": c.CreatedAt.Unix(),
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"success": true, "message": "ok", "list": items})
	return nil
}

func (s *CommunityService) handleUserRecentSolutions(ctx khttp.Context) error {
	uid := queryUint(ctx, "userId")
	if uid == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "缺少用户"})
		return nil
	}
	limit := queryInt(ctx, "limit", 10)
	if limit > 50 {
		limit = 50
	}
	var list []model.ProblemUserSolution
	_ = s.db.Where("user_id = ?", uid).Order("id desc").Limit(limit).Find(&list).Error
	pids := make([]uint, 0, len(list))
	seen := map[uint]struct{}{}
	for _, sol := range list {
		if _, ok := seen[sol.ProblemID]; !ok {
			seen[sol.ProblemID] = struct{}{}
			pids = append(pids, sol.ProblemID)
		}
	}
	probs := s.batchProblems(pids)
	items := make([]map[string]interface{}, 0, len(list))
	for _, sol := range list {
		p := probs[sol.ProblemID]
		items = append(items, map[string]interface{}{
			"id": sol.ID, "problemId": sol.ProblemID, "problemTitle": p.title, "platform": p.platform,
			"title": sol.Title, "excerpt": excerpt(sol.ContentMD, maxExcerptRunes),
			"createdAt": sol.CreatedAt.Unix(),
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"success": true, "message": "ok", "list": items})
	return nil
}

// ---------- helpers ----------

type userBrief struct {
	username, name, avatar string
}

type probBrief struct {
	title, platform string
}

func (s *CommunityService) problemExists(id uint) bool {
	var n int64
	_ = s.db.Model(&model.Problem{}).Where("id = ?", id).Count(&n).Error
	return n > 0
}

func (s *CommunityService) batchUsers(ctx khttp.Context, ids []uint) map[uint]userBrief {
	out := map[uint]userBrief{}
	if len(ids) == 0 || s.reg == nil {
		return out
	}
	intIDs := make([]int64, 0, len(ids))
	for _, id := range ids {
		intIDs = append(intIDs, int64(id))
	}
	client, err := userrpc.ProfileClient(s.reg)
	if err != nil {
		return out
	}
	var orgID int64
	if pd := auth.GetCurrentUser(ctx); pd != nil {
		orgID = int64(pd.OrgID)
	}
	res, err := client.GetByIds(context.Background(), &profile.GetByIdsReq{UserIds: intIDs, OrgId: orgID})
	if err != nil || res == nil {
		return out
	}
	for _, u := range res.Profiles {
		if u == nil {
			continue
		}
		out[uint(u.UserId)] = userBrief{
			username: u.Username,
			name:     u.Name,
			avatar:   u.Avatar,
		}
	}
	return out
}

func (s *CommunityService) batchProblems(ids []uint) map[uint]probBrief {
	out := map[uint]probBrief{}
	if len(ids) == 0 {
		return out
	}
	var list []model.Problem
	_ = s.db.Select("id", "title", "platform").Where("id IN ?", ids).Find(&list).Error
	for _, p := range list {
		out[p.ID] = probBrief{title: p.Title, platform: p.Platform}
	}
	return out
}

func (s *CommunityService) emitMentions(ctx khttp.Context, actorID uint, actorName, text, refType string, refID, problemID uint) {
	names := notify.ExtractMentions(text)
	if len(names) == 0 {
		return
	}
	// 解析 username → id
	resolved := s.resolveUsernames(ctx, names)
	rows := make([]notify.Row, 0, len(resolved))
	for uname, uid := range resolved {
		if uid == 0 || uid == actorID {
			continue
		}
		title := "有人提到了你"
		body := actorName + " 在"
		if refType == "solution" {
			body += "题解"
		} else {
			body += "评论"
		}
		body += "中 @ 了你"
		payload, _ := json.Marshal(map[string]interface{}{
			"username": uname, "actorName": actorName,
		})
		rows = append(rows, notify.Row{
			UserID:    uid,
			Type:      notify.TypeMention,
			Title:     title,
			Body:      body,
			ActorID:   actorID,
			RefType:   refType,
			RefID:     refID,
			ProblemID: problemID,
			Payload:   string(payload),
		})
	}
	if err := notify.CreateMany(s.udb, rows); err != nil {
		log.Warnf("emitMentions: %v", err)
	}
}

func (s *CommunityService) resolveUsernames(ctx khttp.Context, names []string) map[string]uint {
	out := map[string]uint{}
	if s.reg == nil {
		return out
	}
	client, err := userrpc.ProfileClient(s.reg)
	if err != nil {
		return out
	}
	for _, name := range names {
		res, err := client.GetByUsername(context.Background(), &profile.GetByUsernameReq{Username: name})
		if err != nil || res == nil || res.UserId == 0 {
			res2, err2 := client.GetByUsername(context.Background(), &profile.GetByUsernameReq{Username: strings.ToLower(name)})
			if err2 != nil || res2 == nil || res2.UserId == 0 {
				continue
			}
			out[name] = uint(res2.UserId)
			continue
		}
		out[name] = uint(res.UserId)
	}
	return out
}

func userIDsFromComments(list []model.ProblemComment) []uint {
	seen := map[uint]struct{}{}
	out := make([]uint, 0, len(list))
	for _, c := range list {
		if _, ok := seen[c.UserID]; ok {
			continue
		}
		seen[c.UserID] = struct{}{}
		out = append(out, c.UserID)
	}
	return out
}

func userIDsFromSolutions(list []model.ProblemUserSolution) []uint {
	seen := map[uint]struct{}{}
	out := make([]uint, 0, len(list))
	for _, c := range list {
		if _, ok := seen[c.UserID]; ok {
			continue
		}
		seen[c.UserID] = struct{}{}
		out = append(out, c.UserID)
	}
	return out
}

func excerpt(s string, max int) string {
	s = strings.TrimSpace(s)
	s = strings.ReplaceAll(s, "\n", " ")
	if utf8.RuneCountInString(s) <= max {
		return s
	}
	r := []rune(s)
	return string(r[:max]) + "…"
}

func queryUint(ctx khttp.Context, key string) uint {
	v := strings.TrimSpace(ctx.Query().Get(key))
	if v == "" {
		return 0
	}
	n, err := strconv.ParseUint(v, 10, 64)
	if err != nil {
		return 0
	}
	return uint(n)
}

func queryInt(ctx khttp.Context, key string, def int) int {
	v := strings.TrimSpace(ctx.Query().Get(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil || n <= 0 {
		return def
	}
	return n
}

func pageParams(ctx khttp.Context, defPage, defSize, maxSize int) (page, pageSize int) {
	page = defPage
	pageSize = defSize
	if v := strings.TrimSpace(ctx.Query().Get("page")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			page = n
		}
	}
	if v := strings.TrimSpace(ctx.Query().Get("pageSize")); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			pageSize = n
		}
	}
	if pageSize > maxSize {
		pageSize = maxSize
	}
	return
}

func writeJSON(w http.ResponseWriter, status int, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func readJSONBody(r *http.Request, dst interface{}) error {
	defer r.Body.Close()
	body, err := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	if err != nil {
		return err
	}
	if len(body) == 0 {
		return nil
	}
	return json.Unmarshal(body, dst)
}

// silence unused if any
var _ = time.Now
