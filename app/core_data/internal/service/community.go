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
	maxReportReason  = 500
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
	// 评论（全站，支持层级）
	r.GET("/v1/core/problem/comment/list", s.handleCommentList)
	r.POST("/v1/core/problem/comment/create", s.handleCommentCreate)
	r.POST("/v1/core/problem/comment/delete", s.handleCommentDelete)
	// 用户题解（全站）
	r.GET("/v1/core/problem/solution/list", s.handleSolutionList)
	r.GET("/v1/core/problem/solution/get", s.handleSolutionGet)
	r.POST("/v1/core/problem/solution/create", s.handleSolutionCreate)
	r.POST("/v1/core/problem/solution/update", s.handleSolutionUpdate)
	r.POST("/v1/core/problem/solution/delete", s.handleSolutionDelete)
	// 点赞 / 举报（评论 + 题解）
	r.POST("/v1/core/problem/like", s.handleLikeToggle)
	r.POST("/v1/core/problem/report", s.handleReport)
	// 发现流：公共域全站聚合；私有域按组织隔离
	r.GET("/v1/core/activity/feed", s.handleActivityFeed)
	// 资料页近期
	r.GET("/v1/core/user/recent-comments", s.handleUserRecentComments)
	r.GET("/v1/core/user/recent-solutions", s.handleUserRecentSolutions)
}

// ---------- comments ----------

func (s *CommunityService) handleCommentList(ctx khttp.Context) error {
	pid := queryUint(ctx, "problemId")
	sid := queryUint(ctx, "solutionId")
	// 题解评论：可只传 solutionId；题目讨论：传 problemId（且 solution_id=0）
	if sid == 0 && pid == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "缺少题目或题解"})
		return nil
	}
	if sid > 0 {
		var sol model.ProblemUserSolution
		if s.db.Select("id, problem_id").First(&sol, sid).Error != nil {
			writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题解不存在"})
			return nil
		}
		if pid == 0 {
			pid = sol.ProblemID
		} else if sol.ProblemID != pid {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "题解与题目不匹配"})
			return nil
		}
	}
	page, pageSize := pageParams(ctx, 1, 20, 50)
	// 分页只计顶层评论；题目讨论与题解评论互不混入
	rootQ := s.db.Model(&model.ProblemComment{}).Where("parent_id = 0")
	if sid > 0 {
		rootQ = rootQ.Where("solution_id = ?", sid)
	} else {
		rootQ = rootQ.Where("problem_id = ? AND solution_id = 0", pid)
	}
	var total int64
	_ = rootQ.Count(&total).Error
	var roots []model.ProblemComment
	_ = rootQ.Order("id desc").Offset((page - 1) * pageSize).Limit(pageSize).Find(&roots).Error

	rootIDs := make([]uint, 0, len(roots))
	for _, r := range roots {
		rootIDs = append(rootIDs, r.ID)
	}

	// 拉本页根下的全部回复
	var replies []model.ProblemComment
	if len(rootIDs) > 0 {
		replyQ := s.db.Where("parent_id > 0 AND root_id IN ?", rootIDs)
		if sid > 0 {
			replyQ = replyQ.Where("solution_id = ?", sid)
		} else {
			replyQ = replyQ.Where("problem_id = ? AND solution_id = 0", pid)
		}
		_ = replyQ.Order("id asc").Find(&replies).Error
	}

	all := make([]model.ProblemComment, 0, len(roots)+len(replies))
	all = append(all, roots...)
	all = append(all, replies...)

	// 收集用户：作者 + 被回复用户
	uidSet := map[uint]struct{}{}
	uids := make([]uint, 0, len(all)*2)
	for _, c := range all {
		if _, ok := uidSet[c.UserID]; !ok {
			uidSet[c.UserID] = struct{}{}
			uids = append(uids, c.UserID)
		}
		if c.ReplyToUserID > 0 {
			if _, ok := uidSet[c.ReplyToUserID]; !ok {
				uidSet[c.ReplyToUserID] = struct{}{}
				uids = append(uids, c.ReplyToUserID)
			}
		}
	}
	users := s.batchUsers(ctx, uids)

	// 当前用户点赞集合
	var viewerID uint
	if pd := auth.GetCurrentUser(ctx); pd != nil {
		viewerID = pd.UserID
	}
	commentIDs := make([]uint, 0, len(all))
	for _, c := range all {
		commentIDs = append(commentIDs, c.ID)
	}
	likedSet := s.likedSet(viewerID, model.CommunityTargetComment, commentIDs)

	// 构建树：先 map，再挂 replies
	byID := make(map[uint]map[string]interface{}, len(all))
	for _, c := range all {
		byID[c.ID] = s.commentToMap(c, users, likedSet)
		byID[c.ID]["replies"] = []map[string]interface{}{}
	}
	// 按 id 升序挂到父节点，保证回复时间序
	ordered := make([]model.ProblemComment, 0, len(all))
	ordered = append(ordered, roots...)
	// replies 已 id asc
	ordered = append(ordered, replies...)
	for _, c := range ordered {
		if c.ParentID == 0 {
			continue
		}
		parent, ok := byID[c.ParentID]
		if !ok {
			// 父已删或跨页：挂到根
			if root, ok2 := byID[c.RootID]; ok2 {
				parent = root
			} else {
				continue
			}
		}
		list, _ := parent["replies"].([]map[string]interface{})
		parent["replies"] = append(list, byID[c.ID])
	}

	items := make([]map[string]interface{}, 0, len(roots))
	for _, r := range roots {
		if m, ok := byID[r.ID]; ok {
			items = append(items, m)
		}
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
		ProblemID    uint   `json:"problemId"`
		SolutionID   uint   `json:"solutionId"`   // 0=题目讨论；>0=题解评论
		Content      string `json:"content"`
		ParentID     uint   `json:"parentId"`     // 0=顶层；>0 回复某条
		SyncToPublic bool   `json:"syncToPublic"` // 仅题目顶层：非公共域时可选同步公共域发现流
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil {
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

	var sol model.ProblemUserSolution
	if req.SolutionID > 0 {
		if s.db.First(&sol, req.SolutionID).Error != nil {
			writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题解不存在"})
			return nil
		}
		if req.ProblemID == 0 {
			req.ProblemID = sol.ProblemID
		} else if sol.ProblemID != req.ProblemID {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "题解与题目不匹配"})
			return nil
		}
	}
	if req.ProblemID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	if !s.problemExists(req.ProblemID) {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题目不存在"})
		return nil
	}

	row := model.ProblemComment{
		ProblemID:  req.ProblemID,
		SolutionID: req.SolutionID,
		UserID:     pd.UserID,
		Content:    content,
		ParentID:   0,
		RootID:     0,
		Depth:      0,
	}

	var parent model.ProblemComment
	if req.ParentID > 0 {
		if s.db.First(&parent, req.ParentID).Error != nil {
			writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "要回复的评论不存在"})
			return nil
		}
		if parent.ProblemID != req.ProblemID {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "评论与题目不匹配"})
			return nil
		}
		if parent.SolutionID != req.SolutionID {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "评论与题解不匹配"})
			return nil
		}
		// 挂载点：父深度已达上限时，挂到其父节点（仍记录 replyTo 为用户点击的那条）
		attach := parent
		if parent.Depth >= model.MaxCommentDepth && parent.ParentID > 0 {
			var up model.ProblemComment
			if s.db.First(&up, parent.ParentID).Error == nil {
				attach = up
			}
		}
		row.ParentID = attach.ID
		if attach.RootID > 0 {
			row.RootID = attach.RootID
		} else {
			row.RootID = attach.ID
		}
		row.Depth = attach.Depth + 1
		if row.Depth > model.MaxCommentDepth {
			row.Depth = model.MaxCommentDepth
		}
		row.ReplyToUserID = parent.UserID
	}

	if err := s.db.Create(&row).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "发布失败"})
		return nil
	}
	// 顶层：root_id = 自身
	if row.ParentID == 0 {
		_ = s.db.Model(&row).Update("root_id", row.ID).Error
		row.RootID = row.ID
	}

	// 仅题目顶层评论写发现流（题解评论不进组织动态，避免刷屏）
	if row.ParentID == 0 && row.SolutionID == 0 {
		ex := excerpt(content, maxExcerptRunes)
		if pd.OrgID > 0 {
			_ = s.db.Create(&model.ActivityFeed{
				OrgID:     pd.OrgID,
				UserID:    pd.UserID,
				Type:      model.ActivityTypeComment,
				RefID:     row.ID,
				ProblemID: req.ProblemID,
				Title:     ex,
				Excerpt:   ex,
			}).Error
		}
		if req.SyncToPublic {
			pubID := s.resolvePublicOrgID(ctx)
			if pubID > 0 && pubID != pd.OrgID {
				_ = s.db.Create(&model.ActivityFeed{
					OrgID:     pubID,
					UserID:    pd.UserID,
					Type:      model.ActivityTypeComment,
					RefID:     row.ID,
					ProblemID: req.ProblemID,
					Title:     ex,
					Excerpt:   ex,
				}).Error
			}
		}
	}

	actorName := pd.Name
	if actorName == "" {
		actorName = pd.Username
	}

	// 回复通知（不通知自己）；题解线程跳转用 solution
	if row.ParentID > 0 && parent.UserID > 0 && parent.UserID != pd.UserID {
		refType, refID := "comment", row.ID
		if row.SolutionID > 0 {
			refType, refID = "solution", row.SolutionID
		}
		_ = notify.Create(s.udb, notify.Row{
			UserID:    parent.UserID,
			Type:      notify.TypeCommentReply,
			Title:     "有人回复了你",
			Body:      actorName + " 回复了你的评论",
			ActorID:   pd.UserID,
			RefType:   refType,
			RefID:     refID,
			ProblemID: req.ProblemID,
		})
	}

	// 题解顶层评论：通知题解作者
	if row.ParentID == 0 && row.SolutionID > 0 && sol.UserID > 0 && sol.UserID != pd.UserID {
		_ = notify.Create(s.udb, notify.Row{
			UserID:    sol.UserID,
			Type:      notify.TypeCommentReply,
			Title:     "有人评论了你的题解",
			Body:      actorName + " 评论了你的题解",
			ActorID:   pd.UserID,
			RefType:   "solution",
			RefID:     row.SolutionID,
			ProblemID: req.ProblemID,
		})
	}

	// @ 通知：题解下的评论跳到题解页
	mentionRefType, mentionRefID := "comment", row.ID
	if row.SolutionID > 0 {
		mentionRefType, mentionRefID = "solution", row.SolutionID
	}
	s.emitMentions(ctx, pd.UserID, pd.Username, content, mentionRefType, mentionRefID, req.ProblemID)

	users := s.batchUsers(ctx, []uint{row.UserID, row.ReplyToUserID})
	item := s.commentToMap(row, users, map[uint]bool{})
	item["replies"] = []interface{}{}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "已发布",
		"data": item,
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
	// 级联删除子树
	ids := s.collectCommentSubtreeIDs(row.ID)
	if len(ids) == 0 {
		ids = []uint{row.ID}
	}
	_ = s.db.Where("id IN ?", ids).Delete(&model.ProblemComment{}).Error
	_ = s.db.Where("type = ? AND ref_id IN ?", model.ActivityTypeComment, ids).Delete(&model.ActivityFeed{}).Error
	_ = s.db.Where("target_type = ? AND target_id IN ?", model.CommunityTargetComment, ids).Delete(&model.CommunityLike{}).Error
	_ = s.db.Where("target_type = ? AND target_id IN ?", model.CommunityTargetComment, ids).Delete(&model.CommunityReport{}).Error
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
	var viewerID uint
	if pd := auth.GetCurrentUser(ctx); pd != nil {
		viewerID = pd.UserID
	}
	solIDs := make([]uint, 0, len(list))
	for _, sol := range list {
		solIDs = append(solIDs, sol.ID)
	}
	likedSet := s.likedSet(viewerID, model.CommunityTargetSolution, solIDs)
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
			"likeCount": sol.LikeCount,
			"liked":     likedSet[sol.ID],
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
	var viewerID uint
	if pd := auth.GetCurrentUser(ctx); pd != nil {
		viewerID = pd.UserID
	}
	liked := false
	if viewerID > 0 {
		var n int64
		_ = s.db.Model(&model.CommunityLike{}).
			Where("user_id = ? AND target_type = ? AND target_id = ?", viewerID, model.CommunityTargetSolution, sol.ID).
			Count(&n).Error
		liked = n > 0
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok",
		"data": map[string]interface{}{
			"id": sol.ID, "problemId": sol.ProblemID, "userId": sol.UserID,
			"username": u.username, "name": u.name, "avatar": u.avatar,
			"title": sol.Title, "contentMd": sol.ContentMD,
			"likeCount": sol.LikeCount, "liked": liked,
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
	// 级联清理题解下评论及其点赞/举报/发现流
	var commentIDs []uint
	_ = s.db.Model(&model.ProblemComment{}).Where("solution_id = ?", row.ID).Pluck("id", &commentIDs).Error
	if len(commentIDs) > 0 {
		_ = s.db.Where("id IN ?", commentIDs).Delete(&model.ProblemComment{}).Error
		_ = s.db.Where("type = ? AND ref_id IN ?", model.ActivityTypeComment, commentIDs).Delete(&model.ActivityFeed{}).Error
		_ = s.db.Where("target_type = ? AND target_id IN ?", model.CommunityTargetComment, commentIDs).Delete(&model.CommunityLike{}).Error
		_ = s.db.Where("target_type = ? AND target_id IN ?", model.CommunityTargetComment, commentIDs).Delete(&model.CommunityReport{}).Error
	}
	_ = s.db.Delete(&row).Error
	_ = s.db.Where("type = ? AND ref_id = ?", model.ActivityTypeSolution, row.ID).Delete(&model.ActivityFeed{}).Error
	_ = s.db.Where("target_type = ? AND target_id = ?", model.CommunityTargetSolution, row.ID).Delete(&model.CommunityLike{}).Error
	_ = s.db.Where("target_type = ? AND target_id = ?", model.CommunityTargetSolution, row.ID).Delete(&model.CommunityReport{}).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{"success": true, "message": "已删除"})
	return nil
}

// ---------- like / report ----------

func (s *CommunityService) handleLikeToggle(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		TargetType string `json:"targetType"` // comment|solution
		TargetID   uint   `json:"targetId"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.TargetID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	tt := strings.TrimSpace(req.TargetType)
	if tt != model.CommunityTargetComment && tt != model.CommunityTargetSolution {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "不支持的点赞类型"})
		return nil
	}
	// 校验目标存在
	if !s.communityTargetExists(tt, req.TargetID) {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "内容不存在"})
		return nil
	}

	var existing model.CommunityLike
	err := s.db.Where("user_id = ? AND target_type = ? AND target_id = ?", pd.UserID, tt, req.TargetID).
		First(&existing).Error
	liked := false
	if err == nil {
		// 取消点赞
		_ = s.db.Delete(&existing).Error
		s.adjustLikeCount(tt, req.TargetID, -1)
		liked = false
	} else {
		if err := s.db.Create(&model.CommunityLike{
			UserID: pd.UserID, TargetType: tt, TargetID: req.TargetID,
		}).Error; err != nil {
			// 并发唯一冲突：视为已点赞
			liked = true
		} else {
			s.adjustLikeCount(tt, req.TargetID, 1)
			liked = true
		}
	}
	count := s.readLikeCount(tt, req.TargetID)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok",
		"data": map[string]interface{}{
			"liked": liked, "likeCount": count,
			"targetType": tt, "targetId": req.TargetID,
		},
	})
	return nil
}

func (s *CommunityService) handleReport(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		TargetType string `json:"targetType"` // comment|solution
		TargetID   uint   `json:"targetId"`
		Reason     string `json:"reason"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.TargetID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	tt := strings.TrimSpace(req.TargetType)
	if tt != model.CommunityTargetComment && tt != model.CommunityTargetSolution {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "不支持的举报类型"})
		return nil
	}
	reason := strings.TrimSpace(strings.ReplaceAll(req.Reason, "\r\n", "\n"))
	if reason == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "请填写举报原因"})
		return nil
	}
	if utf8.RuneCountInString(reason) > maxReportReason {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "举报原因过长"})
		return nil
	}
	if !s.communityTargetExists(tt, req.TargetID) {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "内容不存在"})
		return nil
	}
	// 不能举报自己
	if owner := s.communityTargetOwner(tt, req.TargetID); owner == pd.UserID {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "不能举报自己的内容"})
		return nil
	}
	var existing model.CommunityReport
	if s.db.Where("user_id = ? AND target_type = ? AND target_id = ?", pd.UserID, tt, req.TargetID).
		First(&existing).Error == nil {
		writeJSON(ctx.Response(), 200, map[string]interface{}{
			"success": true, "message": "你已举报过该内容，我们会尽快处理",
			"data": map[string]interface{}{"id": existing.ID, "alreadyReported": true},
		})
		return nil
	}
	row := model.CommunityReport{
		UserID:     pd.UserID,
		TargetType: tt,
		TargetID:   req.TargetID,
		Reason:     reason,
		Status:     model.ReportStatusPending,
	}
	if err := s.db.Create(&row).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "提交失败，请稍后重试"})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "已收到举报，我们会尽快处理",
		"data": map[string]interface{}{"id": row.ID, "alreadyReported": false},
	})
	return nil
}

// ---------- activity feed ----------
// 公共域 / 未登录：全站聚合（评论+题解），不区分发布时所属组织；按 (type,ref_id) 去重。
// 私有域：仅本组织条目。

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
	page, pageSize := pageParams(ctx, 1, 20, 50)
	typ := strings.TrimSpace(ctx.Query().Get("type")) // comment|solution|空=全部

	// 公共域视图：orgId=0（访客）或当前组织即公共域 → 全站聚合
	publicView := orgID == 0 || s.isPublicOrgID(ctx, orgID)

	var total int64
	var list []model.ActivityFeed
	if publicView {
		// 同一内容可能因 syncToPublic 写过多条 org 行：按 type+ref_id 取最大 id
		idSub := s.db.Model(&model.ActivityFeed{}).Select("MAX(id)")
		if typ == model.ActivityTypeComment || typ == model.ActivityTypeSolution {
			idSub = idSub.Where("type = ?", typ)
		}
		idSub = idSub.Group("type, ref_id")
		q := s.db.Model(&model.ActivityFeed{}).Where("id IN (?)", idSub)
		_ = q.Count(&total).Error
		_ = s.db.Model(&model.ActivityFeed{}).Where("id IN (?)", idSub).
			Order("id desc").Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error
	} else {
		q := s.db.Model(&model.ActivityFeed{}).Where("org_id = ?", orgID)
		if typ == model.ActivityTypeComment || typ == model.ActivityTypeSolution {
			q = q.Where("type = ?", typ)
		}
		_ = q.Count(&total).Error
		_ = q.Order("id desc").Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error
	}

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
			"id":           a.ID,
			"orgId":        a.OrgID,
			"userId":       a.UserID,
			"username":     u.username,
			"name":         u.name,
			"avatar":       u.avatar,
			"type":         a.Type,
			"refId":        a.RefID,
			"problemId":    a.ProblemID,
			"problemTitle": p.title,
			"platform":     p.platform,
			"title":        a.Title,
			"excerpt":      a.Excerpt,
			"createdAt":    a.CreatedAt.Unix(),
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "list": items, "total": total, "page": page, "pageSize": pageSize,
	})
	return nil
}

// isPublicOrgID 当前 org 是否为系统公共域。
func (s *CommunityService) isPublicOrgID(ctx khttp.Context, orgID uint) bool {
	if orgID == 0 {
		return true
	}
	pub := s.resolvePublicOrgID(ctx)
	return pub > 0 && pub == orgID
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

func (s *CommunityService) commentToMap(c model.ProblemComment, users map[uint]userBrief, likedSet map[uint]bool) map[string]interface{} {
	u := users[c.UserID]
	m := map[string]interface{}{
		"id":         c.ID,
		"problemId":  c.ProblemID,
		"solutionId": c.SolutionID,
		"userId":     c.UserID,
		"username":   u.username,
		"name":       u.name,
		"avatar":     u.avatar,
		"content":    c.Content,
		"parentId":   c.ParentID,
		"rootId":     c.RootID,
		"depth":      c.Depth,
		"likeCount":  c.LikeCount,
		"liked":      likedSet[c.ID],
		"createdAt":  c.CreatedAt.Unix(),
	}
	if c.ReplyToUserID > 0 {
		ru := users[c.ReplyToUserID]
		m["replyToUserId"] = c.ReplyToUserID
		m["replyToUsername"] = ru.username
		m["replyToName"] = ru.name
	}
	return m
}

func (s *CommunityService) collectCommentSubtreeIDs(root uint) []uint {
	ids := []uint{}
	queue := []uint{root}
	seen := map[uint]struct{}{root: {}}
	for len(queue) > 0 {
		cur := queue[0]
		queue = queue[1:]
		ids = append(ids, cur)
		var children []uint
		_ = s.db.Model(&model.ProblemComment{}).Where("parent_id = ?", cur).Pluck("id", &children).Error
		for _, cid := range children {
			if _, ok := seen[cid]; ok {
				continue
			}
			seen[cid] = struct{}{}
			queue = append(queue, cid)
		}
	}
	return ids
}

func (s *CommunityService) likedSet(userID uint, targetType string, ids []uint) map[uint]bool {
	out := map[uint]bool{}
	if userID == 0 || len(ids) == 0 {
		return out
	}
	var rows []model.CommunityLike
	_ = s.db.Where("user_id = ? AND target_type = ? AND target_id IN ?", userID, targetType, ids).
		Find(&rows).Error
	for _, r := range rows {
		out[r.TargetID] = true
	}
	return out
}

func (s *CommunityService) communityTargetExists(tt string, id uint) bool {
	var n int64
	switch tt {
	case model.CommunityTargetComment:
		_ = s.db.Model(&model.ProblemComment{}).Where("id = ?", id).Count(&n).Error
	case model.CommunityTargetSolution:
		_ = s.db.Model(&model.ProblemUserSolution{}).Where("id = ?", id).Count(&n).Error
	}
	return n > 0
}

func (s *CommunityService) communityTargetOwner(tt string, id uint) uint {
	switch tt {
	case model.CommunityTargetComment:
		var c model.ProblemComment
		if s.db.Select("user_id").First(&c, id).Error == nil {
			return c.UserID
		}
	case model.CommunityTargetSolution:
		var sol model.ProblemUserSolution
		if s.db.Select("user_id").First(&sol, id).Error == nil {
			return sol.UserID
		}
	}
	return 0
}

func (s *CommunityService) adjustLikeCount(tt string, id uint, delta int) {
	if delta == 0 {
		return
	}
	switch tt {
	case model.CommunityTargetComment:
		if delta > 0 {
			_ = s.db.Model(&model.ProblemComment{}).Where("id = ?", id).
				UpdateColumn("like_count", gorm.Expr("like_count + ?", delta)).Error
		} else {
			_ = s.db.Model(&model.ProblemComment{}).Where("id = ? AND like_count > 0", id).
				UpdateColumn("like_count", gorm.Expr("like_count + ?", delta)).Error
		}
	case model.CommunityTargetSolution:
		if delta > 0 {
			_ = s.db.Model(&model.ProblemUserSolution{}).Where("id = ?", id).
				UpdateColumn("like_count", gorm.Expr("like_count + ?", delta)).Error
		} else {
			_ = s.db.Model(&model.ProblemUserSolution{}).Where("id = ? AND like_count > 0", id).
				UpdateColumn("like_count", gorm.Expr("like_count + ?", delta)).Error
		}
	}
}

func (s *CommunityService) readLikeCount(tt string, id uint) int {
	switch tt {
	case model.CommunityTargetComment:
		var c model.ProblemComment
		if s.db.Select("like_count").First(&c, id).Error == nil {
			return c.LikeCount
		}
	case model.CommunityTargetSolution:
		var sol model.ProblemUserSolution
		if s.db.Select("like_count").First(&sol, id).Error == nil {
			return sol.LikeCount
		}
	}
	return 0
}

// resolvePublicOrgID 通过 user 服务 GetUserIdsByOrg(0) 回落得到公共域 orgId。
func (s *CommunityService) resolvePublicOrgID(ctx khttp.Context) uint {
	if s.reg == nil {
		return 0
	}
	client, err := userrpc.ProfileClient(s.reg)
	if err != nil {
		log.Warnf("resolvePublicOrgID dial: %v", err)
		return 0
	}
	pub, err := client.GetUserIdsByOrg(context.Background(), &profile.GetUserIdsByOrgReq{OrgId: 0})
	if err != nil || pub == nil {
		log.Warnf("resolvePublicOrgID: %v", err)
		return 0
	}
	return uint(pub.GetOrgId())
}

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
