package service

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	_const "cwxu-algo/app/common/const"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/utils/auth"
	biz "cwxu-algo/app/core_data/internal/biz/service"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/userrpc"
	"cwxu-algo/api/user/v1/profile"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

const (
	maxProblemsetTitleRunes = 100
	maxProblemsetDescRunes  = 5000
	problemsetUnlockTTL     = 24 * time.Hour
)

// ProblemsetService 题单（收藏/待做/自定义 + 广场）
type ProblemsetService struct {
	db  *gorm.DB
	uc  *biz.ProblemUseCase
	reg *registry.Registrar
}

func NewProblemsetService(d *data.Data, uc *biz.ProblemUseCase, reg *discovery.Register) *ProblemsetService {
	var r *registry.Registrar
	if reg != nil {
		r = &reg.Reg
	}
	return &ProblemsetService{db: d.DB, uc: uc, reg: r}
}

// RegisterProblemsetRoutes 注册题单路由
func RegisterProblemsetRoutes(srv *khttp.Server, s *ProblemsetService) {
	r := srv.Route("/")
	r.GET("/v1/core/problemset/mine", s.handleMine)
	r.GET("/v1/core/problemset/square", s.handleSquare)
	r.GET("/v1/core/problemset/get", s.handleGet)
	r.GET("/v1/core/problemset/by-problem", s.handleByProblem)
	r.POST("/v1/core/problemset/create", s.handleCreate)
	r.POST("/v1/core/problemset/update", s.handleUpdate)
	r.POST("/v1/core/problemset/delete", s.handleDelete)
	r.POST("/v1/core/problemset/unlock", s.handleUnlock)
	r.POST("/v1/core/problemset/add", s.handleAdd)
	r.POST("/v1/core/problemset/remove", s.handleRemove)
	r.POST("/v1/core/problemset/like", s.handleLike)
}

// ---------- visibility helpers（可单测）----------

// CanViewProblemset 是否可读题单正文/题目列表
// unlockOK=true 表示已校验密码 unlock token
func CanViewProblemset(viewerID uint, ps *model.Problemset, unlockOK bool) bool {
	if ps == nil {
		return false
	}
	if viewerID > 0 && viewerID == ps.OwnerID {
		return true
	}
	switch ps.Visibility {
	case model.ProblemsetVisPublic:
		return true
	case model.ProblemsetVisPassword:
		return unlockOK
	default: // private
		return false
	}
}

// IsPublicProblemset 是否开放到广场 / 题目页挂出
func IsPublicProblemset(ps *model.Problemset) bool {
	return ps != nil && ps.Visibility == model.ProblemsetVisPublic && ps.Kind == model.ProblemsetKindCustom
}

func hashProblemsetPassword(plain string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func checkProblemsetPassword(hash, plain string) bool {
	if hash == "" || plain == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)) == nil
}

func problemsetUnlockKey() []byte {
	h := sha256.Sum256([]byte("problemset-unlock:" + _const.JWTSecret()))
	return h[:]
}

func makeProblemsetUnlockToken(setID uint) string {
	exp := time.Now().Add(problemsetUnlockTTL).Unix()
	payload := fmt.Sprintf("%d:%d", setID, exp)
	mac := hmac.New(sha256.New, problemsetUnlockKey())
	_, _ = mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(payload + ":" + sig))
}

func verifyProblemsetUnlockToken(token string, setID uint) bool {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(token))
	if err != nil {
		return false
	}
	parts := strings.Split(string(raw), ":")
	if len(parts) != 3 {
		return false
	}
	id, _ := strconv.ParseUint(parts[0], 10, 64)
	exp, _ := strconv.ParseInt(parts[1], 10, 64)
	if uint(id) != setID || exp < time.Now().Unix() {
		return false
	}
	payload := parts[0] + ":" + parts[1]
	mac := hmac.New(sha256.New, problemsetUnlockKey())
	_, _ = mac.Write([]byte(payload))
	expect := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expect), []byte(parts[2]))
}

func normalizeVisibility(v string) string {
	switch strings.ToLower(strings.TrimSpace(v)) {
	case model.ProblemsetVisPublic:
		return model.ProblemsetVisPublic
	case model.ProblemsetVisPassword:
		return model.ProblemsetVisPassword
	default:
		return model.ProblemsetVisPrivate
	}
}

// ---------- handlers ----------

func (s *ProblemsetService) viewerID(ctx khttp.Context) uint {
	if pd := auth.GetCurrentUser(ctx); pd != nil {
		return pd.UserID
	}
	return 0
}

func (s *ProblemsetService) handleMine(ctx khttp.Context) error {
	uid := s.viewerID(ctx)
	if uid == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	if err := dal.EnsureSystemProblemsets(context.Background(), s.db, uid); err != nil {
		log.Warnf("EnsureSystemProblemsets user=%d: %v", uid, err)
	}
	var list []model.Problemset
	if err := s.db.Where("owner_id = ?", uid).
		Order("CASE kind WHEN 'favorites' THEN 0 WHEN 'todo' THEN 1 ELSE 2 END, updated_at DESC").
		Find(&list).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "加载失败"})
		return nil
	}
	liked := s.likedMap(uid, idsOfSets(list))
	items := make([]map[string]interface{}, 0, len(list))
	for i := range list {
		items = append(items, s.toBrief(&list[i], uid, liked[list[i].ID], false))
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "data": items,
	})
	return nil
}

func (s *ProblemsetService) handleSquare(ctx khttp.Context) error {
	page, pageSize := pageParams(ctx, 1, 20, 50)
	keyword := strings.TrimSpace(ctx.Query().Get("keyword"))
	q := s.db.Model(&model.Problemset{}).
		Where("visibility = ? AND kind = ?", model.ProblemsetVisPublic, model.ProblemsetKindCustom)
	if keyword != "" {
		like := "%" + keyword + "%"
		q = q.Where("title ILIKE ? OR description ILIKE ?", like, like)
	}
	var total int64
	_ = q.Count(&total).Error
	var list []model.Problemset
	if err := q.Order("like_count DESC, updated_at DESC").
		Offset((page - 1) * pageSize).Limit(pageSize).
		Find(&list).Error; err != nil {
		// sqlite 无 ILIKE：降级
		if keyword != "" {
			q2 := s.db.Model(&model.Problemset{}).
				Where("visibility = ? AND kind = ?", model.ProblemsetVisPublic, model.ProblemsetKindCustom).
				Where("title LIKE ? OR description LIKE ?", "%"+keyword+"%", "%"+keyword+"%")
			_ = q2.Count(&total).Error
			_ = q2.Order("like_count DESC, updated_at DESC").
				Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error
		} else {
			writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "加载失败"})
			return nil
		}
	}
	uid := s.viewerID(ctx)
	liked := s.likedMap(uid, idsOfSets(list))
	ownerNames := s.batchOwnerNames(ctx, list)
	items := make([]map[string]interface{}, 0, len(list))
	for i := range list {
		b := s.toBrief(&list[i], uid, liked[list[i].ID], false)
		b["ownerName"] = ownerNames[list[i].OwnerID]
		items = append(items, b)
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "data": items,
		"total": total, "page": page, "pageSize": pageSize,
	})
	return nil
}

func (s *ProblemsetService) handleGet(ctx khttp.Context) error {
	id := queryUint(ctx, "id")
	if id == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "缺少题单 id"})
		return nil
	}
	var ps model.Problemset
	if err := s.db.First(&ps, id).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题单不存在"})
		return nil
	}
	uid := s.viewerID(ctx)
	// 访问自己的题单时确保系统题单存在
	if uid > 0 && uid == ps.OwnerID {
		_ = dal.EnsureSystemProblemsets(context.Background(), s.db, uid)
	}
	unlockToken := strings.TrimSpace(ctx.Query().Get("unlockToken"))
	unlockOK := unlockToken != "" && verifyProblemsetUnlockToken(unlockToken, ps.ID)
	if !CanViewProblemset(uid, &ps, unlockOK) {
		if ps.Visibility == model.ProblemsetVisPassword {
			// HTTP 200 + success=false：便于前端拿到 locked 摘要（axios 对 403 会丢 body.data）
			writeJSON(ctx.Response(), 200, map[string]interface{}{
				"success": false, "message": "需要密码", "code": "PASSWORD_REQUIRED",
				"data": map[string]interface{}{
					"id": ps.ID, "title": ps.Title, "visibility": ps.Visibility,
					"ownerId": ps.OwnerID, "kind": ps.Kind, "likeCount": ps.LikeCount,
					"itemCount": ps.ItemCount, "locked": true,
				},
			})
			return nil
		}
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "无权查看该题单"})
		return nil
	}
	// 题目列表
	var items []model.ProblemsetItem
	_ = s.db.Where("problemset_id = ?", ps.ID).Order("sort_order ASC, id ASC").Find(&items).Error
	problemIDs := make([]uint, 0, len(items))
	for _, it := range items {
		problemIDs = append(problemIDs, it.ProblemID)
	}
	probMap := s.batchProblemsFull(problemIDs)
	statusMap := map[uint]string{}
	if uid > 0 && len(problemIDs) > 0 {
		statusMap, _ = dal.GetUserProblemStatuses(context.Background(), s.db, int64(uid), problemIDs)
	}
	outItems := make([]map[string]interface{}, 0, len(items))
	for _, it := range items {
		p := probMap[it.ProblemID]
		row := map[string]interface{}{
			"id": it.ID, "problemId": it.ProblemID, "sortOrder": it.SortOrder,
			"createdAt": it.CreatedAt.Unix(),
		}
		if p != nil {
			row["title"] = p.Title
			row["platform"] = p.Platform
			row["externalId"] = p.ExternalID
			row["url"] = p.URL
			row["difficulty"] = p.Difficulty
			row["status"] = p.Status
		}
		if st, ok := statusMap[it.ProblemID]; ok {
			row["userStatus"] = st
		}
		outItems = append(outItems, row)
	}
	liked := s.likedMap(uid, []uint{ps.ID})
	ownerNames := s.batchOwnerNames(ctx, []model.Problemset{ps})
	data := s.toBrief(&ps, uid, liked[ps.ID], true)
	data["description"] = ps.Description
	data["items"] = outItems
	data["ownerName"] = ownerNames[ps.OwnerID]
	data["isOwner"] = uid > 0 && uid == ps.OwnerID
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "data": data,
	})
	return nil
}

func (s *ProblemsetService) handleByProblem(ctx khttp.Context) error {
	pid := queryUint(ctx, "problemId")
	if pid == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "缺少 problemId"})
		return nil
	}
	// 仅公有自定义题单
	var setIDs []uint
	_ = s.db.Model(&model.ProblemsetItem{}).
		Where("problem_id = ?", pid).
		Pluck("problemset_id", &setIDs).Error
	if len(setIDs) == 0 {
		writeJSON(ctx.Response(), 200, map[string]interface{}{
			"success": true, "message": "ok", "data": []interface{}{},
		})
		return nil
	}
	var list []model.Problemset
	_ = s.db.Where("id IN ? AND visibility = ? AND kind = ?",
		setIDs, model.ProblemsetVisPublic, model.ProblemsetKindCustom).
		Order("like_count DESC, updated_at DESC").
		Limit(20).
		Find(&list).Error
	uid := s.viewerID(ctx)
	liked := s.likedMap(uid, idsOfSets(list))
	ownerNames := s.batchOwnerNames(ctx, list)
	items := make([]map[string]interface{}, 0, len(list))
	for i := range list {
		b := s.toBrief(&list[i], uid, liked[list[i].ID], false)
		b["ownerName"] = ownerNames[list[i].OwnerID]
		items = append(items, b)
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "data": items,
	})
	return nil
}

func (s *ProblemsetService) handleCreate(ctx khttp.Context) error {
	uid := s.viewerID(ctx)
	if uid == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		Title       string `json:"title"`
		Description string `json:"description"`
		Visibility  string `json:"visibility"`
		Password    string `json:"password"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	title := strings.TrimSpace(req.Title)
	if title == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "请填写题单标题"})
		return nil
	}
	if utf8.RuneCountInString(title) > maxProblemsetTitleRunes {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "标题过长"})
		return nil
	}
	desc := strings.TrimSpace(req.Description)
	if utf8.RuneCountInString(desc) > maxProblemsetDescRunes {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "描述过长"})
		return nil
	}
	vis := normalizeVisibility(req.Visibility)
	row := model.Problemset{
		OwnerID:     uid,
		Title:       title,
		Description: desc,
		Kind:        model.ProblemsetKindCustom,
		Visibility:  vis,
	}
	if vis == model.ProblemsetVisPassword {
		pw := strings.TrimSpace(req.Password)
		if pw == "" {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "请设置访问密码"})
			return nil
		}
		hash, err := hashProblemsetPassword(pw)
		if err != nil {
			writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "密码处理失败"})
			return nil
		}
		row.PasswordHash = hash
	}
	if err := s.db.Create(&row).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "创建失败"})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "data": s.toBrief(&row, uid, false, true),
	})
	return nil
}

func (s *ProblemsetService) handleUpdate(ctx khttp.Context) error {
	uid := s.viewerID(ctx)
	if uid == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		ID            uint   `json:"id"`
		Title         string `json:"title"`
		Description   string `json:"description"`
		Visibility    string `json:"visibility"`
		Password      string `json:"password"`
		ClearPassword bool   `json:"clearPassword"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	var ps model.Problemset
	if err := s.db.First(&ps, req.ID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题单不存在"})
		return nil
	}
	if ps.OwnerID != uid {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "只能修改自己的题单"})
		return nil
	}
	updates := map[string]interface{}{}
	if t := strings.TrimSpace(req.Title); t != "" {
		if utf8.RuneCountInString(t) > maxProblemsetTitleRunes {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "标题过长"})
			return nil
		}
		// 系统题单标题固定
		if ps.Kind == model.ProblemsetKindCustom {
			updates["title"] = t
		}
	}
	if req.Description != "" || ctx.Request().ContentLength > 0 {
		// 允许清空描述：前端始终传 description 字段
		desc := strings.TrimSpace(req.Description)
		if utf8.RuneCountInString(desc) > maxProblemsetDescRunes {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "描述过长"})
			return nil
		}
		updates["description"] = desc
	}
	// 系统题单强制 private
	if ps.Kind != model.ProblemsetKindCustom {
		// 只允许改描述
	} else if v := strings.TrimSpace(req.Visibility); v != "" {
		vis := normalizeVisibility(v)
		updates["visibility"] = vis
		if vis == model.ProblemsetVisPassword {
			pw := strings.TrimSpace(req.Password)
			if pw != "" {
				hash, err := hashProblemsetPassword(pw)
				if err != nil {
					writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "密码处理失败"})
					return nil
				}
				updates["password_hash"] = hash
			} else if ps.PasswordHash == "" {
				writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "请设置访问密码"})
				return nil
			}
		} else {
			if req.ClearPassword || vis != model.ProblemsetVisPassword {
				updates["password_hash"] = ""
			}
		}
	}
	if len(updates) > 0 {
		if err := s.db.Model(&ps).Updates(updates).Error; err != nil {
			writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "更新失败"})
			return nil
		}
		_ = s.db.First(&ps, ps.ID)
	}
	liked := s.likedMap(uid, []uint{ps.ID})
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok", "data": s.toBrief(&ps, uid, liked[ps.ID], true),
	})
	return nil
}

func (s *ProblemsetService) handleDelete(ctx khttp.Context) error {
	uid := s.viewerID(ctx)
	if uid == 0 {
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
	var ps model.Problemset
	if err := s.db.First(&ps, req.ID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题单不存在"})
		return nil
	}
	if ps.OwnerID != uid {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "只能删除自己的题单"})
		return nil
	}
	if ps.Kind != model.ProblemsetKindCustom {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "系统题单不可删除"})
		return nil
	}
	_ = s.db.Where("problemset_id = ?", ps.ID).Delete(&model.ProblemsetItem{}).Error
	_ = s.db.Where("problemset_id = ?", ps.ID).Delete(&model.ProblemsetLike{}).Error
	if err := s.db.Delete(&ps).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "删除失败"})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"success": true, "message": "ok"})
	return nil
}

func (s *ProblemsetService) handleUnlock(ctx khttp.Context) error {
	var req struct {
		ID       uint   `json:"id"`
		Password string `json:"password"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	var ps model.Problemset
	if err := s.db.First(&ps, req.ID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题单不存在"})
		return nil
	}
	if ps.Visibility != model.ProblemsetVisPassword {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "该题单无需密码"})
		return nil
	}
	if !checkProblemsetPassword(ps.PasswordHash, strings.TrimSpace(req.Password)) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "密码错误"})
		return nil
	}
	token := makeProblemsetUnlockToken(ps.ID)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok",
		"data": map[string]interface{}{"unlockToken": token, "expiresIn": int(problemsetUnlockTTL.Seconds())},
	})
	return nil
}

func (s *ProblemsetService) handleAdd(ctx khttp.Context) error {
	uid := s.viewerID(ctx)
	if uid == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		ProblemsetID uint   `json:"problemsetId"`
		ProblemID    uint   `json:"problemId"`
		URL          string `json:"url"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.ProblemsetID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	var ps model.Problemset
	if err := s.db.First(&ps, req.ProblemsetID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题单不存在"})
		return nil
	}
	if ps.OwnerID != uid {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "只能向自己的题单加题"})
		return nil
	}

	var problemID uint
	fetchTriggered := false
	if req.ProblemID > 0 {
		var p model.Problem
		if err := s.db.First(&p, req.ProblemID).Error; err != nil {
			writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题目不存在"})
			return nil
		}
		problemID = p.ID
		if strings.TrimSpace(p.ContentMD) == "" && s.uc != nil {
			if err := s.uc.ForceEnqueueFetchOnly(p.ID); err != nil {
				log.Warnf("problemset add force fetch id=%d: %v", p.ID, err)
			} else {
				fetchTriggered = true
			}
		}
	} else if u := strings.TrimSpace(req.URL); u != "" {
		parsed, err := biz.ParseProblemURL(u)
		if err != nil {
			writeJSON(ctx.Response(), 400, map[string]interface{}{
				"success": false, "message": "无法识别该题目链接", "code": "URL_PARSE_FAILED",
			})
			return nil
		}
		if s.uc == nil {
			// 无 usecase：仅查库或建空记录
			var existing model.Problem
			err := s.db.Where("platform = ? AND external_id = ?", parsed.Platform, parsed.ExternalID).First(&existing).Error
			if err == gorm.ErrRecordNotFound {
				existing = model.Problem{
					Platform: parsed.Platform, ExternalID: parsed.ExternalID,
					Title: parsed.Title, URL: parsed.URL, Status: model.ProblemStatusPending,
					Tags: model.StringArray{},
				}
				if err := s.db.Create(&existing).Error; err != nil {
					writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "入库失败"})
					return nil
				}
			} else if err != nil {
				writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "查询题目失败"})
				return nil
			}
			problemID = existing.ID
		} else {
			p, err := s.uc.UpsertProblemFromParsed(parsed)
			if err != nil || p == nil {
				writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "题目处理失败"})
				return nil
			}
			problemID = p.ID
			fetchTriggered = strings.TrimSpace(p.ContentMD) == ""
		}
	} else {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "请提供题目 id 或链接"})
		return nil
	}

	// 已存在则幂等成功
	var n int64
	_ = s.db.Model(&model.ProblemsetItem{}).
		Where("problemset_id = ? AND problem_id = ?", ps.ID, problemID).
		Count(&n).Error
	if n == 0 {
		// sort_order = max+1
		var maxSort int
		_ = s.db.Model(&model.ProblemsetItem{}).
			Where("problemset_id = ?", ps.ID).
			Select("COALESCE(MAX(sort_order),0)").Scan(&maxSort).Error
		item := model.ProblemsetItem{
			ProblemsetID: ps.ID,
			ProblemID:    problemID,
			SortOrder:    maxSort + 1,
		}
		if err := s.db.Create(&item).Error; err != nil {
			writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "加入失败"})
			return nil
		}
		_ = s.db.Model(&model.Problemset{}).Where("id = ?", ps.ID).
			UpdateColumn("item_count", gorm.Expr("item_count + 1")).Error
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok",
		"data": map[string]interface{}{
			"problemId": problemID, "fetchTriggered": fetchTriggered,
		},
	})
	return nil
}

func (s *ProblemsetService) handleRemove(ctx khttp.Context) error {
	uid := s.viewerID(ctx)
	if uid == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"success": false, "message": "请先登录"})
		return nil
	}
	var req struct {
		ProblemsetID uint `json:"problemsetId"`
		ProblemID    uint `json:"problemId"`
	}
	if err := readJSONBody(ctx.Request(), &req); err != nil || req.ProblemsetID == 0 || req.ProblemID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"success": false, "message": "参数错误"})
		return nil
	}
	var ps model.Problemset
	if err := s.db.First(&ps, req.ProblemsetID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题单不存在"})
		return nil
	}
	if ps.OwnerID != uid {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "只能修改自己的题单"})
		return nil
	}
	res := s.db.Where("problemset_id = ? AND problem_id = ?", ps.ID, req.ProblemID).
		Delete(&model.ProblemsetItem{})
	if res.Error != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "移除失败"})
		return nil
	}
	if res.RowsAffected > 0 {
		_ = s.db.Model(&model.Problemset{}).
			Where("id = ? AND item_count > 0", ps.ID).
			UpdateColumn("item_count", gorm.Expr("item_count - 1")).Error
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"success": true, "message": "ok"})
	return nil
}

func (s *ProblemsetService) handleLike(ctx khttp.Context) error {
	uid := s.viewerID(ctx)
	if uid == 0 {
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
	var ps model.Problemset
	if err := s.db.First(&ps, req.ID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"success": false, "message": "题单不存在"})
		return nil
	}
	// 仅公有题单可点赞；所有者也可赞自己
	if ps.Visibility != model.ProblemsetVisPublic && ps.OwnerID != uid {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"success": false, "message": "该题单不可点赞"})
		return nil
	}
	var existing model.ProblemsetLike
	err := s.db.Where("user_id = ? AND problemset_id = ?", uid, ps.ID).First(&existing).Error
	liked := false
	if err == gorm.ErrRecordNotFound {
		if err := s.db.Create(&model.ProblemsetLike{UserID: uid, ProblemsetID: ps.ID}).Error; err != nil {
			writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "点赞失败"})
			return nil
		}
		_ = s.db.Model(&model.Problemset{}).Where("id = ?", ps.ID).
			UpdateColumn("like_count", gorm.Expr("like_count + 1")).Error
		liked = true
	} else if err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"success": false, "message": "点赞失败"})
		return nil
	} else {
		_ = s.db.Delete(&existing).Error
		_ = s.db.Model(&model.Problemset{}).Where("id = ? AND like_count > 0", ps.ID).
			UpdateColumn("like_count", gorm.Expr("like_count - 1")).Error
		liked = false
	}
	var likeCount int
	_ = s.db.Model(&model.Problemset{}).Select("like_count").Where("id = ?", ps.ID).Scan(&likeCount).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"success": true, "message": "ok",
		"data": map[string]interface{}{"liked": liked, "likeCount": likeCount},
	})
	return nil
}

// ---------- serializers / helpers ----------

func (s *ProblemsetService) toBrief(ps *model.Problemset, viewerID uint, liked, withDesc bool) map[string]interface{} {
	m := map[string]interface{}{
		"id":          ps.ID,
		"ownerId":     ps.OwnerID,
		"title":       ps.Title,
		"kind":        ps.Kind,
		"visibility":  ps.Visibility,
		"likeCount":   ps.LikeCount,
		"itemCount":   ps.ItemCount,
		"liked":       liked,
		"isOwner":     viewerID > 0 && viewerID == ps.OwnerID,
		"createdAt":   ps.CreatedAt.Unix(),
		"updatedAt":   ps.UpdatedAt.Unix(),
		"isSystem":    ps.Kind == model.ProblemsetKindFavorites || ps.Kind == model.ProblemsetKindTodo,
	}
	if withDesc {
		m["description"] = ps.Description
	}
	return m
}

func idsOfSets(list []model.Problemset) []uint {
	out := make([]uint, 0, len(list))
	for _, p := range list {
		out = append(out, p.ID)
	}
	return out
}

func (s *ProblemsetService) likedMap(userID uint, setIDs []uint) map[uint]bool {
	out := map[uint]bool{}
	if userID == 0 || len(setIDs) == 0 {
		return out
	}
	var rows []model.ProblemsetLike
	_ = s.db.Where("user_id = ? AND problemset_id IN ?", userID, setIDs).Find(&rows).Error
	for _, r := range rows {
		out[r.ProblemsetID] = true
	}
	return out
}

func (s *ProblemsetService) batchProblemsFull(ids []uint) map[uint]*model.Problem {
	out := map[uint]*model.Problem{}
	if len(ids) == 0 {
		return out
	}
	var list []model.Problem
	_ = s.db.Where("id IN ?", ids).Find(&list).Error
	for i := range list {
		p := list[i]
		out[p.ID] = &p
	}
	return out
}

func (s *ProblemsetService) batchOwnerNames(ctx khttp.Context, list []model.Problemset) map[uint]string {
	out := map[uint]string{}
	if len(list) == 0 || s.reg == nil {
		return out
	}
	seen := map[uint]struct{}{}
	ids := make([]int64, 0)
	for _, p := range list {
		if _, ok := seen[p.OwnerID]; ok {
			continue
		}
		seen[p.OwnerID] = struct{}{}
		ids = append(ids, int64(p.OwnerID))
	}
	client, err := userrpc.ProfileClient(s.reg)
	if err != nil {
		return out
	}
	var orgID int64
	if pd := auth.GetCurrentUser(ctx); pd != nil {
		orgID = int64(pd.OrgID)
	}
	res, err := client.GetByIds(context.Background(), &profile.GetByIdsReq{UserIds: ids, OrgId: orgID})
	if err != nil || res == nil {
		return out
	}
	for _, u := range res.Profiles {
		if u == nil {
			continue
		}
		name := u.Name
		if name == "" {
			name = u.Username
		}
		out[uint(u.UserId)] = name
	}
	return out
}

// silence unused import if rand unused in some builds
var _ = rand.Read
var _ = http.StatusOK
