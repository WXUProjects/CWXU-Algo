package service

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"cwxu-algo/app/common/blogsync"
	_const "cwxu-algo/app/common/const"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/user/internal/biz/blogaccess"
	"cwxu-algo/app/user/internal/data"
	"cwxu-algo/app/user/internal/data/model"

	khttp "github.com/go-kratos/kratos/v2/transport/http"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

const (
	maxBlogTitle   = 200
	maxBlogSummary = 500
	maxBlogContent = 512 << 10 // 512KB
	maxBlogCover   = 1024
	maxBlogSlug    = 96
	maxCommentLen  = 4000
	blogUnlockTTL  = 12 * time.Hour
)

// BlogService personal blog articles, comments, likes, categories, theme flags.
type BlogService struct {
	db *gorm.DB
}

func NewBlogService(d *data.Data) *BlogService {
	return &BlogService{db: d.DB}
}

// RegisterBlogRoutes registers blog HTTP routes.
func RegisterBlogRoutes(srv *khttp.Server, bs *BlogService) {
	r := srv.Route("/")
	// Public / optional-JWT reads
	r.GET("/v1/user/blog/by-username", bs.handleListByUsername)
	r.GET("/v1/user/blog/article/get", bs.handleGetArticle)
	r.POST("/v1/user/blog/article/unlock", bs.handleUnlock)
	r.GET("/v1/user/blog/recommend", bs.handleRecommend)
	r.GET("/v1/user/blog/plaza", bs.handlePlaza)
	r.GET("/v1/user/blog/authors", bs.handleAuthors)
	r.GET("/v1/user/blog/categories", bs.handleListCategoriesPublic)
	r.GET("/v1/user/blog/comment/list", bs.handleListComments)
	r.GET("/v1/user/blog/theme/status", bs.handleThemeStatus)

	// Owner / authenticated writes
	r.POST("/v1/user/blog/article/create", bs.handleCreate)
	r.POST("/v1/user/blog/article/update", bs.handleUpdate)
	r.POST("/v1/user/blog/article/delete", bs.handleDelete)
	r.GET("/v1/user/blog/article/mine", bs.handleMine)
	r.GET("/v1/user/blog/analytics", bs.handleAnalytics)

	r.POST("/v1/user/blog/category/create", bs.handleCategoryCreate)
	r.POST("/v1/user/blog/category/update", bs.handleCategoryUpdate)
	r.POST("/v1/user/blog/category/delete", bs.handleCategoryDelete)
	r.GET("/v1/user/blog/category/mine", bs.handleCategoryMine)

	r.POST("/v1/user/blog/comment/create", bs.handleCommentCreate)
	r.POST("/v1/user/blog/comment/delete", bs.handleCommentDelete)
	r.POST("/v1/user/blog/like", bs.handleLikeToggle)

	// Owner theme config (themeId + social links)
	r.POST("/v1/user/blog/theme/config", bs.handleThemeConfigSave)
	// Site-admin theme enable (legacy custom-theme capability)
	r.POST("/v1/user/blog/theme/enable", bs.handleThemeEnable)
}

// ---------- helpers ----------

func blogViewerID(ctx khttp.Context) uint {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil {
		return 0
	}
	return pd.UserID
}

func blogIsSiteAdmin(ctx khttp.Context) bool {
	pd := auth.GetCurrentUser(ctx)
	return pd != nil && pd.IsSiteAdmin
}

func (s *BlogService) publicOrgID() uint {
	id, err := data.EnsurePublicOrgID(s.db)
	if err != nil {
		return 0
	}
	return id
}

func (s *BlogService) isSystemOrg(orgID uint) bool {
	var o model.Org
	if err := s.db.Select("id", "is_system").Where("id = ?", orgID).First(&o).Error; err != nil {
		return false
	}
	return o.IsSystem
}

func (s *BlogService) findUserByUsername(username string) (*model.User, error) {
	var u model.User
	err := s.db.Where("username = ?", strings.TrimSpace(username)).First(&u).Error
	if err != nil {
		return nil, err
	}
	return &u, nil
}

func hashBlogPassword(plain string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(plain), bcrypt.DefaultCost)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func checkBlogPassword(hash, plain string) bool {
	if hash == "" || plain == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(plain)) == nil
}

// unlock token: base64(articleID:expUnix:hmac)
func (s *BlogService) makeUnlockToken(articleID uint) string {
	exp := time.Now().Add(blogUnlockTTL).Unix()
	payload := fmt.Sprintf("%d:%d", articleID, exp)
	mac := hmac.New(sha256.New, blogUnlockKey())
	_, _ = mac.Write([]byte(payload))
	sig := hex.EncodeToString(mac.Sum(nil))
	return base64.RawURLEncoding.EncodeToString([]byte(payload + ":" + sig))
}

func (s *BlogService) verifyUnlockToken(token string, articleID uint) bool {
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
	if uint(id) != articleID || exp < time.Now().Unix() {
		return false
	}
	payload := parts[0] + ":" + parts[1]
	mac := hmac.New(sha256.New, blogUnlockKey())
	_, _ = mac.Write([]byte(payload))
	expect := hex.EncodeToString(mac.Sum(nil))
	return hmac.Equal([]byte(expect), []byte(parts[2]))
}

func blogUnlockKey() []byte {
	// derive from JWT secret so tokens invalidate when secret rotates
	h := sha256.Sum256([]byte("blog-unlock:" + _const.JWTSecret()))
	return h[:]
}

func randomBlogSlug(n int) (string, error) {
	const alphabet = "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		var rb [1]byte
		if _, err := rand.Read(rb[:]); err != nil {
			return "", err
		}
		b[i] = alphabet[int(rb[0])%len(alphabet)]
	}
	return string(b), nil
}

func slugifyTitle(title string) string {
	// keep alnum and hyphen; fallback random
	var b strings.Builder
	for _, r := range strings.ToLower(strings.TrimSpace(title)) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else if r == ' ' || r == '_' || r == '-' {
			b.WriteByte('-')
		}
	}
	s := strings.Trim(b.String(), "-")
	for strings.Contains(s, "--") {
		s = strings.ReplaceAll(s, "--", "-")
	}
	if s == "" {
		return ""
	}
	if utf8.RuneCountInString(s) > 60 {
		runes := []rune(s)
		s = string(runes[:60])
		s = strings.Trim(s, "-")
	}
	return s
}

func (s *BlogService) loadOrgIDs(articleID uint) []uint {
	var rows []model.BlogArticleOrg
	_ = s.db.Where("article_id = ?", articleID).Find(&rows).Error
	out := make([]uint, 0, len(rows))
	for _, r := range rows {
		out = append(out, r.OrgID)
	}
	return out
}

func (s *BlogService) replaceOrgSync(articleID uint, orgIDs []uint) error {
	pub := s.publicOrgID()
	expanded := blogaccess.ExpandSyncOrgIDs(orgIDs, pub, s.isSystemOrg)
	return s.db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("article_id = ?", articleID).Delete(&model.BlogArticleOrg{}).Error; err != nil {
			return err
		}
		for _, oid := range expanded {
			if err := tx.Create(&model.BlogArticleOrg{ArticleID: articleID, OrgID: oid}).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *BlogService) likedBy(articleID, userID uint) bool {
	if userID == 0 {
		return false
	}
	var n int64
	s.db.Model(&model.BlogLike{}).Where("article_id = ? AND user_id = ?", articleID, userID).Count(&n)
	return n > 0
}

func (s *BlogService) articleToMap(a *model.BlogArticle, author *model.User, d blogaccess.Decision, viewerID uint, includeBody bool) map[string]interface{} {
	m := map[string]interface{}{
		"id":                a.ID,
		"slug":              a.Slug,
		"title":             a.Title,
		"summary":           a.Summary,
		"coverUrl":          a.CoverURL,
		"visibility":        a.Visibility,
		"recommend":         a.Recommend,
		"syncToMainProfile": a.SyncToMainProfile,
		"categoryId":        a.CategoryID,
		"viewCount":         a.ViewCount,
		"likeCount":         a.LikeCount,
		"commentCount":      a.CommentCount,
		"liked":             s.likedBy(a.ID, viewerID),
		"requiresPassword":  d.RequiresPassword,
		"canSeeBody":        d.CanSeeBody,
		"createdAt":         a.CreatedAt.Unix(),
		"updatedAt":         a.UpdatedAt.Unix(),
		"orgIds":            s.loadOrgIDs(a.ID),
	}
	if a.SourceSolutionID != nil && *a.SourceSolutionID > 0 {
		m["sourceSolutionId"] = *a.SourceSolutionID
	}
	if a.PublishedAt != nil {
		m["publishedAt"] = a.PublishedAt.Unix()
	} else {
		m["publishedAt"] = a.CreatedAt.Unix()
	}
	if author != nil {
		m["author"] = map[string]interface{}{
			"id":       author.ID,
			"username": author.Username,
			"name":     author.Name,
			"avatar":   author.Avatar,
		}
		m["userId"] = author.ID
		m["username"] = author.Username
	} else {
		m["userId"] = a.UserID
	}
	if includeBody && d.CanSeeBody {
		m["content"] = a.Content
	} else {
		m["content"] = ""
	}
	// never leak password hash
	return m
}

func parsePage(q *http.Request) (page, pageSize int) {
	page, _ = strconv.Atoi(q.URL.Query().Get("page"))
	pageSize, _ = strconv.Atoi(q.URL.Query().Get("pageSize"))
	if page < 1 {
		page = 1
	}
	if pageSize < 1 || pageSize > 50 {
		pageSize = 20
	}
	return page, pageSize
}

// ---------- list by username ----------

func (s *BlogService) handleListByUsername(ctx khttp.Context) error {
	username := strings.TrimSpace(ctx.Request().URL.Query().Get("username"))
	if username == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "缺少用户名"})
		return nil
	}
	u, err := s.findUserByUsername(username)
	if err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "用户不存在"})
		return nil
	}
	viewer := blogViewerID(ctx)
	page, pageSize := parsePage(ctx.Request())
	categoryID, _ := strconv.ParseUint(ctx.Request().URL.Query().Get("categoryId"), 10, 64)
	keyword := strings.TrimSpace(ctx.Request().URL.Query().Get("keyword"))

	q := s.db.Model(&model.BlogArticle{}).Where("user_id = ?", u.ID)
	// non-owner: only public + password (meta); never private
	if viewer != u.ID {
		q = q.Where("visibility IN ?", []string{blogaccess.VisibilityPublic, blogaccess.VisibilityPassword})
	}
	if categoryID > 0 {
		q = q.Where("category_id = ?", categoryID)
	}
	if keyword != "" {
		like := "%" + keyword + "%"
		q = q.Where("title ILIKE ? OR summary ILIKE ?", like, like)
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "加载失败"})
		return nil
	}
	var list []model.BlogArticle
	if err := q.Order("COALESCE(published_at, created_at) DESC").
		Offset((page - 1) * pageSize).Limit(pageSize).
		Find(&list).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "加载失败"})
		return nil
	}
	out := make([]map[string]interface{}, 0, len(list))
	for i := range list {
		d := blogaccess.Evaluate(blogaccess.ArticleAccess{
			Visibility:  list[i].Visibility,
			OwnerID:     list[i].UserID,
			HasPassword: list[i].PasswordHash != "",
		}, viewer, false)
		if !d.CanSeeMeta {
			continue
		}
		out = append(out, s.articleToMap(&list[i], u, d, viewer, false))
	}

	// theme status for blog shell
	themeOn := s.themeEnabledFor(u.ID)
	siteCfg := s.loadSiteConfig(u.ID)

	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"author": map[string]interface{}{
				"id":       u.ID,
				"username": u.Username,
				"name":     u.Name,
				"avatar":   u.Avatar,
			},
			"list":         out,
			"total":        total,
			"page":         page,
			"pageSize":     pageSize,
			"themeEnabled": themeOn,
			"themeId":      siteCfg.ThemeID,
			"subtitle":     siteCfg.Subtitle,
			"socialLinks":  siteCfg.SocialLinks,
			"isOwner":      viewer == u.ID,
		},
	})
	return nil
}

// ---------- get article ----------

func (s *BlogService) handleGetArticle(ctx khttp.Context) error {
	id, _ := strconv.ParseUint(ctx.Request().URL.Query().Get("id"), 10, 64)
	username := strings.TrimSpace(ctx.Request().URL.Query().Get("username"))
	slug := strings.TrimSpace(ctx.Request().URL.Query().Get("slug"))
	password := ctx.Request().URL.Query().Get("password")
	unlock := ctx.Request().URL.Query().Get("unlockToken")

	var a model.BlogArticle
	var err error
	if id > 0 {
		err = s.db.First(&a, id).Error
	} else if username != "" && slug != "" {
		u, e := s.findUserByUsername(username)
		if e != nil {
			writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
			return nil
		}
		err = s.db.Where("user_id = ? AND slug = ?", u.ID, slug).First(&a).Error
	} else {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "缺少 id 或 username+slug"})
		return nil
	}
	if err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
		return nil
	}

	viewer := blogViewerID(ctx)
	passwordOK := false
	if unlock != "" && s.verifyUnlockToken(unlock, a.ID) {
		passwordOK = true
	} else if password != "" && checkBlogPassword(a.PasswordHash, password) {
		passwordOK = true
	}
	d := blogaccess.Evaluate(blogaccess.ArticleAccess{
		Visibility:  a.Visibility,
		OwnerID:     a.UserID,
		HasPassword: a.PasswordHash != "",
	}, viewer, passwordOK)

	if !d.CanSeeMeta {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在或无权查看"})
		return nil
	}

	// increment view when body is visible
	if d.CanSeeBody {
		_ = s.db.Model(&a).UpdateColumn("view_count", gorm.Expr("view_count + 1")).Error
		a.ViewCount++
	}

	var author model.User
	_ = s.db.Select("id", "username", "name", "avatar").First(&author, a.UserID).Error

	m := s.articleToMap(&a, &author, d, viewer, true)
	if d.RequiresPassword {
		m["message"] = "需要密码才能阅读全文"
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    m,
	})
	return nil
}

func (s *BlogService) handleUnlock(ctx khttp.Context) error {
	var body struct {
		ID       uint   `json:"id"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil || body.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var a model.BlogArticle
	if err := s.db.First(&a, body.ID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
		return nil
	}
	if blogaccess.NormalizeVisibility(a.Visibility) != blogaccess.VisibilityPassword {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "该文章无需密码"})
		return nil
	}
	if !checkBlogPassword(a.PasswordHash, body.Password) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "密码不正确"})
		return nil
	}
	token := s.makeUnlockToken(a.ID)
	viewer := blogViewerID(ctx)
	d := blogaccess.Evaluate(blogaccess.ArticleAccess{
		Visibility:  a.Visibility,
		OwnerID:     a.UserID,
		HasPassword: true,
	}, viewer, true)
	var author model.User
	_ = s.db.Select("id", "username", "name", "avatar").First(&author, a.UserID).Error
	_ = s.db.Model(&a).UpdateColumn("view_count", gorm.Expr("view_count + 1")).Error
	a.ViewCount++
	m := s.articleToMap(&a, &author, d, viewer, true)
	m["unlockToken"] = token
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    m,
	})
	return nil
}

// ---------- CRUD ----------

type blogArticleWriteReq struct {
	ID                uint   `json:"id"`
	Title             string `json:"title"`
	Slug              string `json:"slug"`
	Summary           string `json:"summary"`
	Content           string `json:"content"`
	CoverURL          string `json:"coverUrl"`
	Visibility        string `json:"visibility"`
	Password          string `json:"password"`
	Recommend         bool   `json:"recommend"`
	SyncToMainProfile bool   `json:"syncToMainProfile"`
	CategoryID        *uint  `json:"categoryId"`
	OrgIDs            []uint `json:"orgIds"`
	// ClearPassword when true removes password on update (if visibility changes away).
	ClearPassword bool `json:"clearPassword"`
}

func (s *BlogService) handleCreate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req blogArticleWriteReq
	if err := json.NewDecoder(ctx.Request().Body).Decode(&req); err != nil {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	a, msg := s.buildArticleFromReq(pd.UserID, 0, &req, true)
	if msg != "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": msg})
		return nil
	}
	now := time.Now()
	a.PublishedAt = &now
	if err := s.db.Create(a).Error; err != nil {
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "短链已被占用，请换一个"})
			return nil
		}
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "保存失败"})
		return nil
	}
	_ = s.replaceOrgSync(a.ID, req.OrgIDs)
	d := blogaccess.Evaluate(blogaccess.ArticleAccess{
		Visibility:  a.Visibility,
		OwnerID:     a.UserID,
		HasPassword: a.PasswordHash != "",
	}, pd.UserID, true)
	var author model.User
	_ = s.db.Select("id", "username", "name", "avatar").First(&author, a.UserID).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    s.articleToMap(a, &author, d, pd.UserID, true),
	})
	return nil
}

func (s *BlogService) handleUpdate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var req blogArticleWriteReq
	if err := json.NewDecoder(ctx.Request().Body).Decode(&req); err != nil || req.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var existing model.BlogArticle
	if err := s.db.First(&existing, req.ID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
		return nil
	}
	if !blogaccess.CanManage(existing.UserID, pd.UserID, pd.IsSiteAdmin) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "只能管理自己的文章"})
		return nil
	}
	a, msg := s.buildArticleFromReq(existing.UserID, existing.ID, &req, false)
	if msg != "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": msg})
		return nil
	}
	// preserve counters & created
	a.ID = existing.ID
	a.CreatedAt = existing.CreatedAt
	a.ViewCount = existing.ViewCount
	a.LikeCount = existing.LikeCount
	a.CommentCount = existing.CommentCount
	a.PublishedAt = existing.PublishedAt
	if a.PasswordHash == "" && !req.ClearPassword && existing.PasswordHash != "" &&
		blogaccess.NormalizeVisibility(a.Visibility) == blogaccess.VisibilityPassword &&
		strings.TrimSpace(req.Password) == "" {
		a.PasswordHash = existing.PasswordHash
	}
	if err := s.db.Save(a).Error; err != nil {
		if strings.Contains(err.Error(), "duplicate") || strings.Contains(err.Error(), "unique") {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "短链已被占用，请换一个"})
			return nil
		}
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "保存失败"})
		return nil
	}
	if req.OrgIDs != nil {
		_ = s.replaceOrgSync(a.ID, req.OrgIDs)
	}
	d := blogaccess.Evaluate(blogaccess.ArticleAccess{
		Visibility:  a.Visibility,
		OwnerID:     a.UserID,
		HasPassword: a.PasswordHash != "",
	}, pd.UserID, true)
	var author model.User
	_ = s.db.Select("id", "username", "name", "avatar").First(&author, a.UserID).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data":    s.articleToMap(a, &author, d, pd.UserID, true),
	})
	return nil
}

func (s *BlogService) buildArticleFromReq(userID, existingID uint, req *blogArticleWriteReq, isCreate bool) (*model.BlogArticle, string) {
	title := strings.TrimSpace(req.Title)
	if title == "" {
		return nil, "标题不能为空"
	}
	if utf8.RuneCountInString(title) > maxBlogTitle {
		return nil, "标题过长"
	}
	content := strings.ReplaceAll(req.Content, "\r\n", "\n")
	if strings.TrimSpace(content) == "" {
		return nil, "正文不能为空"
	}
	if len(content) > maxBlogContent {
		return nil, "正文过大，最大 512KB"
	}
	// 摘要与题解同步一致：不自动从正文截取，空着留给作者手填
	summary := strings.TrimSpace(req.Summary)
	if utf8.RuneCountInString(summary) > maxBlogSummary {
		return nil, "摘要过长"
	}
	cover := strings.TrimSpace(req.CoverURL)
	if len(cover) > maxBlogCover {
		return nil, "头图链接过长"
	}
	// no file upload — only http(s) links allowed when set
	if cover != "" && !strings.HasPrefix(cover, "http://") && !strings.HasPrefix(cover, "https://") {
		return nil, "头图请使用 http(s) 链接，暂不支持本地上传"
	}
	vis := blogaccess.NormalizeVisibility(req.Visibility)
	if !blogaccess.ValidVisibility(vis) {
		return nil, "可见性无效"
	}
	var pwHash string
	if vis == blogaccess.VisibilityPassword {
		pw := strings.TrimSpace(req.Password)
		if isCreate && pw == "" {
			return nil, "密码访问需要设置密码"
		}
		if pw != "" {
			h, err := hashBlogPassword(pw)
			if err != nil {
				return nil, "密码处理失败"
			}
			pwHash = h
		}
	} else if req.ClearPassword || isCreate {
		pwHash = ""
	}

	slug := strings.TrimSpace(req.Slug)
	if slug == "" {
		slug = slugifyTitle(title)
	}
	slug = strings.ToLower(slug)
	// sanitize slug
	var sb strings.Builder
	for _, r := range slug {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '-' {
			sb.WriteRune(r)
		}
	}
	slug = strings.Trim(sb.String(), "-")
	if slug == "" {
		s2, err := randomBlogSlug(10)
		if err != nil {
			return nil, "生成短链失败"
		}
		slug = s2
	}
	if utf8.RuneCountInString(slug) > maxBlogSlug {
		return nil, "短链过长"
	}

	// unique slug per user (exclude self on update)
	var n int64
	q := s.db.Model(&model.BlogArticle{}).Where("user_id = ? AND slug = ?", userID, slug)
	if existingID > 0 {
		q = q.Where("id <> ?", existingID)
	}
	q.Count(&n)
	if n > 0 {
		// auto suffix on create
		if isCreate {
			for i := 0; i < 6; i++ {
				suf, _ := randomBlogSlug(4)
				cand := slug + "-" + suf
				var n2 int64
				s.db.Model(&model.BlogArticle{}).Where("user_id = ? AND slug = ?", userID, cand).Count(&n2)
				if n2 == 0 {
					slug = cand
					break
				}
			}
		} else {
			return nil, "短链已被占用，请换一个"
		}
	}

	if req.CategoryID != nil && *req.CategoryID > 0 {
		var cat model.BlogCategory
		if err := s.db.Where("id = ? AND user_id = ?", *req.CategoryID, userID).First(&cat).Error; err != nil {
			return nil, "分类不存在"
		}
	}

	// recommend only meaningful for public
	recommend := req.Recommend
	if vis != blogaccess.VisibilityPublic {
		recommend = false
	}

	return &model.BlogArticle{
		UserID:            userID,
		Slug:              slug,
		Title:             title,
		Summary:           summary,
		Content:           content,
		CoverURL:          cover,
		Visibility:        vis,
		PasswordHash:      pwHash,
		Recommend:         recommend,
		SyncToMainProfile: req.SyncToMainProfile,
		CategoryID:        req.CategoryID,
	}, ""
}

func (s *BlogService) handleDelete(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var body struct {
		ID uint `json:"id"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil || body.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var a model.BlogArticle
	if err := s.db.First(&a, body.ID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
		return nil
	}
	if !blogaccess.CanManage(a.UserID, pd.UserID, pd.IsSiteAdmin) {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "只能删除自己的文章"})
		return nil
	}
	_ = s.db.Transaction(func(tx *gorm.DB) error {
		_ = tx.Where("article_id = ?", a.ID).Delete(&model.BlogArticleOrg{}).Error
		_ = tx.Where("article_id = ?", a.ID).Delete(&model.BlogComment{}).Error
		_ = tx.Where("article_id = ?", a.ID).Delete(&model.BlogLike{}).Error
		return tx.Delete(&a).Error
	})
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已删除"})
	return nil
}

func (s *BlogService) handleMine(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	page, pageSize := parsePage(ctx.Request())
	keyword := strings.TrimSpace(ctx.Request().URL.Query().Get("keyword"))
	q := s.db.Model(&model.BlogArticle{}).Where("user_id = ?", pd.UserID)
	if keyword != "" {
		like := "%" + keyword + "%"
		q = q.Where("title ILIKE ? OR summary ILIKE ?", like, like)
	}
	var total int64
	_ = q.Count(&total).Error
	var list []model.BlogArticle
	if err := q.Order("id DESC").Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "加载失败"})
		return nil
	}
	var author model.User
	_ = s.db.Select("id", "username", "name", "avatar").First(&author, pd.UserID).Error
	out := make([]map[string]interface{}, 0, len(list))
	for i := range list {
		d := blogaccess.Evaluate(blogaccess.ArticleAccess{
			Visibility:  list[i].Visibility,
			OwnerID:     list[i].UserID,
			HasPassword: list[i].PasswordHash != "",
		}, pd.UserID, true)
		out = append(out, s.articleToMap(&list[i], &author, d, pd.UserID, false))
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"list":     out,
			"total":    total,
			"page":     page,
			"pageSize": pageSize,
		},
	})
	return nil
}

// ---------- recommend ----------

func (s *BlogService) handleRecommend(ctx khttp.Context) error {
	page, pageSize := parsePage(ctx.Request())
	viewer := blogViewerID(ctx)
	q := s.db.Model(&model.BlogArticle{}).
		Where("visibility = ? AND recommend = ?", blogaccess.VisibilityPublic, true)
	var total int64
	_ = q.Count(&total).Error
	var list []model.BlogArticle
	if err := q.Order("COALESCE(published_at, created_at) DESC").
		Offset((page - 1) * pageSize).Limit(pageSize).
		Find(&list).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "加载失败"})
		return nil
	}
	out := s.batchMapArticles(list, viewer)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"list":     out,
			"total":    total,
			"page":     page,
			"pageSize": pageSize,
		},
	})
	return nil
}

// ---------- plaza (main-site public feed) ----------

func (s *BlogService) handlePlaza(ctx khttp.Context) error {
	page, pageSize := parsePage(ctx.Request())
	// Prefer denser default for plaza cards
	if ctx.Request().URL.Query().Get("pageSize") == "" {
		pageSize = 12
	}
	viewer := blogViewerID(ctx)
	keyword := strings.TrimSpace(ctx.Request().URL.Query().Get("keyword"))
	sort := strings.ToLower(strings.TrimSpace(ctx.Request().URL.Query().Get("sort")))
	if sort == "" {
		sort = "latest"
	}

	q := s.db.Model(&model.BlogArticle{}).
		Where("visibility = ?", blogaccess.VisibilityPublic)
	if sort == "recommend" {
		q = q.Where("recommend = ?", true)
	}
	if keyword != "" {
		like := "%" + keyword + "%"
		q = q.Where("title ILIKE ? OR summary ILIKE ?", like, like)
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "加载失败"})
		return nil
	}

	switch sort {
	case "hot":
		q = q.Order("view_count DESC, like_count DESC, COALESCE(published_at, created_at) DESC")
	case "recommend", "latest":
		q = q.Order("COALESCE(published_at, created_at) DESC")
	default:
		writeJSON(ctx.Response(), 400, map[string]interface{}{
			"code":    1,
			"message": "sort 须为 latest|hot|recommend",
		})
		return nil
	}

	var list []model.BlogArticle
	if err := q.Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "加载失败"})
		return nil
	}
	out := s.batchMapArticles(list, viewer)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"list":     out,
			"total":    total,
			"page":     page,
			"pageSize": pageSize,
		},
	})
	return nil
}

// ---------- active authors (plaza side rail) ----------

func (s *BlogService) handleAuthors(ctx khttp.Context) error {
	page, pageSize := parsePage(ctx.Request())
	if pageSize > 30 {
		pageSize = 30
	}
	if ctx.Request().URL.Query().Get("pageSize") == "" {
		pageSize = 12
	}
	keyword := strings.TrimSpace(ctx.Request().URL.Query().Get("keyword"))

	// Aggregate public articles per author, ordered by last publish time.
	type aggRow struct {
		UserID          uint
		ArticleCount    int64
		LastPublishedAt *time.Time
	}
	base := s.db.Model(&model.BlogArticle{}).
		Select("user_id, COUNT(*) as article_count, MAX(COALESCE(published_at, created_at)) as last_published_at").
		Where("visibility = ?", blogaccess.VisibilityPublic).
		Group("user_id")

	// Optional name/username filter via join
	var total int64
	countQ := s.db.Table("(?) as author_agg", base)
	if keyword != "" {
		like := "%" + keyword + "%"
		countQ = s.db.Table("(?) as author_agg", base).
			Joins("JOIN users ON users.id = author_agg.user_id").
			Where("users.username ILIKE ? OR users.name ILIKE ?", like, like)
	}
	if err := countQ.Count(&total).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "加载失败"})
		return nil
	}

	var aggs []aggRow
	listQ := s.db.Table("(?) as author_agg", base).
		Select("author_agg.user_id, author_agg.article_count, author_agg.last_published_at")
	if keyword != "" {
		like := "%" + keyword + "%"
		listQ = listQ.Joins("JOIN users ON users.id = author_agg.user_id").
			Where("users.username ILIKE ? OR users.name ILIKE ?", like, like)
	}
	if err := listQ.Order("author_agg.last_published_at DESC NULLS LAST").
		Offset((page - 1) * pageSize).Limit(pageSize).
		Scan(&aggs).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "加载失败"})
		return nil
	}

	ids := make([]uint, 0, len(aggs))
	for _, a := range aggs {
		ids = append(ids, a.UserID)
	}
	authors := map[uint]model.User{}
	if len(ids) > 0 {
		var us []model.User
		_ = s.db.Select("id", "username", "name", "avatar").Where("id IN ?", ids).Find(&us).Error
		for _, u := range us {
			authors[u.ID] = u
		}
	}

	// latest public title per author (one query)
	latestTitle := map[uint]string{}
	if len(ids) > 0 {
		type titleRow struct {
			UserID uint
			Title  string
		}
		var titles []titleRow
		// DISTINCT ON is Postgres-specific; project uses postgres.
		_ = s.db.Raw(`
			SELECT DISTINCT ON (user_id) user_id, title
			FROM blog_articles
			WHERE visibility = ? AND user_id IN ?
			ORDER BY user_id, COALESCE(published_at, created_at) DESC
		`, blogaccess.VisibilityPublic, ids).Scan(&titles).Error
		for _, t := range titles {
			latestTitle[t.UserID] = t.Title
		}
	}

	out := make([]map[string]interface{}, 0, len(aggs))
	for _, a := range aggs {
		u := authors[a.UserID]
		item := map[string]interface{}{
			"id":            a.UserID,
			"username":      u.Username,
			"name":          u.Name,
			"avatar":        u.Avatar,
			"articleCount":  a.ArticleCount,
			"latestTitle":   latestTitle[a.UserID],
		}
		if a.LastPublishedAt != nil {
			item["lastPublishedAt"] = a.LastPublishedAt.Unix()
		}
		out = append(out, item)
	}

	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"list":     out,
			"total":    total,
			"page":     page,
			"pageSize": pageSize,
		},
	})
	return nil
}

// batchMapArticles loads authors once and maps list items without body.
func (s *BlogService) batchMapArticles(list []model.BlogArticle, viewer uint) []map[string]interface{} {
	ids := make([]uint, 0, len(list))
	seen := map[uint]struct{}{}
	for _, a := range list {
		if _, ok := seen[a.UserID]; !ok {
			seen[a.UserID] = struct{}{}
			ids = append(ids, a.UserID)
		}
	}
	authors := map[uint]model.User{}
	if len(ids) > 0 {
		var us []model.User
		_ = s.db.Select("id", "username", "name", "avatar").Where("id IN ?", ids).Find(&us).Error
		for _, u := range us {
			authors[u.ID] = u
		}
	}
	out := make([]map[string]interface{}, 0, len(list))
	for i := range list {
		u := authors[list[i].UserID]
		d := blogaccess.Evaluate(blogaccess.ArticleAccess{
			Visibility: list[i].Visibility,
			OwnerID:    list[i].UserID,
		}, viewer, false)
		out = append(out, s.articleToMap(&list[i], &u, d, viewer, false))
	}
	return out
}

// ---------- analytics ----------

func (s *BlogService) handleAnalytics(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	type row struct {
		Views    int64
		Likes    int64
		Comments int64
		Articles int64
	}
	var r row
	_ = s.db.Model(&model.BlogArticle{}).
		Select("COALESCE(SUM(view_count),0) as views, COALESCE(SUM(like_count),0) as likes, COALESCE(SUM(comment_count),0) as comments, COUNT(*) as articles").
		Where("user_id = ?", pd.UserID).
		Scan(&r).Error

	// top articles by views
	var top []model.BlogArticle
	_ = s.db.Where("user_id = ?", pd.UserID).Order("view_count DESC").Limit(10).Find(&top).Error
	topOut := make([]map[string]interface{}, 0, len(top))
	for _, a := range top {
		topOut = append(topOut, map[string]interface{}{
			"id":           a.ID,
			"slug":         a.Slug,
			"title":        a.Title,
			"viewCount":    a.ViewCount,
			"likeCount":    a.LikeCount,
			"commentCount": a.CommentCount,
			"visibility":   a.Visibility,
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code":    0,
		"message": "success",
		"data": map[string]interface{}{
			"totalArticles": r.Articles,
			"totalViews":    r.Views,
			"totalLikes":    r.Likes,
			"totalComments": r.Comments,
			"topArticles":   topOut,
		},
	})
	return nil
}

// ---------- categories ----------

func (s *BlogService) handleListCategoriesPublic(ctx khttp.Context) error {
	username := strings.TrimSpace(ctx.Request().URL.Query().Get("username"))
	if username == "" {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "缺少用户名"})
		return nil
	}
	u, err := s.findUserByUsername(username)
	if err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "用户不存在"})
		return nil
	}
	// 访客侧不强制创建默认分类（避免写路径）；仅列出已有
	var list []model.BlogCategory
	_ = s.db.Where("user_id = ?", u.ID).Order("is_default DESC, sort_order ASC, id ASC").Find(&list).Error
	out := make([]map[string]interface{}, 0, len(list))
	for _, c := range list {
		var cnt int64
		s.db.Model(&model.BlogArticle{}).Where("category_id = ? AND visibility = ?", c.ID, blogaccess.VisibilityPublic).Count(&cnt)
		out = append(out, map[string]interface{}{
			"id": c.ID, "name": c.Name, "sortOrder": c.SortOrder, "articleCount": cnt, "isDefault": c.IsDefault,
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "success", "list": out})
	return nil
}

func (s *BlogService) handleCategoryMine(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	// 管理页确保默认分类存在
	_, _ = blogsync.EnsureDefaultCategory(s.db, pd.UserID)
	var list []model.BlogCategory
	_ = s.db.Where("user_id = ?", pd.UserID).Order("is_default DESC, sort_order ASC, id ASC").Find(&list).Error
	out := make([]map[string]interface{}, 0, len(list))
	for _, c := range list {
		var cnt int64
		s.db.Model(&model.BlogArticle{}).Where("category_id = ?", c.ID).Count(&cnt)
		out = append(out, map[string]interface{}{
			"id": c.ID, "name": c.Name, "sortOrder": c.SortOrder, "articleCount": cnt, "isDefault": c.IsDefault,
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "success", "list": out})
	return nil
}

func (s *BlogService) handleCategoryCreate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var body struct {
		Name      string `json:"name"`
		SortOrder int    `json:"sortOrder"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	name := strings.TrimSpace(body.Name)
	if name == "" || utf8.RuneCountInString(name) > 64 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "分类名无效"})
		return nil
	}
	c := model.BlogCategory{UserID: pd.UserID, Name: name, SortOrder: body.SortOrder, IsDefault: false}
	if err := s.db.Create(&c).Error; err != nil {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "创建失败，可能重名"})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success",
		"data": map[string]interface{}{"id": c.ID, "name": c.Name, "sortOrder": c.SortOrder, "isDefault": c.IsDefault},
	})
	return nil
}

func (s *BlogService) handleCategoryUpdate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var body struct {
		ID        uint   `json:"id"`
		Name      string `json:"name"`
		SortOrder *int   `json:"sortOrder"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil || body.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var c model.BlogCategory
	if err := s.db.Where("id = ? AND user_id = ?", body.ID, pd.UserID).First(&c).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "分类不存在"})
		return nil
	}
	if n := strings.TrimSpace(body.Name); n != "" {
		c.Name = n
	}
	if body.SortOrder != nil {
		c.SortOrder = *body.SortOrder
	}
	if err := s.db.Save(&c).Error; err != nil {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "保存失败"})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "success", "data": map[string]interface{}{
		"id": c.ID, "name": c.Name, "sortOrder": c.SortOrder, "isDefault": c.IsDefault,
	}})
	return nil
}

func (s *BlogService) handleCategoryDelete(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var body struct {
		ID uint `json:"id"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil || body.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var c model.BlogCategory
	if err := s.db.Where("id = ? AND user_id = ?", body.ID, pd.UserID).First(&c).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "分类不存在"})
		return nil
	}
	if c.IsDefault {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "默认分类不能删除"})
		return nil
	}
	res := s.db.Where("id = ? AND user_id = ?", body.ID, pd.UserID).Delete(&model.BlogCategory{})
	if res.RowsAffected == 0 {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "分类不存在"})
		return nil
	}
	// 非默认分类文章改挂到默认分类
	if defID, err := blogsync.EnsureDefaultCategory(s.db, pd.UserID); err == nil && defID > 0 {
		_ = s.db.Model(&model.BlogArticle{}).Where("category_id = ?", body.ID).Update("category_id", defID).Error
	} else {
		_ = s.db.Model(&model.BlogArticle{}).Where("category_id = ?", body.ID).Update("category_id", nil).Error
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已删除"})
	return nil
}

// ---------- comments ----------

func (s *BlogService) handleListComments(ctx khttp.Context) error {
	articleID, _ := strconv.ParseUint(ctx.Request().URL.Query().Get("articleId"), 10, 64)
	if articleID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "缺少 articleId"})
		return nil
	}
	// ensure article is at least meta-visible
	var a model.BlogArticle
	if err := s.db.First(&a, articleID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
		return nil
	}
	viewer := blogViewerID(ctx)
	d := blogaccess.Evaluate(blogaccess.ArticleAccess{
		Visibility:  a.Visibility,
		OwnerID:     a.UserID,
		HasPassword: a.PasswordHash != "",
	}, viewer, false)
	// comments only when meta visible (public/password teaser/owner)
	if !d.CanSeeMeta {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
		return nil
	}
	page, pageSize := parsePage(ctx.Request())
	var total int64
	s.db.Model(&model.BlogComment{}).Where("article_id = ?", articleID).Count(&total)
	var list []model.BlogComment
	_ = s.db.Where("article_id = ?", articleID).Order("id ASC").
		Offset((page - 1) * pageSize).Limit(pageSize).Find(&list).Error
	uids := map[uint]struct{}{}
	for _, c := range list {
		uids[c.UserID] = struct{}{}
	}
	idList := make([]uint, 0, len(uids))
	for id := range uids {
		idList = append(idList, id)
	}
	users := map[uint]model.User{}
	if len(idList) > 0 {
		var us []model.User
		_ = s.db.Select("id", "username", "name", "avatar").Where("id IN ?", idList).Find(&us).Error
		for _, u := range us {
			users[u.ID] = u
		}
	}
	out := make([]map[string]interface{}, 0, len(list))
	for _, c := range list {
		u := users[c.UserID]
		out = append(out, map[string]interface{}{
			"id": c.ID, "articleId": c.ArticleID, "parentId": c.ParentID,
			"content": c.Content, "createdAt": c.CreatedAt.Unix(),
			"userId": c.UserID,
			"author": map[string]interface{}{
				"id": u.ID, "username": u.Username, "name": u.Name, "avatar": u.Avatar,
			},
		})
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success",
		"data": map[string]interface{}{"list": out, "total": total, "page": page, "pageSize": pageSize},
	})
	return nil
}

func (s *BlogService) handleCommentCreate(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var body struct {
		ArticleID uint   `json:"articleId"`
		ParentID  uint   `json:"parentId"`
		Content   string `json:"content"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil || body.ArticleID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	content := strings.TrimSpace(body.Content)
	if content == "" || utf8.RuneCountInString(content) > maxCommentLen {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "评论内容无效"})
		return nil
	}
	var a model.BlogArticle
	if err := s.db.First(&a, body.ArticleID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
		return nil
	}
	// only comment when body is readable without password OR viewer is owner
	// (password articles: must unlock first — we allow comment if public or owner)
	d := blogaccess.Evaluate(blogaccess.ArticleAccess{
		Visibility:  a.Visibility,
		OwnerID:     a.UserID,
		HasPassword: a.PasswordHash != "",
	}, pd.UserID, false)
	if !d.CanSeeBody && blogaccess.NormalizeVisibility(a.Visibility) != blogaccess.VisibilityPassword {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "无法评论此文章"})
		return nil
	}
	if blogaccess.NormalizeVisibility(a.Visibility) == blogaccess.VisibilityPrivate && pd.UserID != a.UserID {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "无法评论此文章"})
		return nil
	}
	if body.ParentID > 0 {
		var parent model.BlogComment
		if err := s.db.Where("id = ? AND article_id = ?", body.ParentID, body.ArticleID).First(&parent).Error; err != nil {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "父评论不存在"})
			return nil
		}
	}
	c := model.BlogComment{
		ArticleID: body.ArticleID,
		UserID:    pd.UserID,
		ParentID:  body.ParentID,
		Content:   content,
	}
	if err := s.db.Create(&c).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "发表失败"})
		return nil
	}
	_ = s.db.Model(&model.BlogArticle{}).Where("id = ?", a.ID).
		UpdateColumn("comment_count", gorm.Expr("comment_count + 1")).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success",
		"data": map[string]interface{}{
			"id": c.ID, "articleId": c.ArticleID, "parentId": c.ParentID,
			"content": c.Content, "createdAt": c.CreatedAt.Unix(), "userId": c.UserID,
		},
	})
	return nil
}

func (s *BlogService) handleCommentDelete(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var body struct {
		ID uint `json:"id"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil || body.ID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var c model.BlogComment
	if err := s.db.First(&c, body.ID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "评论不存在"})
		return nil
	}
	var a model.BlogArticle
	_ = s.db.First(&a, c.ArticleID).Error
	if c.UserID != pd.UserID && a.UserID != pd.UserID && !pd.IsSiteAdmin {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "无权删除"})
		return nil
	}
	if err := s.db.Delete(&c).Error; err != nil {
		writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "删除失败"})
		return nil
	}
	_ = s.db.Model(&model.BlogArticle{}).Where("id = ? AND comment_count > 0", c.ArticleID).
		UpdateColumn("comment_count", gorm.Expr("comment_count - 1")).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "已删除"})
	return nil
}

// ---------- likes ----------

func (s *BlogService) handleLikeToggle(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var body struct {
		ArticleID uint `json:"articleId"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil || body.ArticleID == 0 {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	var a model.BlogArticle
	if err := s.db.First(&a, body.ArticleID).Error; err != nil {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
		return nil
	}
	d := blogaccess.Evaluate(blogaccess.ArticleAccess{
		Visibility: a.Visibility, OwnerID: a.UserID, HasPassword: a.PasswordHash != "",
	}, pd.UserID, false)
	if !d.CanSeeMeta {
		writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "文章不存在"})
		return nil
	}
	var existing model.BlogLike
	err := s.db.Where("article_id = ? AND user_id = ?", body.ArticleID, pd.UserID).First(&existing).Error
	liked := false
	if err == nil {
		_ = s.db.Delete(&existing).Error
		_ = s.db.Model(&model.BlogArticle{}).Where("id = ? AND like_count > 0", body.ArticleID).
			UpdateColumn("like_count", gorm.Expr("like_count - 1")).Error
		liked = false
	} else {
		_ = s.db.Create(&model.BlogLike{ArticleID: body.ArticleID, UserID: pd.UserID}).Error
		_ = s.db.Model(&model.BlogArticle{}).Where("id = ?", body.ArticleID).
			UpdateColumn("like_count", gorm.Expr("like_count + 1")).Error
		liked = true
	}
	var likeCount int
	_ = s.db.Model(&model.BlogArticle{}).Select("like_count").Where("id = ?", body.ArticleID).Scan(&likeCount).Error
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success",
		"data": map[string]interface{}{"liked": liked, "likeCount": likeCount},
	})
	return nil
}

// ---------- theme ----------

const (
	blogThemeChirpy = "chirpy"
	blogThemeSimple = "simple"
	blogThemeMizuki = "mizuki"
	maxSocialLinks  = 12
	maxSocialURL    = 512
	maxSubtitle     = 200
)

type blogSocialLink struct {
	Type  string `json:"type"`
	URL   string `json:"url"`
	Label string `json:"label,omitempty"`
}

type blogSiteConfigView struct {
	ThemeID     string           `json:"themeId"`
	Subtitle    string           `json:"subtitle"`
	SocialLinks []blogSocialLink `json:"socialLinks"`
}

func normalizeThemeID(raw string) string {
	switch strings.ToLower(strings.TrimSpace(raw)) {
	case blogThemeSimple:
		return blogThemeSimple
	case blogThemeMizuki:
		return blogThemeMizuki
	default:
		return blogThemeChirpy
	}
}

func parseSocialLinksJSON(raw string) []blogSocialLink {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return []blogSocialLink{}
	}
	var list []blogSocialLink
	if err := json.Unmarshal([]byte(raw), &list); err != nil {
		return []blogSocialLink{}
	}
	out := make([]blogSocialLink, 0, len(list))
	for _, l := range list {
		t := strings.ToLower(strings.TrimSpace(l.Type))
		u := strings.TrimSpace(l.URL)
		if t == "" || u == "" {
			continue
		}
		if len(u) > maxSocialURL {
			u = u[:maxSocialURL]
		}
		label := strings.TrimSpace(l.Label)
		if utf8.RuneCountInString(label) > 32 {
			label = string([]rune(label)[:32])
		}
		out = append(out, blogSocialLink{Type: t, URL: u, Label: label})
		if len(out) >= maxSocialLinks {
			break
		}
	}
	return out
}

func (s *BlogService) loadSiteConfig(userID uint) blogSiteConfigView {
	view := blogSiteConfigView{
		ThemeID:     blogThemeChirpy,
		Subtitle:    "",
		SocialLinks: []blogSocialLink{},
	}
	if userID == 0 {
		return view
	}
	var cfg model.BlogSiteConfig
	if err := s.db.Where("user_id = ?", userID).First(&cfg).Error; err != nil {
		return view
	}
	view.ThemeID = normalizeThemeID(cfg.ThemeID)
	view.Subtitle = strings.TrimSpace(cfg.Subtitle)
	view.SocialLinks = parseSocialLinksJSON(cfg.SocialLinks)
	return view
}

func (s *BlogService) themeEnabledFor(userID uint) bool {
	var global model.BlogThemeFlag
	globalAll := false
	if err := s.db.Where("user_id = 0").First(&global).Error; err == nil {
		globalAll = global.Enabled
	}
	var per model.BlogThemeFlag
	if err := s.db.Where("user_id = ?", userID).First(&per).Error; err == nil {
		v := per.Enabled
		return blogaccess.ThemeEnabled(globalAll, &v)
	}
	return blogaccess.ThemeEnabled(globalAll, nil)
}

func (s *BlogService) handleThemeStatus(ctx khttp.Context) error {
	username := strings.TrimSpace(ctx.Request().URL.Query().Get("username"))
	var userID uint
	if username != "" {
		u, err := s.findUserByUsername(username)
		if err != nil {
			writeJSON(ctx.Response(), 404, map[string]interface{}{"code": 1, "message": "用户不存在"})
			return nil
		}
		userID = u.ID
	} else {
		userID = blogViewerID(ctx)
	}
	enabled := false
	if userID > 0 {
		enabled = s.themeEnabledFor(userID)
	}
	siteCfg := s.loadSiteConfig(userID)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success",
		"data": map[string]interface{}{
			"enabled":     enabled,
			"themeId":     siteCfg.ThemeID,
			"subtitle":    siteCfg.Subtitle,
			"socialLinks": siteCfg.SocialLinks,
			"customTheme": nil, // reserved extension point
		},
	})
	return nil
}

// handleThemeConfigSave owner saves theme id + subtitle + social links.
func (s *BlogService) handleThemeConfigSave(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || pd.UserID == 0 {
		writeJSON(ctx.Response(), 401, map[string]interface{}{"code": 1, "message": "请先登录"})
		return nil
	}
	var body struct {
		ThemeID     string           `json:"themeId"`
		Subtitle    string           `json:"subtitle"`
		SocialLinks []blogSocialLink `json:"socialLinks"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	themeID := normalizeThemeID(body.ThemeID)
	subtitle := strings.TrimSpace(body.Subtitle)
	if utf8.RuneCountInString(subtitle) > maxSubtitle {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "副标题过长"})
		return nil
	}
	// re-validate social links via JSON round-trip
	rawLinks, _ := json.Marshal(body.SocialLinks)
	links := parseSocialLinksJSON(string(rawLinks))
	// only allow http(s) / mailto for urls
	clean := make([]blogSocialLink, 0, len(links))
	for _, l := range links {
		u := l.URL
		lu := strings.ToLower(u)
		if strings.HasPrefix(lu, "javascript:") || strings.HasPrefix(lu, "data:") {
			continue
		}
		if l.Type == "email" {
			if !strings.HasPrefix(lu, "mailto:") && !strings.Contains(u, "@") {
				continue
			}
			if !strings.HasPrefix(lu, "mailto:") {
				u = "mailto:" + u
			}
		} else if !strings.HasPrefix(lu, "http://") && !strings.HasPrefix(lu, "https://") {
			continue
		}
		clean = append(clean, blogSocialLink{Type: l.Type, URL: u, Label: l.Label})
	}
	linksJSON, _ := json.Marshal(clean)

	var cfg model.BlogSiteConfig
	err := s.db.Where("user_id = ?", pd.UserID).First(&cfg).Error
	if err != nil {
		cfg = model.BlogSiteConfig{
			UserID:      pd.UserID,
			ThemeID:     themeID,
			Subtitle:    subtitle,
			SocialLinks: string(linksJSON),
		}
		if err := s.db.Create(&cfg).Error; err != nil {
			writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "保存失败"})
			return nil
		}
	} else {
		cfg.ThemeID = themeID
		cfg.Subtitle = subtitle
		cfg.SocialLinks = string(linksJSON)
		if err := s.db.Save(&cfg).Error; err != nil {
			writeJSON(ctx.Response(), 500, map[string]interface{}{"code": 1, "message": "保存失败"})
			return nil
		}
	}
	view := s.loadSiteConfig(pd.UserID)
	writeJSON(ctx.Response(), 200, map[string]interface{}{
		"code": 0, "message": "success",
		"data": map[string]interface{}{
			"themeId":     view.ThemeID,
			"subtitle":    view.Subtitle,
			"socialLinks": view.SocialLinks,
		},
	})
	return nil
}

func (s *BlogService) handleThemeEnable(ctx khttp.Context) error {
	pd := auth.GetCurrentUser(ctx)
	if pd == nil || !pd.IsSiteAdmin {
		writeJSON(ctx.Response(), 403, map[string]interface{}{"code": 1, "message": "仅站点管理员可操作"})
		return nil
	}
	var body struct {
		// Mode: user | batch | all
		Mode    string `json:"mode"`
		UserID  uint   `json:"userId"`
		UserIDs []uint `json:"userIds"`
		Enabled bool   `json:"enabled"`
	}
	if err := json.NewDecoder(ctx.Request().Body).Decode(&body); err != nil {
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "参数错误"})
		return nil
	}
	mode := strings.ToLower(strings.TrimSpace(body.Mode))
	switch mode {
	case "all":
		var g model.BlogThemeFlag
		err := s.db.Where("user_id = 0").First(&g).Error
		if err != nil {
			g = model.BlogThemeFlag{UserID: 0, Enabled: body.Enabled}
			_ = s.db.Create(&g).Error
		} else {
			g.Enabled = body.Enabled
			_ = s.db.Save(&g).Error
		}
	case "user":
		if body.UserID == 0 {
			writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "缺少 userId"})
			return nil
		}
		s.upsertThemeFlag(body.UserID, body.Enabled)
	case "batch":
		for _, id := range body.UserIDs {
			if id > 0 {
				s.upsertThemeFlag(id, body.Enabled)
			}
		}
	default:
		writeJSON(ctx.Response(), 400, map[string]interface{}{"code": 1, "message": "mode 须为 user|batch|all"})
		return nil
	}
	writeJSON(ctx.Response(), 200, map[string]interface{}{"code": 0, "message": "success"})
	return nil
}

func (s *BlogService) upsertThemeFlag(userID uint, enabled bool) {
	var f model.BlogThemeFlag
	err := s.db.Where("user_id = ?", userID).First(&f).Error
	if err != nil {
		_ = s.db.Create(&model.BlogThemeFlag{UserID: userID, Enabled: enabled}).Error
		return
	}
	f.Enabled = enabled
	_ = s.db.Save(&f).Error
}
