package model

import "time"

// Blog visibility constants (mirror blogaccess).
const (
	BlogVisPublic   = "public"
	BlogVisPrivate  = "private"
	BlogVisPassword = "password"
)

// BlogArticle is the single shared article entity (blog shell + main-site surfaces).
type BlogArticle struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time

	UserID   uint   `gorm:"not null;index:idx_blog_user_created,priority:1;uniqueIndex:idx_blog_user_slug,priority:1;comment:作者"`
	Slug     string `gorm:"size:96;not null;uniqueIndex:idx_blog_user_slug,priority:2;comment:作者内短链"`
	Title    string `gorm:"size:200;not null;comment:标题"`
	Summary  string `gorm:"size:500;comment:摘要"`
	Content  string `gorm:"type:text;not null;comment:Markdown 正文"`
	CoverURL string `gorm:"size:1024;comment:头图外链"`

	// Visibility: public | private | password
	Visibility   string `gorm:"size:16;not null;default:public;index;comment:可见性"`
	PasswordHash string `gorm:"size:255;comment:访问密码 bcrypt"`

	// Recommend: show on main-site recommend page when public.
	Recommend bool `gorm:"not null;default:false;index;comment:主站推荐"`

	// SyncToMainProfile: allow main-site profile activity to surface this article.
	SyncToMainProfile bool `gorm:"not null;default:false;comment:同步到主站资料动态"`

	CategoryID *uint `gorm:"index;comment:分类"`

	// SourceSolutionID: when set, this article was synced from a main-site problem solution.
	// Unique so one solution maps to at most one blog post.
	SourceSolutionID *uint `gorm:"uniqueIndex:idx_blog_source_solution;comment:主站题解id"`

	// Denormalized counters for owner analytics.
	ViewCount    int `gorm:"not null;default:0;comment:阅读数"`
	LikeCount    int `gorm:"not null;default:0;comment:点赞数"`
	CommentCount int `gorm:"not null;default:0;comment:评论数"`

	PublishedAt *time.Time `gorm:"index:idx_blog_user_created,priority:2;comment:发布时间"`
}

func (BlogArticle) TableName() string { return "blog_articles" }

// BlogCategory is a per-user article category.
type BlogCategory struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	UserID    uint   `gorm:"not null;uniqueIndex:idx_blog_cat_user_name,priority:1;comment:作者"`
	Name      string `gorm:"size:64;not null;uniqueIndex:idx_blog_cat_user_name,priority:2;comment:分类名"`
	SortOrder int    `gorm:"not null;default:0;comment:排序"`
	// IsDefault: 每用户至多一个（业务保证）；主站题解同步到此分类。不可删除。
	IsDefault bool `gorm:"not null;default:false;comment:默认分类"`
}

func (BlogCategory) TableName() string { return "blog_categories" }

// BlogArticleOrg marks which orgs an article is synced to.
// Product rule: private org sync auto-includes public domain (enforced in service).
type BlogArticleOrg struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	ArticleID uint `gorm:"not null;uniqueIndex:idx_blog_art_org,priority:1;comment:文章"`
	OrgID     uint `gorm:"not null;uniqueIndex:idx_blog_art_org,priority:2;index;comment:组织"`
}

func (BlogArticleOrg) TableName() string { return "blog_article_orgs" }

// BlogComment is a top-level or reply comment on an article.
type BlogComment struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	ArticleID uint   `gorm:"not null;index:idx_blog_cmt_art,priority:1;comment:文章"`
	UserID    uint   `gorm:"not null;index;comment:作者"`
	ParentID  uint   `gorm:"not null;default:0;comment:父评论"`
	Content   string `gorm:"type:text;not null;comment:内容"`
}

func (BlogComment) TableName() string { return "blog_comments" }

// BlogLike is one like per user per article.
type BlogLike struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	ArticleID uint `gorm:"not null;uniqueIndex:idx_blog_like_art_user,priority:1;comment:文章"`
	UserID    uint `gorm:"not null;uniqueIndex:idx_blog_like_art_user,priority:2;comment:用户"`
}

func (BlogLike) TableName() string { return "blog_likes" }

// BlogThemeFlag stores custom-theme enablement.
// UserID=0 row holds the global "all users" flag (Enabled=true means open for everyone).
// Per-user rows override global when present.
type BlogThemeFlag struct {
	ID        uint `gorm:"primaryKey"`
	CreatedAt time.Time
	UpdatedAt time.Time
	// UserID 0 = global-all flag
	UserID  uint `gorm:"not null;uniqueIndex;comment:0=全局 否则用户"`
	Enabled bool `gorm:"not null;default:false;comment:是否开放自定义主题"`
}

func (BlogThemeFlag) TableName() string { return "blog_theme_flags" }
