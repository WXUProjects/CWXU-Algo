package dal

import (
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"

	"gorm.io/gorm"
)

// BulletinDal 公告数据操作模块
type BulletinDal struct {
	db *gorm.DB
}

func NewBulletinDal(data *data.Data) *BulletinDal {
	return &BulletinDal{db: data.DB}
}

func (d *BulletinDal) Create(bulletin *model.Bulletin) error {
	if bulletin.Scope == "" {
		bulletin.Scope = model.BulletinScopeSite
	}
	return d.db.Create(bulletin).Error
}

func (d *BulletinDal) Update(id int64, updates map[string]interface{}) error {
	return d.db.Model(&model.Bulletin{}).Where("id = ?", id).Updates(updates).Error
}

func (d *BulletinDal) Delete(id int64) error {
	return d.db.Delete(&model.Bulletin{}, id).Error
}

func (d *BulletinDal) GetById(id int64) (*model.Bulletin, error) {
	var bulletin model.Bulletin
	err := d.db.First(&bulletin, id).Error
	if err != nil {
		return nil, err
	}
	return &bulletin, nil
}

// applyScopeFilter 按 scope 过滤：
//   - scope=site：仅站点公告
//   - scope=org：仅指定组织公告（orgID 必须 >0）
//   - 空：全站 ∪ 指定组织（orgID=0 时仅全站）
func applyScopeFilter(q *gorm.DB, scope string, orgID uint) *gorm.DB {
	switch scope {
	case model.BulletinScopeSite:
		return q.Where("scope = ? OR scope = '' OR scope IS NULL", model.BulletinScopeSite)
	case model.BulletinScopeOrg:
		if orgID == 0 {
			// 无组织上下文时不返回任何组织公告
			return q.Where("1 = 0")
		}
		return q.Where("scope = ? AND org_id = ?", model.BulletinScopeOrg, orgID)
	default:
		if orgID > 0 {
			return q.Where(
				"scope = ? OR scope = '' OR scope IS NULL OR (scope = ? AND org_id = ?)",
				model.BulletinScopeSite, model.BulletinScopeOrg, orgID,
			)
		}
		return q.Where("scope = ? OR scope = '' OR scope IS NULL", model.BulletinScopeSite)
	}
}

// listOrder 站点公告优先，其次置顶，再按创建时间倒序
func listOrder(q *gorm.DB) *gorm.DB {
	// scope=site（含空/NULL）排前；置顶其次；新创建优先
	return q.Order(`
		CASE
			WHEN scope = 'site' OR scope = '' OR scope IS NULL THEN 0
			ELSE 1
		END ASC,
		is_pinned DESC,
		created_at DESC
	`)
}

// List 分页列表
func (d *BulletinDal) List(page, pageSize int64, orgID uint, scope string) ([]model.Bulletin, int64, error) {
	var bulletins []model.Bulletin
	var total int64

	q := applyScopeFilter(d.db.Model(&model.Bulletin{}), scope, orgID)
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	lq := applyScopeFilter(d.db.Model(&model.Bulletin{}), scope, orgID)
	lq = listOrder(lq).Offset(int(offset)).Limit(int(pageSize))
	if err := lq.Find(&bulletins).Error; err != nil {
		return nil, 0, err
	}
	return bulletins, total, nil
}
