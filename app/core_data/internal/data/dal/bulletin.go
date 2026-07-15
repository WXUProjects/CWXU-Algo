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

// List 分页：全站公告 ∪ 指定组织公告
func (d *BulletinDal) List(page, pageSize int64, orgID uint) ([]model.Bulletin, int64, error) {
	var bulletins []model.Bulletin
	var total int64

	q := d.db.Model(&model.Bulletin{})
	if orgID > 0 {
		q = q.Where("scope = ? OR (scope = ? AND org_id = ?)", model.BulletinScopeSite, model.BulletinScopeOrg, orgID)
	} else {
		// 未登录/无 org：仅全站
		q = q.Where("scope = ? OR scope = '' OR scope IS NULL", model.BulletinScopeSite)
	}

	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	lq := d.db.Order("is_pinned DESC, created_at DESC").Offset(int(offset)).Limit(int(pageSize))
	if orgID > 0 {
		lq = lq.Where("scope = ? OR (scope = ? AND org_id = ?)", model.BulletinScopeSite, model.BulletinScopeOrg, orgID)
	} else {
		lq = lq.Where("scope = ? OR scope = '' OR scope IS NULL", model.BulletinScopeSite)
	}
	if err := lq.Find(&bulletins).Error; err != nil {
		return nil, 0, err
	}
	return bulletins, total, nil
}
