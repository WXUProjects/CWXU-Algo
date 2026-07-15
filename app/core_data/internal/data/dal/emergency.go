package dal

import (
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"

	"gorm.io/gorm"
)

type EmergencyDal struct {
	db *gorm.DB
}

func NewEmergencyDal(data *data.Data) *EmergencyDal {
	return &EmergencyDal{db: data.DB}
}

func (d *EmergencyDal) Create(m *model.EmergencyNotice) error {
	return d.db.Create(m).Error
}

func (d *EmergencyDal) Update(id int64, updates map[string]interface{}) error {
	return d.db.Model(&model.EmergencyNotice{}).Where("id = ?", id).Updates(updates).Error
}

func (d *EmergencyDal) Delete(id int64) error {
	return d.db.Delete(&model.EmergencyNotice{}, id).Error
}

func (d *EmergencyDal) GetById(id int64) (*model.EmergencyNotice, error) {
	var m model.EmergencyNotice
	err := d.db.First(&m, id).Error
	if err != nil {
		return nil, err
	}
	return &m, nil
}

func (d *EmergencyDal) List(page, pageSize int64) ([]model.EmergencyNotice, int64, error) {
	var list []model.EmergencyNotice
	var total int64
	q := d.db.Model(&model.EmergencyNotice{})
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	offset := (page - 1) * pageSize
	if err := d.db.Order("sort_order ASC, id ASC").
		Offset(int(offset)).Limit(int(pageSize)).
		Find(&list).Error; err != nil {
		return nil, 0, err
	}
	return list, total, nil
}

// ListActive 生效中的通知，按展示顺序
func (d *EmergencyDal) ListActive() ([]model.EmergencyNotice, error) {
	var list []model.EmergencyNotice
	err := d.db.Where("enabled = ?", true).
		Order("sort_order ASC, id ASC").
		Find(&list).Error
	return list, err
}
