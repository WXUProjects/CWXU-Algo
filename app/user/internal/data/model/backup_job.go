package model

import "time"

const (
	BackupKindExport = "export"
	BackupKindImport = "import"

	BackupStatusPending = "pending"
	BackupStatusRunning = "running"
	BackupStatusDone    = "done"
	BackupStatusFailed  = "failed"
)

// BackupJob 站点数据备份/恢复异步任务
type BackupJob struct {
	ID          uint `gorm:"primaryKey"`
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Kind        string     `gorm:"size:16;not null;index;comment:export|import"`
	Status      string     `gorm:"size:16;not null;index;default:pending;comment:pending|running|done|failed"`
	Scopes      string     `gorm:"type:text;comment:JSON scopes 数组"`
	Progress    int        `gorm:"default:0;comment:0-100"`
	Message     string     `gorm:"size:512"`
	FilePath    string     `gorm:"size:512;comment:相对 backup 目录的 zip 路径"`
	FileSize    int64      `gorm:"default:0"`
	CreatedBy   uint       `gorm:"index;comment:发起人 user id"`
	ErrorDetail string     `gorm:"type:text"`
	StartedAt   *time.Time `gorm:"comment:开始时间"`
	FinishedAt  *time.Time `gorm:"comment:结束时间"`
}

func (BackupJob) TableName() string { return "backup_jobs" }
