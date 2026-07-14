package model

import (
	"database/sql/driver"
	"encoding/json"
	"time"
)

const (
	ProblemStatusPending   = "PENDING"
	ProblemStatusFetching  = "FETCHING"
	ProblemStatusCompleted = "COMPLETED"
	ProblemStatusFailed    = "FAILED"
	ProblemStatusSkipped   = "SKIPPED"
)

// StringArray JSON 数组字段
type StringArray []string

func (a StringArray) Value() (driver.Value, error) {
	if a == nil {
		return []byte("[]"), nil
	}
	b, err := json.Marshal(a)
	return b, err
}

func (a *StringArray) Scan(value interface{}) error {
	if value == nil {
		*a = StringArray{}
		return nil
	}
	var b []byte
	switch v := value.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		*a = StringArray{}
		return nil
	}
	if len(b) == 0 {
		*a = StringArray{}
		return nil
	}
	return json.Unmarshal(b, a)
}

// SolutionsMeta AI 识别的可用解法
type SolutionMeta struct {
	Name              string `json:"name"`
	TimeComplexity    string `json:"time_complexity"`
	SpaceComplexity   string `json:"space_complexity"`
	BriefExplanation  string `json:"brief_explanation"`
}

type SolutionsMeta []SolutionMeta

func (s SolutionsMeta) Value() (driver.Value, error) {
	if s == nil {
		return []byte("[]"), nil
	}
	b, err := json.Marshal(s)
	return b, err
}

func (s *SolutionsMeta) Scan(value interface{}) error {
	if value == nil {
		*s = SolutionsMeta{}
		return nil
	}
	var b []byte
	switch v := value.(type) {
	case []byte:
		b = v
	case string:
		b = []byte(v)
	default:
		*s = SolutionsMeta{}
		return nil
	}
	if len(b) == 0 {
		*s = SolutionsMeta{}
		return nil
	}
	return json.Unmarshal(b, s)
}

// Problem 全局去重题库
type Problem struct {
	ID              uint          `gorm:"primaryKey"`
	Platform        string        `gorm:"size:32;not null;uniqueIndex:idx_platform_external"`
	ExternalID      string        `gorm:"size:128;not null;uniqueIndex:idx_platform_external"`
	Title           string        `gorm:"size:512"`
	URL             string        `gorm:"size:1024"`
	ContentMD       string        `gorm:"type:text"`
	ProblemType     string        `gorm:"size:128"`
	Tags            StringArray   `gorm:"type:jsonb;default:'[]'"`
	SolutionsMeta   SolutionsMeta `gorm:"type:jsonb;default:'[]'"`
	Difficulty      string        `gorm:"size:32"`
	Status          string        `gorm:"size:32;index;default:'PENDING'"`
	ErrorMsg        string        `gorm:"type:text"`
	LastSubmittedAt *time.Time    `gorm:"index"`
	CreatedAt       time.Time
	UpdatedAt       time.Time
}
