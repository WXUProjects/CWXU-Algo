package dal

import (
	"context"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"

	"gorm.io/gorm"
)

// HotProblemAgg 近窗热题聚合行
type HotProblemAgg struct {
	ProblemID   uint      `gorm:"column:problem_id"`
	SubmitCount int64     `gorm:"column:submit_count"`
	SolverCount int64     `gorm:"column:solver_count"`
	AcCount     int64     `gorm:"column:ac_count"`
	LastTime    time.Time `gorm:"column:last_time"`
	Score       float64   `gorm:"column:score"`
}

// ListHotProblems 近 since 窗口内按综合热度排序分页。
// 热度 = submit_count*1 + solver_count*3 + ac_count*2
// - submit_count：排除力扣合成/不计入提交的行
// - solver_count：去重 user_id（做题人数）
// - ac_count：is_ac=true 次数
func ListHotProblems(ctx context.Context, db *gorm.DB, since time.Time, page, pageSize int64) ([]HotProblemAgg, int64, error) {
	if db == nil {
		return nil, 0, gorm.ErrInvalidDB
	}
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}

	// 先数有多少题在窗口内有提交（有 problem_id）
	var total int64
	countSQL := `
		SELECT COUNT(*) FROM (
			SELECT problem_id
			FROM submit_logs
			WHERE problem_id IS NOT NULL
			  AND problem_id > 0
			  AND time >= ?
			GROUP BY problem_id
		) t`
	if err := db.WithContext(ctx).Raw(countSQL, since).Scan(&total).Error; err != nil {
		return nil, 0, err
	}
	if total == 0 {
		return []HotProblemAgg{}, 0, nil
	}

	// 综合分：提交 1 分、做题人数 3 分、AC 2 分（人数权重最高）
	listSQL := `
		SELECT
			problem_id,
			submit_count,
			solver_count,
			ac_count,
			last_time,
			(submit_count * 1.0 + solver_count * 3.0 + ac_count * 2.0) AS score
		FROM (
			SELECT
				problem_id,
				COUNT(*) FILTER (
					WHERE ` + model.SQLExcludeLeetCodeNonSubmit + `
				) AS submit_count,
				COUNT(DISTINCT user_id) AS solver_count,
				COUNT(*) FILTER (WHERE is_ac = true) AS ac_count,
				MAX(time) AS last_time
			FROM submit_logs
			WHERE problem_id IS NOT NULL
			  AND problem_id > 0
			  AND time >= ?
			GROUP BY problem_id
		) agg
		ORDER BY score DESC, last_time DESC, problem_id DESC
		LIMIT ? OFFSET ?`

	offset := (page - 1) * pageSize
	var rows []HotProblemAgg
	if err := db.WithContext(ctx).Raw(listSQL, since, pageSize, offset).Scan(&rows).Error; err != nil {
		return nil, 0, err
	}
	return rows, total, nil
}
