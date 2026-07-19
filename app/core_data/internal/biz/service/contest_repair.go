package service

import (
	"fmt"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

// RepairContestCellSubmitData 修复站内榜 cell-submits 相关脏数据：
//  1. AtCoder submit_logs 空 external_id → problem
//  2. 赛后练习误入 contest_user_problems 的 AC 格
//  3. AtCoder relative_sec 按 history 结束时间 − 100min 重算
//
// 可安全重复执行（幂等）。
func RepairContestCellSubmitData(db *gorm.DB) (map[string]int64, error) {
	out := map[string]int64{}
	if db == nil {
		return out, nil
	}

	// 1) external_id 回填
	res := db.Exec(`
		UPDATE submit_logs
		SET external_id = problem
		WHERE platform = ?
		  AND (external_id IS NULL OR BTRIM(external_id) = '')
		  AND problem IS NOT NULL AND BTRIM(problem) <> ''
	`, spider.AtCoder)
	if res.Error != nil {
		// SQLite 单测无 BTRIM：降级
		res = db.Exec(`
			UPDATE submit_logs
			SET external_id = problem
			WHERE platform = ?
			  AND (external_id IS NULL OR external_id = '')
			  AND problem IS NOT NULL AND problem <> ''
		`, spider.AtCoder)
	}
	if res.Error != nil {
		return out, fmt.Errorf("backfill external_id: %w", res.Error)
	}
	out["submit_external_id"] = res.RowsAffected

	// 2) 赛后练习格：first_ac 明显晚于本场 contest_logs 结束时间
	// Postgres: 子查询 max(time)
	res = db.Exec(`
		DELETE FROM contest_user_problems AS cup
		USING (
			SELECT platform, contest_id, MAX(time) AS end_t
			FROM contest_logs
			WHERE platform = ?
			GROUP BY platform, contest_id
		) AS e
		WHERE cup.platform = e.platform
		  AND cup.contest_id = e.contest_id
		  AND cup.first_ac_at IS NOT NULL
		  AND cup.first_ac_at > e.end_t + INTERVAL '15 minutes'
	`, spider.AtCoder)
	if res.Error != nil {
		// SQLite / 无 USING：逐场清理
		n, err := repairDeletePracticeCellsSQLite(db)
		if err != nil {
			return out, fmt.Errorf("delete practice cells: %w", err)
		}
		out["practice_cells_deleted"] = n
	} else {
		out["practice_cells_deleted"] = res.RowsAffected
	}

	// 3) 重算 AtCoder relative_sec（end − 100min 为开赛）
	nRel, err := repairAtCoderRelativeSec(db)
	if err != nil {
		return out, fmt.Errorf("relative_sec: %w", err)
	}
	out["relative_sec_updated"] = nRel

	log.Infof("RepairContestCellSubmitData: %+v", out)
	return out, nil
}

func repairDeletePracticeCellsSQLite(db *gorm.DB) (int64, error) {
	type endRow struct {
		Platform  string
		ContestID string
		EndT      time.Time
	}
	var ends []endRow
	if err := db.Model(&model.ContestLog{}).
		Select("platform, contest_id, MAX(time) AS end_t").
		Where("platform = ?", spider.AtCoder).
		Group("platform, contest_id").
		Scan(&ends).Error; err != nil {
		return 0, err
	}
	var total int64
	for _, e := range ends {
		cutoff := e.EndT.Add(15 * time.Minute)
		res := db.Where(
			"platform = ? AND contest_id = ? AND first_ac_at IS NOT NULL AND first_ac_at > ?",
			e.Platform, e.ContestID, cutoff,
		).Delete(&model.ContestUserProblem{})
		if res.Error != nil {
			return total, res.Error
		}
		total += res.RowsAffected
	}
	return total, nil
}

func repairAtCoderRelativeSec(db *gorm.DB) (int64, error) {
	type endRow struct {
		Platform  string
		ContestID string
		EndT      time.Time
	}
	var ends []endRow
	if err := db.Model(&model.ContestLog{}).
		Select("platform, contest_id, MAX(time) AS end_t").
		Where("platform = ?", spider.AtCoder).
		Group("platform, contest_id").
		Scan(&ends).Error; err != nil {
		return 0, err
	}
	durSec := int64((100 * time.Minute).Seconds())
	var total int64
	for _, e := range ends {
		if e.EndT.IsZero() || e.ContestID == "" {
			continue
		}
		startUnix := e.EndT.Unix() - durSec
		var cells []model.ContestUserProblem
		if err := db.Where(
			"platform = ? AND contest_id = ? AND status = ? AND first_ac_at IS NOT NULL",
			e.Platform, e.ContestID, model.ContestCellAC,
		).Find(&cells).Error; err != nil {
			return total, err
		}
		for _, c := range cells {
			if c.FirstACAt == nil {
				continue
			}
			rel := int(c.FirstACAt.Unix() - startUnix)
			if rel < 0 {
				rel = 0
			}
			// 超过默认赛长+缓冲的视为脏，清空 relative
			if rel > int(durSec)+int(contestInferEndBuffer.Seconds()) {
				if err := db.Model(&model.ContestUserProblem{}).Where("id = ?", c.ID).
					Update("relative_sec", nil).Error; err != nil {
					return total, err
				}
				total++
				continue
			}
			if c.RelativeSec != nil && *c.RelativeSec == rel {
				continue
			}
			if err := db.Model(&model.ContestUserProblem{}).Where("id = ?", c.ID).
				Update("relative_sec", rel).Error; err != nil {
				return total, err
			}
			total++
		}
	}
	return total, nil
}
