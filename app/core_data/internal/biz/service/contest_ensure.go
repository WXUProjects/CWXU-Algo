package service

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"cwxu-algo/app/common/event"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// EnsureContestProblemsOnce 每场比赛只执行一次：主动发现题目 → 入库（external_id 与提交一致）→ 强制爬题面。
// AI 分析不强制：爬完后走标准闸门（有资格用户提交才分析）。
// 调用方可同步等待列表写入；爬取异步。
func (uc *ProblemUseCase) EnsureContestProblemsOnce(platform, contestID string) (status string, err error) {
	if uc == nil || uc.data == nil || uc.data.DB == nil {
		return "", fmt.Errorf("usecase not ready")
	}
	platform = strings.TrimSpace(platform)
	contestID = strings.TrimSpace(contestID)
	if platform == "" || contestID == "" {
		return "", fmt.Errorf("empty platform/contestId")
	}

	// 已 done / 正在 running：直接返回（每场成功只跑一次，不因多用户重复打 OJ）
	// failed 允许再试一次（网络/OJ 抖动）；用 CAS 从 failed→running 抢占
	var existing model.ContestProblemEnsure
	err = uc.data.DB.Where("platform = ? AND contest_id = ?", platform, contestID).First(&existing).Error
	if err == nil {
		if existing.Status == model.ContestEnsureDone || existing.Status == model.ContestEnsureRunning {
			return existing.Status, nil
		}
		if existing.Status == model.ContestEnsureFailed {
			res := uc.data.DB.Model(&model.ContestProblemEnsure{}).
				Where("id = ? AND status = ?", existing.ID, model.ContestEnsureFailed).
				Updates(map[string]interface{}{
					"status":    model.ContestEnsureRunning,
					"error_msg": "",
				})
			if res.Error != nil {
				return "", res.Error
			}
			if res.RowsAffected == 0 {
				_ = uc.data.DB.Where("id = ?", existing.ID).First(&existing).Error
				return existing.Status, nil
			}
			// 本 goroutine 重试
			goto runEnsure
		}
	} else if err != gorm.ErrRecordNotFound {
		return "", err
	}

	// 抢占：插入 running；冲突则别人在做
	row := model.ContestProblemEnsure{
		Platform:  platform,
		ContestID: contestID,
		Status:    model.ContestEnsureRunning,
	}
	res := uc.data.DB.Clauses(clause.OnConflict{DoNothing: true}).Create(&row)
	if res.Error != nil {
		return "", res.Error
	}
	if res.RowsAffected == 0 {
		// 并发：读当前状态
		_ = uc.data.DB.Where("platform = ? AND contest_id = ?", platform, contestID).First(&existing).Error
		return existing.Status, nil
	}

runEnsure:

	// 本 goroutine 执行发现
	if err := uc.runContestEnsure(platform, contestID); err != nil {
		log.Warnf("EnsureContestProblemsOnce %s/%s: %v", platform, contestID, err)
		_ = uc.data.DB.Model(&model.ContestProblemEnsure{}).
			Where("platform = ? AND contest_id = ?", platform, contestID).
			Updates(map[string]interface{}{
				"status":     model.ContestEnsureFailed,
				"error_msg":  truncateStr(err.Error(), 500),
				"ensured_at": time.Now(),
			}).Error
		return model.ContestEnsureFailed, err
	}
	now := time.Now()
	_ = uc.data.DB.Model(&model.ContestProblemEnsure{}).
		Where("platform = ? AND contest_id = ?", platform, contestID).
		Updates(map[string]interface{}{
			"status":     model.ContestEnsureDone,
			"error_msg":  "",
			"ensured_at": now,
		}).Error
	return model.ContestEnsureDone, nil
}

func (uc *ProblemUseCase) runContestEnsure(platform, contestID string) error {
	specs, err := ListContestProblemSpecs(platform, contestID)
	if err != nil {
		return err
	}
	if len(specs) == 0 {
		return fmt.Errorf("empty problem list")
	}
	// 稳定排序
	sort.SliceStable(specs, func(i, j int) bool {
		return labelSortKey(specs[i].Label) < labelSortKey(specs[j].Label)
	})

	for i, sp := range specs {
		if sp.ExternalID == "" || sp.Platform == "" {
			continue
		}
		parsed := &ParsedProblem{
			Platform:   sp.Platform,
			ExternalID: sp.ExternalID,
			Title:      sp.Title,
			URL:        sp.URL,
		}
		p, err := uc.UpsertProblemFromParsedNoAI(parsed)
		if err != nil {
			log.Warnf("contest ensure upsert %s/%s: %v", sp.Platform, sp.ExternalID, err)
			continue
		}
		// 强制爬题面；AI 不强制（SkipAnalyze=false + Actor=0 → 走 submitter 闸门）
		if err := uc.ForceEnqueueFetchContest(p.ID); err != nil {
			log.Warnf("contest ensure fetch %d: %v", p.ID, err)
		}
		item := model.ContestProblem{
			Platform:   platform,
			ContestID:  contestID,
			Label:      firstNonEmpty(sp.Label, sp.ExternalID),
			SortOrder:  i,
			ExternalID: sp.ExternalID,
			Title:      firstNonEmpty(sp.Title, p.Title),
			URL:        firstNonEmpty(sp.URL, p.URL),
			ProblemID:  p.ID,
		}
		_ = uc.data.DB.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "platform"}, {Name: "contest_id"}, {Name: "label"}},
			DoUpdates: clause.AssignmentColumns([]string{
				"sort_order", "external_id", "title", "url", "problem_id", "updated_at",
			}),
		}).Create(&item).Error
	}

	// 回写 contest_logs.total_count（同场所有用户行）
	n := int64(0)
	_ = uc.data.DB.Model(&model.ContestProblem{}).
		Where("platform = ? AND contest_id = ?", platform, contestID).
		Count(&n).Error
	if n > 0 {
		_ = uc.data.DB.Model(&model.ContestLog{}).
			Where("platform = ? AND contest_id = ?", platform, contestID).
			Update("total_count", n).Error
	}
	return nil
}

// UpsertProblemFromParsedNoAI 入库但不按 actor 强制 AI；题面强制爬取走 ForceEnqueueFetchContest。
func (uc *ProblemUseCase) UpsertProblemFromParsedNoAI(parsed *ParsedProblem) (*model.Problem, error) {
	if uc == nil || parsed == nil || parsed.Platform == "" || parsed.ExternalID == "" {
		return nil, fmt.Errorf("invalid parsed problem")
	}
	var existing model.Problem
	err := uc.data.DB.Where("platform = ? AND external_id = ?", parsed.Platform, parsed.ExternalID).
		First(&existing).Error
	if err == gorm.ErrRecordNotFound {
		status := model.ProblemStatusPending
		if parsed.SkipFetch {
			status = model.ProblemStatusSkipped
		}
		p := model.Problem{
			Platform:   parsed.Platform,
			ExternalID: parsed.ExternalID,
			Title:      firstNonEmpty(parsed.Title, parsed.ExternalID),
			URL:        parsed.URL,
			Status:     status,
			Tags:       model.StringArray{},
		}
		if err := uc.data.DB.Create(&p).Error; err != nil {
			if err2 := uc.data.DB.Where("platform = ? AND external_id = ?", parsed.Platform, parsed.ExternalID).
				First(&existing).Error; err2 != nil {
				return nil, err
			}
		} else {
			existing = p
		}
	} else if err != nil {
		return nil, err
	} else {
		updates := map[string]interface{}{}
		if existing.Title == "" && parsed.Title != "" {
			updates["title"] = parsed.Title
			existing.Title = parsed.Title
		}
		if existing.URL == "" && parsed.URL != "" {
			updates["url"] = parsed.URL
			existing.URL = parsed.URL
		}
		if len(updates) > 0 {
			_ = uc.data.DB.Model(&existing).Updates(updates).Error
		}
	}
	return &existing, nil
}

// ForceEnqueueFetchContest 比赛题面：强制爬取；爬完后标准 AI 闸门（有资格用户提交才分析）。
func (uc *ProblemUseCase) ForceEnqueueFetchContest(problemID uint) error {
	if uc == nil || problemID == 0 {
		return nil
	}
	var p model.Problem
	if err := uc.data.DB.First(&p, problemID).Error; err != nil {
		return err
	}
	// 已有题面：尝试标准 AI（有资格提交者）
	if strings.TrimSpace(p.ContentMD) != "" {
		if len(nonEmptyTags(p.Tags)) == 0 {
			return uc.enqueueAnalyzePrio(problemID, mqPriorityIncremental)
		}
		return nil
	}
	if p.Status == model.ProblemStatusCompleted || p.Status == model.ProblemStatusSkipped {
		return nil
	}
	if p.Status == model.ProblemStatusFailedPerm {
		return nil
	}
	if p.Status == model.ProblemStatusFailed && isPermanentFetchError(p.ErrorMsg) {
		// 永久失败不再刷
		return nil
	}
	if uc.mq == nil {
		return fmt.Errorf("mq not ready")
	}
	body, _ := json.Marshal(event.ProblemFetchEvent{
		ProblemID:   p.ID,
		Platform:    p.Platform,
		ExternalID:  p.ExternalID,
		URL:         p.URL,
		Force:       true,  // 忽略用户爬取资格
		SkipAnalyze: false, // 爬完走 enqueueAnalyze（submitter AI 闸门）
		ActorUserID: 0,
	})
	if err := uc.declareProblemQueue("problem_fetch"); err != nil {
		return err
	}
	return uc.mq.Publish("", "problem_fetch", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Priority:     mqPriorityIncremental,
	})
}

// ListContestProblems 读目录 + 关联 problem 状态。
func (uc *ProblemUseCase) ListContestProblems(platform, contestID string) ([]map[string]interface{}, string, error) {
	if uc == nil || uc.data == nil {
		return nil, "", fmt.Errorf("usecase not ready")
	}
	var ensure model.ContestProblemEnsure
	status := ""
	if uc.data.DB.Where("platform = ? AND contest_id = ?", platform, contestID).First(&ensure).Error == nil {
		status = ensure.Status
	}
	var items []model.ContestProblem
	_ = uc.data.DB.Where("platform = ? AND contest_id = ?", platform, contestID).
		Order("sort_order asc, id asc").Find(&items).Error

	// 批量 problem
	ids := make([]uint, 0, len(items))
	for _, it := range items {
		if it.ProblemID > 0 {
			ids = append(ids, it.ProblemID)
		}
	}
	probMap := map[uint]model.Problem{}
	if len(ids) > 0 {
		var probs []model.Problem
		_ = uc.data.DB.Where("id IN ?", ids).Find(&probs).Error
		for _, p := range probs {
			probMap[p.ID] = p
		}
	}
	out := make([]map[string]interface{}, 0, len(items))
	for _, it := range items {
		m := map[string]interface{}{
			"label":      it.Label,
			"externalId": it.ExternalID,
			"title":      it.Title,
			"url":        it.URL,
			"problemId":  it.ProblemID,
			"sortOrder":  it.SortOrder,
		}
		if p, ok := probMap[it.ProblemID]; ok {
			m["title"] = firstNonEmpty(p.Title, it.Title)
			m["status"] = p.Status
			m["hasContent"] = strings.TrimSpace(p.ContentMD) != ""
			m["difficulty"] = p.Difficulty
			m["tags"] = []string(p.Tags)
		} else {
			m["status"] = model.ProblemStatusPending
			m["hasContent"] = false
		}
		out = append(out, m)
	}
	return out, status, nil
}

