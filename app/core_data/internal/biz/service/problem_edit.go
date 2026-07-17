package service

import (
	"context"
	"fmt"
	"strings"

	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"gorm.io/gorm"
)

// normalizeEditTags 去空白、去重、限长
func normalizeEditTags(tags []string) []string {
	seen := map[string]struct{}{}
	out := make([]string, 0, len(tags))
	for _, t := range tags {
		t = strings.TrimSpace(t)
		if t == "" {
			continue
		}
		if len([]rune(t)) > 32 {
			t = string([]rune(t)[:32])
		}
		if _, ok := seen[t]; ok {
			continue
		}
		seen[t] = struct{}{}
		out = append(out, t)
		if len(out) >= 12 {
			break
		}
	}
	return out
}

func nonEmptyTags(tags model.StringArray) []string {
	return normalizeEditTags([]string(tags))
}

// ApplyProblemFields 应用标签/题面修改，并按规则更新状态与 AI 入队。
// updateTags / updateContent 为 true 时才写入对应字段（允许清空标签）。
// 规则：
//   - 标签非空 + 有题面 → COMPLETED（后续 AI 跳过）
//   - 有题面 + 标签空 → TAGGING 并入队分析
//   - 仅标签无题面 → 保留/回到 PENDING，不入队 AI
func (uc *ProblemUseCase) ApplyProblemFields(problemID uint, updateTags bool, tags []string, updateContent bool, contentMD, title string) (*model.Problem, error) {
	if !updateTags && !updateContent && strings.TrimSpace(title) == "" {
		return nil, fmt.Errorf("没有需要修改的内容")
	}
	var p model.Problem
	if err := uc.data.DB.First(&p, problemID).Error; err != nil {
		return nil, fmt.Errorf("题目不存在")
	}

	updates := map[string]interface{}{}
	if updateTags {
		updates["tags"] = model.StringArray(normalizeEditTags(tags))
	}
	if updateContent {
		updates["content_md"] = strings.TrimSpace(contentMD)
	}
	if t := strings.TrimSpace(title); t != "" {
		updates["title"] = t
	}
	if len(updates) == 0 {
		return &p, nil
	}
	oldStatus := p.Status
	if err := uc.data.DB.Model(&p).Updates(updates).Error; err != nil {
		return nil, err
	}
	// 重新加载
	if err := uc.data.DB.First(&p, problemID).Error; err != nil {
		return nil, err
	}

	if updateTags {
		oldTags, newTags, e := dal.SyncProblemTags(context.Background(), uc.data.DB, p.ID, []string(p.Tags))
		if e != nil {
			log.Warnf("SyncProblemTags edit id=%d: %v", p.ID, e)
		} else {
			uc.BumpProblemTagsVer()
			uc.BumpProblemListVer()
			if e2 := dal.AdjustUserTagACForProblemTagsChange(context.Background(), uc.data.DB, p.ID, oldTags, newTags); e2 != nil {
				log.Warnf("AdjustUserTagAC edit id=%d: %v", p.ID, e2)
			}
		}
	}

	hasTags := len(nonEmptyTags(p.Tags)) > 0
	hasContent := strings.TrimSpace(p.ContentMD) != ""
	statusUpdates := map[string]interface{}{}
	needAnalyze := false
	newStatus := oldStatus

	switch {
	case hasContent && hasTags:
		// 人工已补齐：完成，后续 AI 跳过
		statusUpdates["status"] = model.ProblemStatusCompleted
		statusUpdates["error_msg"] = ""
		newStatus = model.ProblemStatusCompleted
	case hasContent && !hasTags:
		// 有题面无标签：仍需 AI 分析
		if p.Status != model.ProblemStatusSkipped {
			statusUpdates["status"] = model.ProblemStatusTagging
			statusUpdates["error_msg"] = ""
			needAnalyze = true
			newStatus = model.ProblemStatusTagging
		}
	case !hasContent && hasTags:
		// 仅有标签：等题面；不强制 COMPLETED
		if p.Status == model.ProblemStatusFailed || p.Status == model.ProblemStatusFailedPerm ||
			p.Status == model.ProblemStatusTagging || p.Status == model.ProblemStatusCompleted {
			// 题面仍缺：回到待爬取（若平台可爬）
			if p.Status != model.ProblemStatusSkipped {
				statusUpdates["status"] = model.ProblemStatusPending
				statusUpdates["error_msg"] = "标签已人工填写，待题面"
				newStatus = model.ProblemStatusPending
			}
		}
	}

	if len(statusUpdates) > 0 {
		if err := uc.data.DB.Model(&p).Updates(statusUpdates).Error; err != nil {
			return nil, err
		}
		for k, v := range statusUpdates {
			switch k {
			case "status":
				p.Status = v.(string)
			case "error_msg":
				p.ErrorMsg = v.(string)
			}
		}
	}

	if needAnalyze {
		if err := uc.enqueueAnalyze(p.ID); err != nil {
			log.Warnf("ApplyProblemFields enqueue analyze id=%d: %v", p.ID, err)
		}
	}
	uc.BumpProblemDetailVer(p.ID)
	if newStatus != oldStatus {
		uc.progressMoveStatus(oldStatus, newStatus)
	}
	return &p, nil
}

// ProposeProblemEdit 用户提交审核（同题仅允许一条 pending）
func (uc *ProblemUseCase) ProposeProblemEdit(userID, problemID uint, updateTags bool, tags []string, updateContent bool, contentMD, title, note string) (uint, error) {
	if userID == 0 {
		return 0, fmt.Errorf("请先登录")
	}
	if !updateTags && !updateContent {
		return 0, fmt.Errorf("请至少修改标签或题面")
	}
	if updateTags {
		tags = normalizeEditTags(tags)
		// 允许清空标签（站管审核后 AI 会补）
	}
	if updateContent {
		contentMD = strings.TrimSpace(contentMD)
		if contentMD == "" {
			return 0, fmt.Errorf("题面内容不能为空")
		}
		if len(contentMD) > 200_000 {
			return 0, fmt.Errorf("题面过长")
		}
	}
	var p model.Problem
	if err := uc.data.DB.First(&p, problemID).Error; err != nil {
		return 0, fmt.Errorf("题目不存在")
	}

	var existing model.ProblemEditRequest
	err := uc.data.DB.Where("problem_id = ? AND user_id = ? AND status = ?", problemID, userID, model.ProblemEditPending).
		First(&existing).Error
	if err == nil {
		// 合并到已有 pending（分次改标签/题面不互相覆盖）
		if updateTags {
			existing.HasTags = true
			existing.ProposedTags = model.StringArray(tags)
		}
		if updateContent {
			existing.HasContent = true
			existing.ProposedContentMD = contentMD
		}
		if t := strings.TrimSpace(title); t != "" {
			existing.ProposedTitle = t
		}
		if n := strings.TrimSpace(note); n != "" {
			existing.Note = n
		}
		if err := uc.data.DB.Save(&existing).Error; err != nil {
			return 0, err
		}
		return existing.ID, nil
	}
	if err != gorm.ErrRecordNotFound {
		return 0, err
	}

	req := model.ProblemEditRequest{
		ProblemID:         problemID,
		UserID:            userID,
		HasTags:           updateTags,
		HasContent:        updateContent,
		ProposedTags:      model.StringArray(tags),
		ProposedContentMD: contentMD,
		ProposedTitle:     strings.TrimSpace(title),
		Note:              strings.TrimSpace(note),
		Status:            model.ProblemEditPending,
	}
	if !updateTags {
		req.ProposedTags = model.StringArray{}
	}
	if !updateContent {
		req.ProposedContentMD = ""
	}
	if err := uc.data.DB.Create(&req).Error; err != nil {
		return 0, err
	}
	return req.ID, nil
}

// ListProblemEditRequests 审核列表
func (uc *ProblemUseCase) ListProblemEditRequests(page, pageSize int64, status string) ([]model.ProblemEditRequest, int64, map[uint]*model.Problem, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	q := uc.data.DB.Model(&model.ProblemEditRequest{})
	status = strings.TrimSpace(status)
	if status != "" {
		q = q.Where("status = ?", status)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, nil, err
	}
	var list []model.ProblemEditRequest
	if err := q.Order("id desc").Offset(int((page - 1) * pageSize)).Limit(int(pageSize)).Find(&list).Error; err != nil {
		return nil, 0, nil, err
	}
	pids := make([]uint, 0, len(list))
	seen := map[uint]struct{}{}
	for _, r := range list {
		if _, ok := seen[r.ProblemID]; ok {
			continue
		}
		seen[r.ProblemID] = struct{}{}
		pids = append(pids, r.ProblemID)
	}
	probMap := map[uint]*model.Problem{}
	if len(pids) > 0 {
		var probs []model.Problem
		if err := uc.data.DB.Where("id IN ?", pids).Find(&probs).Error; err == nil {
			for i := range probs {
				probMap[probs[i].ID] = &probs[i]
			}
		}
	}
	return list, total, probMap, nil
}

// ReviewProblemEdit 站管通过/驳回
func (uc *ProblemUseCase) ReviewProblemEdit(requestID, reviewerID uint, approve bool, reviewNote string) error {
	var req model.ProblemEditRequest
	if err := uc.data.DB.First(&req, requestID).Error; err != nil {
		return fmt.Errorf("申请不存在")
	}
	if req.Status != model.ProblemEditPending {
		return fmt.Errorf("该申请已处理")
	}
	if !approve {
		rid := reviewerID
		return uc.data.DB.Model(&req).Updates(map[string]interface{}{
			"status":      model.ProblemEditRejected,
			"reviewer_id": rid,
			"review_note": strings.TrimSpace(reviewNote),
		}).Error
	}
	// 通过：应用修改
	_, err := uc.ApplyProblemFields(
		req.ProblemID,
		req.HasTags, []string(req.ProposedTags),
		req.HasContent, req.ProposedContentMD,
		req.ProposedTitle,
	)
	if err != nil {
		return err
	}
	rid := reviewerID
	return uc.data.DB.Model(&req).Updates(map[string]interface{}{
		"status":      model.ProblemEditApproved,
		"reviewer_id": rid,
		"review_note": strings.TrimSpace(reviewNote),
	}).Error
}

// MyPendingProblemEdit 当前用户对该题的待审申请
func (uc *ProblemUseCase) MyPendingProblemEdit(userID, problemID uint) (*model.ProblemEditRequest, error) {
	if userID == 0 || problemID == 0 {
		return nil, nil
	}
	var req model.ProblemEditRequest
	err := uc.data.DB.Where("problem_id = ? AND user_id = ? AND status = ?", problemID, userID, model.ProblemEditPending).
		First(&req).Error
	if err == gorm.ErrRecordNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &req, nil
}
