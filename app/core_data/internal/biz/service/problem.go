package service

import (
	"context"
	"cwxu-algo/app/common/event"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"cwxu-algo/app/core_data/internal/spider/problem_fetch"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/streadway/amqp"
	"gorm.io/gorm"
)

type ProblemUseCase struct {
	data   *data.Data
	mq     *event.RabbitMQ
	tagger *ProblemTagger
}

func NewProblemUseCase(data *data.Data, mq *event.RabbitMQ, tagger *ProblemTagger) *ProblemUseCase {
	return &ProblemUseCase{data: data, mq: mq, tagger: tagger}
}

// MQ 优先级：队列需 x-max-priority；增量爬虫入队最高，回填/重置队列为 bulk
const (
	mqPriorityBulk        uint8 = 1
	mqPriorityIncremental uint8 = 9
	mqMaxPriority         int32 = 10
)

// BindSubmitsAfterSpider 爬虫写入提交后绑定/创建题库（增量，最高优先级入队）
func (uc *ProblemUseCase) BindSubmitsAfterSpider(userId int64) {
	var logs []model.SubmitLog
	// 仅处理未绑定的
	if err := uc.data.DB.Where("user_id = ? AND (problem_id IS NULL OR problem_id = 0)", userId).
		Order("id desc").Limit(500).Find(&logs).Error; err != nil {
		log.Errorf("BindSubmitsAfterSpider query: %v", err)
		return
	}
	for i := range logs {
		if _, _, err := uc.resolveOne(&logs[i], true); err != nil {
			log.Debugf("resolve submit %d: %v", logs[i].ID, err)
		}
	}
}

// resolveOne 解析并绑定单条提交；返回 (problem, isNew, err)
// highPriority=true：增量爬虫路径，MQ 最高优先级
func (uc *ProblemUseCase) resolveOne(sl *model.SubmitLog, highPriority bool) (*model.Problem, bool, error) {
	parsed, err := ParseProblemIdentity(sl.Platform, sl.Contest, sl.Problem)
	if err != nil {
		return nil, false, err
	}
	// 不可爬平台（如 LeetCode）不进入题库
	if parsed.SkipBank {
		return nil, false, fmt.Errorf("skip bank: %s", parsed.Platform)
	}

	var existing model.Problem
	err = uc.data.DB.Where("platform = ? AND external_id = ?", parsed.Platform, parsed.ExternalID).First(&existing).Error
	isNew := false
	if err == gorm.ErrRecordNotFound {
		status := model.ProblemStatusPending
		if parsed.SkipFetch {
			status = model.ProblemStatusSkipped
		}
		p := model.Problem{
			Platform:   parsed.Platform,
			ExternalID: parsed.ExternalID,
			Title:      firstNonEmpty(parsed.Title, sl.Problem),
			URL:        parsed.URL,
			Status:     status,
			Tags:       model.StringArray{},
		}
		t := sl.Time
		p.LastSubmittedAt = &t
		if err := uc.data.DB.Create(&p).Error; err != nil {
			// 并发唯一冲突 → 再查
			if err2 := uc.data.DB.Where("platform = ? AND external_id = ?", parsed.Platform, parsed.ExternalID).
				First(&existing).Error; err2 != nil {
				return nil, false, err
			}
		} else {
			existing = p
			isNew = true
		}
	} else if err != nil {
		return nil, false, err
	} else {
		// 更新最近提交时间
		if existing.LastSubmittedAt == nil || sl.Time.After(*existing.LastSubmittedAt) {
			_ = uc.data.DB.Model(&existing).Update("last_submitted_at", sl.Time).Error
			existing.LastSubmittedAt = &sl.Time
		}
		if existing.Title == "" && parsed.Title != "" {
			_ = uc.data.DB.Model(&existing).Update("title", parsed.Title).Error
			existing.Title = parsed.Title
		}
		if existing.URL == "" && parsed.URL != "" {
			_ = uc.data.DB.Model(&existing).Update("url", parsed.URL).Error
			existing.URL = parsed.URL
		}
	}

	// 绑定 submit
	pid := existing.ID
	_ = uc.data.DB.Model(sl).Updates(map[string]interface{}{
		"problem_id":  pid,
		"external_id": parsed.ExternalID,
	}).Error

	prio := mqPriorityBulk
	if highPriority {
		prio = mqPriorityIncremental
	}

	// 新题且可爬 → 入队抓题面 + AI
	if isNew && !parsed.SkipFetch && existing.Status == model.ProblemStatusPending {
		if err := uc.enqueueFetchPrio(existing.ID, existing.Platform, existing.ExternalID, existing.URL, prio); err != nil {
			log.Errorf("enqueue problem %d: %v", existing.ID, err)
		}
	}
	// 永久失败：升级标记后不再入队
	if existing.Status == model.ProblemStatusFailed && isPermanentFetchError(existing.ErrorMsg) {
		_ = uc.data.DB.Model(&existing).Update("status", model.ProblemStatusFailedPerm).Error
		existing.Status = model.ProblemStatusFailedPerm
	}

	// 已存在但题面未完成：补入队（例如之前失败/卡住）；FAILED_PERM 永不重试
	// 已 COMPLETED：分析过则丢弃，不入队
	if !isNew && !parsed.SkipFetch {
		switch existing.Status {
		case model.ProblemStatusPending, model.ProblemStatusFailed:
			if strings.TrimSpace(existing.ContentMD) == "" {
				if err := uc.enqueueFetchPrio(existing.ID, existing.Platform, existing.ExternalID, existing.URL, prio); err != nil {
					log.Errorf("re-enqueue fetch problem %d: %v", existing.ID, err)
				}
			} else {
				// 有题面未分析完 → 分析队列
				_ = uc.data.DB.Model(&existing).Update("status", model.ProblemStatusTagging).Error
				if err := uc.enqueueAnalyzePrio(existing.ID, prio); err != nil {
					log.Errorf("re-enqueue analyze problem %d: %v", existing.ID, err)
				}
			}
		case model.ProblemStatusTagging:
			if strings.TrimSpace(existing.ContentMD) != "" {
				if err := uc.enqueueAnalyzePrio(existing.ID, prio); err != nil {
					log.Errorf("re-enqueue analyze problem %d: %v", existing.ID, err)
				}
			}
		case model.ProblemStatusCompleted, model.ProblemStatusFailedPerm, model.ProblemStatusSkipped:
			// 已分析完成 / 永久失败 / 跳过：不入队
		}
	}
	return &existing, isNew, nil
}

func (uc *ProblemUseCase) declareProblemQueue(name string) error {
	if uc.mq == nil {
		return fmt.Errorf("mq not ready")
	}
	// 队列已存在：直接成功。重复 QueueDeclare 且 args 不一致会 PRECONDITION 杀 channel，
	// 导致后续 Publish 失败且消费者永远注册不上。
	if _, err := uc.mq.QueueInspect(name); err == nil {
		return nil
	}
	args := amqp.Table{"x-max-priority": mqMaxPriority}
	if _, err := uc.mq.QueueDeclare(name, true, false, false, false, args); err != nil {
		// 已存在且无 max-priority 时 PRECONDITION_FAILED：降级声明
		if _, err2 := uc.mq.QueueDeclare(name, true, false, false, false, nil); err2 != nil {
			return err
		}
	}
	return nil
}

func (uc *ProblemUseCase) enqueueFetch(id uint, platform, externalID, url string) error {
	return uc.enqueueFetchPrio(id, platform, externalID, url, mqPriorityBulk)
}

func (uc *ProblemUseCase) enqueueFetchPrio(id uint, platform, externalID, url string, priority uint8) error {
	if uc.mq == nil {
		return fmt.Errorf("mq not ready")
	}
	body, _ := json.Marshal(event.ProblemFetchEvent{
		ProblemID:  id,
		Platform:   platform,
		ExternalID: externalID,
		URL:        url,
	})
	if err := uc.declareProblemQueue("problem_fetch"); err != nil {
		return err
	}
	return uc.mq.Publish("", "problem_fetch", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Priority:     priority,
	})
}

func (uc *ProblemUseCase) enqueueAnalyze(id uint) error {
	return uc.enqueueAnalyzePrio(id, mqPriorityBulk)
}

func (uc *ProblemUseCase) enqueueAnalyzePrio(id uint, priority uint8) error {
	if uc.mq == nil {
		return fmt.Errorf("mq not ready")
	}
	body, _ := json.Marshal(event.ProblemAnalyzeEvent{ProblemID: id})
	if err := uc.declareProblemQueue("problem_analyze"); err != nil {
		return err
	}
	return uc.mq.Publish("", "problem_analyze", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Priority:     priority,
	})
}

// ProcessFetch 仅爬取题面；成功后状态 TAGGING 并投递 AI 队列
func (uc *ProblemUseCase) ProcessFetch(ctx context.Context, ev event.ProblemFetchEvent) error {
	if pipelineControl.IsFetchPaused() {
		return fmt.Errorf("fetch paused")
	}
	var p model.Problem
	if err := uc.data.DB.First(&p, ev.ProblemID).Error; err != nil {
		return err
	}
	pipelineControl.TrackStart("fetch", p.ID, p.Platform, p.ExternalID, p.Title)
	defer pipelineControl.TrackEnd("fetch", p.ID)
	// 已识别完成：跳过
	if p.Status == model.ProblemStatusCompleted {
		return nil
	}
	// 已有题面：不再爬取；近 6 个月（submit_logs）才入 AI
	if strings.TrimSpace(p.ContentMD) != "" || p.Status == model.ProblemStatusTagging {
		if p.Status != model.ProblemStatusCompleted {
			_ = uc.data.DB.Model(&p).Update("status", model.ProblemStatusTagging).Error
			if !pipelineControl.IsAnalyzePaused() && uc.withinAnalyzeWindow(&p) {
				return uc.enqueueAnalyze(p.ID)
			}
		}
		return nil
	}
	// 永久失败：直接丢弃消息，不再爬
	if p.Status == model.ProblemStatusFailedPerm {
		return nil
	}
	if p.Status == model.ProblemStatusFailed && isPermanentFetchError(p.ErrorMsg) {
		_ = uc.data.DB.Model(&p).Update("status", model.ProblemStatusFailedPerm).Error
		return nil
	}
	if p.Platform == spider.LeetCode || p.Status == model.ProblemStatusSkipped {
		_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
			"status": model.ProblemStatusSkipped,
		}).Error
		return nil
	}

	res := uc.data.DB.Model(&model.Problem{}).
		Where("id = ? AND status IN ?", p.ID, []string{model.ProblemStatusPending, model.ProblemStatusFailed, model.ProblemStatusFetching}).
		Update("status", model.ProblemStatusFetching)
	if res.Error != nil {
		return res.Error
	}
	// 已被别人标成永久失败 / 并发跳过
	if res.RowsAffected == 0 {
		return nil
	}

	url := p.URL
	if url == "" {
		url = ev.URL
	}
	fetched, err := problem_fetch.Fetch(p.Platform, p.ExternalID, url)
	if err != nil {
		return uc.handleFetchError(&p, err)
	}

	title := p.Title
	if fetched.Title != "" {
		title = fetched.Title
	}
	updates := map[string]interface{}{
		"content_md":       fetched.ContentMD,
		"title":            title,
		"error_msg":        "",
		"status":           model.ProblemStatusTagging,
		"fetch_attempts":   0,
		"fetch_fail_since": nil,
	}
	if p.URL == "" && url != "" {
		updates["url"] = url
	}
	if err := uc.data.DB.Model(&p).Updates(updates).Error; err != nil {
		return err
	}
	// 爬取成功后：仅近 6 个月有提交的题进 AI（以 submit_logs 为准）
	// 分析暂停时仍入队（暂停不清队列，恢复后继续）；高优先级延续当前已出队的爬取任务
	if !uc.withinAnalyzeWindow(&p) {
		return nil
	}
	return uc.enqueueAnalyzePrio(p.ID, mqPriorityIncremental)
}

// ProcessAnalyze 仅 AI 打标（不爬取、不送用户代码）
// 6 个月窗口：以 submit_logs 中该题最近一次提交时间为准（并回写 last_submitted_at）。
func (uc *ProblemUseCase) ProcessAnalyze(ctx context.Context, ev event.ProblemAnalyzeEvent) error {
	if pipelineControl.IsAnalyzePaused() {
		return fmt.Errorf("ai analyze paused")
	}
	var p model.Problem
	if err := uc.data.DB.First(&p, ev.ProblemID).Error; err != nil {
		return err
	}
	pipelineControl.TrackStart("analyze", p.ID, p.Platform, p.ExternalID, p.Title)
	defer pipelineControl.TrackEnd("analyze", p.ID)
	// 已识别完成：跳过
	if p.Status == model.ProblemStatusCompleted {
		log.Debugf("ProcessAnalyze skip completed id=%d", p.ID)
		return nil
	}
	if p.Status == model.ProblemStatusSkipped || p.Platform == spider.LeetCode {
		log.Debugf("ProcessAnalyze skip skipped/leetcode id=%d", p.ID)
		return nil
	}
	if p.Status == model.ProblemStatusFailedPerm {
		log.Debugf("ProcessAnalyze skip failed_perm id=%d", p.ID)
		return nil
	}
	if strings.TrimSpace(p.ContentMD) == "" {
		// 永久错误不重爬
		if isPermanentFetchError(p.ErrorMsg) {
			_ = uc.data.DB.Model(&p).Update("status", model.ProblemStatusFailedPerm).Error
			return nil
		}
		// 无题面，退回爬取
		_ = uc.data.DB.Model(&p).Update("status", model.ProblemStatusPending).Error
		return uc.enqueueFetch(p.ID, p.Platform, p.ExternalID, p.URL)
	}

	// 近 6 个月：以 submit_logs 最近提交为准（与看板「待分析近6月」同一口径）
	if !uc.withinAnalyzeWindow(&p) {
		// 超窗：不再占「待分析」名额；静默 Ack 会让人以为在跑 AI
		log.Warnf("ProcessAnalyze out-of-window id=%d last=%v → SKIPPED_ANALYZE", p.ID, p.LastSubmittedAt)
		_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
			"status":    model.ProblemStatusTagging, // 仍保留有题面待分析语义
			"error_msg": "超出6个月分析窗口(以submit_logs最近提交为准)，已跳过",
		}).Error
		// 返回 error 会 requeue；此处 Ack 丢弃，靠重置/新提交再入队
		return nil
	}

	if uc.tagger == nil || !uc.tagger.Ready() {
		return uc.data.DB.Model(&p).Updates(map[string]interface{}{
			"status":    model.ProblemStatusFailed,
			"error_msg": "ai_analyze 未配置",
		}).Error
	}

	_ = uc.data.DB.Model(&p).Update("status", model.ProblemStatusTagging).Error
	log.Infof("ProcessAnalyze start id=%d platform=%s ext=%s last=%v", p.ID, p.Platform, p.ExternalID, p.LastSubmittedAt)

	result, aerr := uc.tagger.Analyze(ctx, p.Title, p.ContentMD)
	if aerr != nil {
		log.Errorf("AI tag problem %d: %v", p.ID, aerr)
		_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
			"status":    model.ProblemStatusFailed,
			"error_msg": "AI: " + truncateErr(aerr.Error()),
		}).Error
		return aerr
	}
	updates := map[string]interface{}{
		"problem_type":   result.ProblemType,
		"difficulty":     result.Difficulty,
		"tags":           model.StringArray(result.AlgorithmTags),
		"solutions_meta": model.SolutionsMeta(result.SuggestedSolutions),
		"status":         model.ProblemStatusCompleted,
		"error_msg":      "",
	}
	// AI 顺手优化排版后的题面
	if strings.TrimSpace(result.ContentMD) != "" {
		updates["content_md"] = result.ContentMD
	}
	return uc.data.DB.Model(&p).Updates(updates).Error
}

// backfillWindow 历史回填 / AI 分析仅处理最近 N 个月有提交的题
const backfillWindow = 6 * 30 * 24 * time.Hour // ≈6 个月

// maxFetchAttempts 非瞬时爬取失败最大次数，超过则 FAILED_PERM
const maxFetchAttempts = 3

// transientFailWindow 405/WAF 等瞬时错误允许持续重试的最长时间，超时 → FAILED_PERM
const transientFailWindow = 24 * time.Hour

// latestSubmitTimeFromLogs 从 submit_logs 取该题最近一次提交时间
func (uc *ProblemUseCase) latestSubmitTimeFromLogs(problemID uint) *time.Time {
	var t *time.Time
	_ = uc.data.DB.Model(&model.SubmitLog{}).
		Select("MAX(time)").
		Where("problem_id = ?", problemID).
		Scan(&t).Error
	return t
}

// refreshLastSubmittedAt 用 submit_logs 最近提交回写 problems.last_submitted_at
func (uc *ProblemUseCase) refreshLastSubmittedAt(p *model.Problem) *time.Time {
	if p == nil || p.ID == 0 {
		return nil
	}
	latest := uc.latestSubmitTimeFromLogs(p.ID)
	if latest == nil {
		return p.LastSubmittedAt
	}
	if p.LastSubmittedAt == nil || latest.After(*p.LastSubmittedAt) {
		_ = uc.data.DB.Model(p).Update("last_submitted_at", *latest).Error
		p.LastSubmittedAt = latest
	}
	return p.LastSubmittedAt
}

// withinAnalyzeWindow 是否在 AI 分析 6 个月窗口内（以 submit_logs 为准）
// 无任何提交记录：不算近 6 月，不分析（避免 NULL last_submitted_at 虚高待分析后入队即 Ack）
func (uc *ProblemUseCase) withinAnalyzeWindow(p *model.Problem) bool {
	t := uc.refreshLastSubmittedAt(p)
	if t == nil {
		return false
	}
	return !t.Before(time.Now().Add(-backfillWindow))
}

// sqlRecentSubmitCutoff 近 6 月有提交：submit_logs 存在 time>=cutoff 的绑定记录
// 用于 Progress 统计 / ResetAll 入队，与 ProcessAnalyze 窗口一致
func sqlHasRecentSubmit(cutoff time.Time) (clause string, args []interface{}) {
	return `EXISTS (
		SELECT 1 FROM submit_logs s
		WHERE s.problem_id = problems.id
		  AND s.time IS NOT NULL
		  AND s.time >= ?
	)`, []interface{}{cutoff}
}

// Backfill 增量回填（近 6 个月提交）：
// 1) 绑定未关联提交
// 2) 无题面 → 入爬取队列（bulk 优先级）
// 3) 有题面且未分析完 → 入分析队列；已 COMPLETED → 丢弃
// 不清空 MQ（与 ResetQueues 区分）
func (uc *ProblemUseCase) Backfill(limit int) (scanned, bound, created, enqueued, enqueuedFetch, enqueuedAnalyze int64, err error) {
	if limit <= 0 {
		limit = 5000
	}

	// 0) 牛客错误 external_id → 解绑后重解析
	if res := uc.data.DB.Exec(`
		UPDATE submit_logs
		SET problem_id = NULL, external_id = ''
		WHERE platform = ?
		  AND (
		    external_id IS NULL OR external_id = ''
		    OR (
		      external_id !~ '^[0-9]+$'
		      AND external_id !~ '^[0-9a-fA-F]{32}$'
		    )
		  )
	`, spider.NowCoder); res.Error == nil && res.RowsAffected > 0 {
		log.Infof("Backfill: unbound %d NowCoder submits with bad external_id", res.RowsAffected)
	}

	_ = uc.markExistingPermanentFailures()

	// 1) 绑定近 6 个月未关联提交（resolveOne 按状态入爬/分析；已分析则丢弃）
	cutoff := time.Now().Add(-backfillWindow)
	var logs []model.SubmitLog
	err = uc.data.DB.Where("(problem_id IS NULL OR problem_id = 0) AND platform != ?", spider.LeetCode).
		Where("time IS NULL OR time >= ?", cutoff).
		Order("CASE WHEN platform = 'NowCoder' THEN 0 ELSE 1 END, id DESC").
		Limit(limit).Find(&logs).Error
	if err != nil {
		return
	}
	scanned = int64(len(logs))
	for i := range logs {
		_, isNew, rerr := uc.resolveOne(&logs[i], false)
		if rerr != nil {
			continue
		}
		bound++
		if isNew {
			created++
		}
	}

	// 2) 近 6 月有提交的题：无题面补爬；有题面且未完成补分析；COMPLETED 跳过
	recentClause, recentArgs := sqlHasRecentSubmit(cutoff)
	var todos []model.Problem
	_ = uc.data.DB.Where("platform != ?", spider.LeetCode).
		Where("status NOT IN ?", []string{
			model.ProblemStatusSkipped,
			model.ProblemStatusCompleted,
			model.ProblemStatusFailedPerm,
		}).
		Where(recentClause, recentArgs...).
		Order("last_submitted_at DESC NULLS LAST, id DESC").
		Find(&todos).Error

	seen := map[uint]bool{}
	for _, p := range todos {
		if seen[p.ID] {
			continue
		}
		seen[p.ID] = true
		if strings.TrimSpace(p.ContentMD) == "" {
			_ = uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
				Updates(map[string]interface{}{
					"status":           model.ProblemStatusPending,
					"error_msg":        "",
					"fetch_attempts":   0,
					"fetch_fail_since": nil,
				}).Error
			if e := uc.enqueueFetchPrio(p.ID, p.Platform, p.ExternalID, p.URL, mqPriorityBulk); e == nil {
				enqueuedFetch++
				enqueued++
			}
			continue
		}
		// 有题面：入分析（已分析在查询中已排除 COMPLETED；ProcessAnalyze 再丢弃）
		_ = uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
			Updates(map[string]interface{}{
				"status":    model.ProblemStatusTagging,
				"error_msg": "",
			}).Error
		if e := uc.enqueueAnalyzePrio(p.ID, mqPriorityBulk); e == nil {
			enqueuedAnalyze++
			enqueued++
		}
	}
	log.Infof("Backfill: scanned=%d bound=%d created=%d fetch=%d analyze=%d",
		scanned, bound, created, enqueuedFetch, enqueuedAnalyze)
	return
}

// ResetQueues 重置 MQ：purge 爬取/分析队列，再按 DB 待爬取/待分析重灌（bulk 优先级）
// 与 Backfill 不同：不扫提交、不改业务状态，只重建队列。
func (uc *ProblemUseCase) ResetQueues() (purgedFetch, purgedAnalyze, enqueuedFetch, enqueuedAnalyze int, err error) {
	if n, e := uc.purgeFetchQueue(); e == nil {
		purgedFetch = n
	} else if err == nil {
		err = e
	}
	if n, e := uc.purgeAnalyzeQueue(); e == nil {
		purgedAnalyze = n
	} else if err == nil {
		err = e
	}

	// 待爬取：PENDING / FETCHING（卡住的 FETCHING 一并重入）
	var fetchTodos []model.Problem
	_ = uc.data.DB.Where("platform != ?", spider.LeetCode).
		Where("status IN ?", []string{model.ProblemStatusPending, model.ProblemStatusFetching}).
		Where("(content_md IS NULL OR content_md = '')").
		Order("last_submitted_at DESC NULLS LAST, id DESC").
		Find(&fetchTodos).Error
	for _, p := range fetchTodos {
		_ = uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
			Update("status", model.ProblemStatusPending).Error
		if e := uc.enqueueFetchPrio(p.ID, p.Platform, p.ExternalID, p.URL, mqPriorityBulk); e == nil {
			enqueuedFetch++
		}
	}

	// 待分析：TAGGING + 有题面；已 COMPLETED 不入队
	cutoff := time.Now().Add(-backfillWindow)
	recentClause, recentArgs := sqlHasRecentSubmit(cutoff)
	var analyzeTodos []model.Problem
	_ = uc.data.DB.Where("platform != ?", spider.LeetCode).
		Where("status = ?", model.ProblemStatusTagging).
		Where("content_md IS NOT NULL AND content_md != ''").
		Where(recentClause, recentArgs...).
		Order("last_submitted_at DESC NULLS LAST, id DESC").
		Find(&analyzeTodos).Error
	for _, p := range analyzeTodos {
		if e := uc.enqueueAnalyzePrio(p.ID, mqPriorityBulk); e == nil {
			enqueuedAnalyze++
		}
	}
	log.Infof("ResetQueues: purged_fetch=%d purged_analyze=%d enq_fetch=%d enq_analyze=%d",
		purgedFetch, purgedAnalyze, enqueuedFetch, enqueuedAnalyze)
	return
}

// RetryFailed 重试错误队列：仅重入 FAILED（可重试失败），排除 FAILED_PERM 黑名单
// 会先把永久错误升级为 FAILED_PERM，并解除误标的 WAF/登录墙 FAILED_PERM
func (uc *ProblemUseCase) RetryFailed(limit int) (scanned, enqueued, blacklisted int64, err error) {
	pipelineControl.SetAnalyzePaused(false)
	pipelineControl.SetFetchPaused(false)

	// 解除误标：WAF/登录墙不应进黑名单（历史曾标 FAILED_PERM）
	if res := uc.data.DB.Model(&model.Problem{}).
		Where("status = ?", model.ProblemStatusFailedPerm).
		Where("error_msg LIKE ? OR error_msg LIKE ? OR error_msg LIKE ?",
			"%需要登录%", "%被拦截%", "%WAF%").
		Updates(map[string]interface{}{
			"status":    model.ProblemStatusFailed,
			"error_msg": "retry: was false permanent (WAF/login)",
		}); res.Error == nil && res.RowsAffected > 0 {
		log.Infof("RetryFailed: unblocked %d false FAILED_PERM (WAF/login)", res.RowsAffected)
	}

	// 先把已是永久错误文案的 FAILED 升为黑名单
	blacklisted = uc.markExistingPermanentFailures()

	// 仅近 6 个月；新题优先
	cutoff := time.Now().Add(-backfillWindow)
	q := uc.data.DB.Where("status = ?", model.ProblemStatusFailed).
		Where("platform != ?", spider.LeetCode).
		Where("last_submitted_at IS NULL OR last_submitted_at >= ?", cutoff).
		Order("last_submitted_at DESC NULLS LAST, id DESC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	var todos []model.Problem
	if err = q.Find(&todos).Error; err != nil {
		return
	}
	scanned = int64(len(todos))

	seen := map[uint]bool{}
	for _, p := range todos {
		if seen[p.ID] {
			continue
		}
		seen[p.ID] = true
		// 双保险：error_msg 已是永久错误 → 黑名单，不入队
		if isPermanentFetchError(p.ErrorMsg) {
			_ = uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
				Update("status", model.ProblemStatusFailedPerm).Error
			blacklisted++
			continue
		}
		if strings.TrimSpace(p.ContentMD) != "" {
			_ = uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
				Updates(map[string]interface{}{
					"status":    model.ProblemStatusTagging,
					"error_msg": "",
				}).Error
			if e := uc.enqueueAnalyze(p.ID); e == nil {
				enqueued++
			}
		} else {
			_ = uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
				Updates(map[string]interface{}{
					"status":    model.ProblemStatusPending,
					"error_msg": "",
				}).Error
			if e := uc.enqueueFetch(p.ID, p.Platform, p.ExternalID, p.URL); e == nil {
				enqueued++
			}
		}
	}
	return
}

// markExistingPermanentFailures 将历史 FAILED 中匹配永久错误文案的标为 FAILED_PERM
func (uc *ProblemUseCase) markExistingPermanentFailures() int64 {
	var list []model.Problem
	_ = uc.data.DB.Where("status = ?", model.ProblemStatusFailed).
		Where("error_msg IS NOT NULL AND error_msg != ''").
		Find(&list).Error
	var n int64
	for _, p := range list {
		if !isPermanentFetchError(p.ErrorMsg) {
			continue
		}
		if err := uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
			Update("status", model.ProblemStatusFailedPerm).Error; err == nil {
			n++
		}
	}
	if n > 0 {
		log.Infof("markExistingPermanentFailures: %d → FAILED_PERM", n)
	}
	return n
}

type ListProblemFilter struct {
	Page       int64
	PageSize   int64
	Sort       string
	Platforms  []string
	Tags       []string
	UserStatus string
	UserID     int64
	Keyword    string
	Difficulty string
}

func (uc *ProblemUseCase) List(f ListProblemFilter) ([]model.Problem, map[uint]string, int64, error) {
	if f.Page <= 0 {
		f.Page = 1
	}
	if f.PageSize <= 0 {
		f.PageSize = 20
	}
	q := uc.data.DB.Model(&model.Problem{})
	if len(f.Platforms) > 0 {
		q = q.Where("platform IN ?", f.Platforms)
	}
	if len(f.Tags) > 0 {
		// jsonb 数组包含任一 tag（OR）
		// 禁止 tags::jsonb ? ?（GORM 占位符冲突）
		// 禁止 jsonb_build_array(?)（PG 无法推断 $1 类型 → 42P18）
		// 用 CAST(? AS jsonb) 传入 JSON 数组字面量，类型明确
		ors := make([]string, 0, len(f.Tags))
		args := make([]interface{}, 0, len(f.Tags))
		for _, t := range f.Tags {
			t = strings.TrimSpace(t)
			if t == "" {
				continue
			}
			b, err := json.Marshal([]string{t})
			if err != nil {
				continue
			}
			ors = append(ors, "tags::jsonb @> CAST(? AS jsonb)")
			args = append(args, string(b))
		}
		if len(ors) > 0 {
			q = q.Where(strings.Join(ors, " OR "), args...)
		}
	}
	if kw := strings.TrimSpace(f.Keyword); kw != "" {
		like := "%" + kw + "%"
		q = q.Where("(title ILIKE ? OR external_id ILIKE ?)", like, like)
	}
	if d := strings.TrimSpace(f.Difficulty); d != "" {
		q = q.Where("difficulty = ?", d)
	}

	// 用户状态：用 SQL 聚合，避免拉全量 submit_logs
	userStatusMap := map[uint]string{}
	if f.UserID > 0 {
		if f.UserStatus != "" {
			want := strings.ToUpper(strings.TrimSpace(f.UserStatus))
			switch want {
			case "NONE":
				q = q.Where(`NOT EXISTS (
					SELECT 1 FROM submit_logs s
					WHERE s.problem_id = problems.id AND s.user_id = ?
				)`, f.UserID)
			case "AC":
				// 与 isACStatus / 画像统计一致：按 problem_id 关联
				q = q.Where(`EXISTS (
					SELECT 1 FROM submit_logs s
					WHERE s.problem_id = problems.id AND s.user_id = ?
					  AND (`+sqlACStatusCond("s.status")+`)
				)`, f.UserID)
			case "TRIED":
				q = q.Where(`EXISTS (
					SELECT 1 FROM submit_logs s
					WHERE s.problem_id = problems.id AND s.user_id = ?
				) AND NOT EXISTS (
					SELECT 1 FROM submit_logs s
					WHERE s.problem_id = problems.id AND s.user_id = ?
					  AND (`+sqlACStatusCond("s.status")+`)
				)`, f.UserID, f.UserID)
			}
		}
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, nil, 0, err
	}
	// 固定：最近提交降序（不再提供其它排序）
	order := "last_submitted_at DESC NULLS LAST, id DESC"
	var list []model.Problem
	err := q.Order(order).Offset(int((f.Page - 1) * f.PageSize)).Limit(int(f.PageSize)).Find(&list).Error
	if err != nil {
		return nil, nil, 0, err
	}

	// 仅补当前页的用户状态
	if f.UserID > 0 && len(list) > 0 {
		ids := make([]uint, 0, len(list))
		for i := range list {
			ids = append(ids, list[i].ID)
		}
		type row struct {
			ProblemID uint
			Status    string
		}
		var rows []row
		_ = uc.data.DB.Model(&model.SubmitLog{}).
			Select("problem_id, status").
			Where("user_id = ? AND problem_id IN ?", f.UserID, ids).
			Find(&rows).Error
		for _, r := range rows {
			if r.ProblemID == 0 {
				continue
			}
			cur := userStatusMap[r.ProblemID]
			ns := mapSubmitStatus(r.Status)
			if rankStatus(ns) > rankStatus(cur) {
				userStatusMap[r.ProblemID] = ns
			}
		}
	}
	return list, userStatusMap, total, nil
}

// TagCount 标签及题目数（用于筛选器）
type TagCount struct {
	Tag   string
	Count int64
}

// ListTags 聚合已有算法标签及题量，按 count 降序
func (uc *ProblemUseCase) ListTags(limit int) ([]TagCount, error) {
	if limit <= 0 {
		limit = 100
	}
	if limit > 300 {
		limit = 300
	}
	var rows []TagCount
	err := uc.data.DB.Raw(`
		SELECT tag, COUNT(DISTINCT p.id) AS count
		FROM problems p
		CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(p.tags::jsonb, '[]'::jsonb)) AS tag
		WHERE p.status = ?
		  AND p.tags IS NOT NULL
		  AND p.tags::text NOT IN ('', '[]', 'null')
		  AND BTRIM(tag) <> ''
		GROUP BY tag
		ORDER BY count DESC, tag ASC
		LIMIT ?
	`, model.ProblemStatusCompleted, limit).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	return rows, nil
}

func (uc *ProblemUseCase) Get(id uint) (*model.Problem, error) {
	var p model.Problem
	if err := uc.data.DB.First(&p, id).Error; err != nil {
		return nil, err
	}
	return &p, nil
}

func (uc *ProblemUseCase) ListSubmissions(problemID uint, userID, page, pageSize int64) ([]model.SubmitLog, int64, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	q := uc.data.DB.Model(&model.SubmitLog{}).Where("problem_id = ?", problemID)
	if userID > 0 {
		q = q.Where("user_id = ?", userID)
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []model.SubmitLog
	err := q.Order("time desc").Offset(int((page - 1) * pageSize)).Limit(int(pageSize)).Find(&list).Error
	return list, total, err
}

// UserProfile 纯 SQL 聚合画像
func (uc *ProblemUseCase) UserProfile(userID int64) (radar []struct {
	Tag     string
	Score   float64
	ACCount int64
}, platforms []struct {
	Name  string
	Count int64
}, difficulties []struct {
	Name  string
	Count int64
}, totalAC int64, err error) {

	// AC 判定必须用 s.status：JOIN problems 后裸 status 会与 p.status 歧义，SQL 失败被吞掉导致雷达/平台全空
	acCond := sqlACStatusCond("s.status")

	type tagRow struct {
		Tag   string
		Count int64
	}
	var tags []tagRow
	// jsonb_array_elements_text；仅 COMPLETED 有 AI 标签
	err = uc.data.DB.Raw(`
		SELECT tag, COUNT(DISTINCT p.id) AS count
		FROM submit_logs s
		JOIN problems p ON p.id = s.problem_id
		CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(p.tags::jsonb, '[]'::jsonb)) AS tag
		WHERE s.user_id = ? AND `+acCond+` AND p.status = ?
		  AND p.tags IS NOT NULL AND p.tags::text NOT IN ('', '[]', 'null')
		GROUP BY tag
		ORDER BY count DESC
		LIMIT 20
	`, userID, model.ProblemStatusCompleted).Scan(&tags).Error
	if err != nil {
		log.Errorf("radar sql user=%d: %v", userID, err)
		err = nil
	}

	// 归一化：max 为 100
	var maxC int64
	for _, t := range tags {
		if t.Count > maxC {
			maxC = t.Count
		}
	}
	for _, t := range tags {
		score := 0.0
		if maxC > 0 {
			score = math.Round(float64(t.Count)/float64(maxC)*1000) / 10
		}
		radar = append(radar, struct {
			Tag     string
			Score   float64
			ACCount int64
		}{Tag: t.Tag, Score: score, ACCount: t.Count})
	}

	type nc struct {
		Name  string
		Count int64
	}
	var plats []nc
	if e := uc.data.DB.Raw(`
		SELECT p.platform AS name, COUNT(DISTINCT p.id) AS count
		FROM submit_logs s
		JOIN problems p ON p.id = s.problem_id
		WHERE s.user_id = ? AND `+acCond+`
		GROUP BY p.platform
	`, userID).Scan(&plats).Error; e != nil {
		log.Errorf("platforms sql user=%d: %v", userID, e)
	}
	for _, p := range plats {
		platforms = append(platforms, struct {
			Name  string
			Count int64
		}{p.Name, p.Count})
	}

	var diffs []nc
	if e := uc.data.DB.Raw(`
		SELECT p.difficulty AS name, COUNT(DISTINCT p.id) AS count
		FROM submit_logs s
		JOIN problems p ON p.id = s.problem_id
		WHERE s.user_id = ? AND `+acCond+`
		  AND p.difficulty IS NOT NULL AND BTRIM(p.difficulty) <> ''
		  AND UPPER(BTRIM(p.difficulty)) NOT IN ('UNKNOWN','NULL','NONE')
		GROUP BY p.difficulty
	`, userID).Scan(&diffs).Error; e != nil {
		log.Errorf("difficulties sql user=%d: %v", userID, e)
	}
	for _, d := range diffs {
		difficulties = append(difficulties, struct {
			Name  string
			Count int64
		}{d.Name, d.Count})
	}

	_ = uc.data.DB.Raw(`
		SELECT COUNT(DISTINCT s.problem_id) FROM submit_logs s
		WHERE s.user_id = ? AND s.problem_id IS NOT NULL AND s.problem_id <> 0 AND `+acCond+`
	`, userID).Scan(&totalAC).Error

	return
}

type ProgressSnapshot struct {
	Items      []struct {
		Status string
		Count  int64
	}
	Failed         []model.Problem
	FailedPerm     []model.Problem
	InProgress     []model.Problem
	Total          int64
	Paused         bool // AI 暂停（兼容）
	FetchPaused    bool
	AnalyzePaused  bool
	ActiveJobs     []ActiveJob
	Queues         []struct {
		Name        string
		Messages    int64
		Consumers   int64
		Concurrency int64
	}
}

func (uc *ProblemUseCase) Progress() (ProgressSnapshot, error) {
	var snap ProgressSnapshot
	type sc struct {
		Status string
		Count  int64
	}
	// 全量：PENDING / FETCHING / COMPLETED
	// 近 6 个月：以 submit_logs 有 time>=cutoff 为准（与 ProcessAnalyze 一致，禁止 NULL 虚高）
	cutoff := time.Now().Add(-backfillWindow)
	recentClause, recentArgs := sqlHasRecentSubmit(cutoff)
	fullStatuses := []string{
		model.ProblemStatusPending,
		model.ProblemStatusFetching,
		model.ProblemStatusCompleted,
	}
	var rows []sc
	if err := uc.data.DB.Model(&model.Problem{}).
		Select("status, count(*) as count").
		Where("status IN ?", fullStatuses).
		Group("status").Scan(&rows).Error; err != nil {
		return snap, err
	}
	var recent []sc
	if err := uc.data.DB.Model(&model.Problem{}).
		Select("status, count(*) as count").
		Where("status NOT IN ?", fullStatuses).
		Where(recentClause, recentArgs...).
		Group("status").Scan(&recent).Error; err != nil {
		return snap, err
	}
	rows = append(rows, recent...)
	for _, r := range rows {
		snap.Items = append(snap.Items, struct {
			Status string
			Count  int64
		}{r.Status, r.Count})
		snap.Total += r.Count
	}
	_ = uc.data.DB.Where("status = ?", model.ProblemStatusFailed).
		Where(recentClause, recentArgs...).
		Order("updated_at desc").Limit(20).Find(&snap.Failed).Error
	_ = uc.data.DB.Where("status = ?", model.ProblemStatusFailedPerm).
		Where(recentClause, recentArgs...).
		Order("updated_at desc").Limit(50).Find(&snap.FailedPerm).Error
	// 爬取中全量；待分析仅近 6 个月（submit_logs）
	_ = uc.data.DB.Where(
		"(status = ?) OR (status = ? AND "+recentClause+")",
		append([]interface{}{model.ProblemStatusFetching, model.ProblemStatusTagging}, recentArgs...)...,
	).Order("updated_at desc").Limit(30).Find(&snap.InProgress).Error

	snap.Paused = pipelineControl.IsAnalyzePaused()
	snap.FetchPaused = pipelineControl.IsFetchPaused()
	snap.AnalyzePaused = pipelineControl.IsAnalyzePaused()
	snap.ActiveJobs = pipelineControl.SnapshotActive()
	snap.Queues = uc.queueStats()
	return snap, nil
}

func (uc *ProblemUseCase) queueStats() []struct {
	Name        string
	Messages    int64
	Consumers   int64
	Concurrency int64
} {
	out := make([]struct {
		Name        string
		Messages    int64
		Consumers   int64
		Concurrency int64
	}, 0, 2)
	for _, q := range []struct {
		name string
		conc int64
		stat string
	}{
		{"problem_fetch", problemFetchConcurrency, model.ProblemStatusPending},
		{"problem_analyze", problemAnalyzeConcurrency, model.ProblemStatusTagging},
	} {
		var msgs, consumers int64
		inspected := false
		// 优先读真实 MQ 积压/消费者
		if uc.mq != nil {
			if info, err := uc.mq.QueueInspect(q.name); err == nil {
				msgs = int64(info.Messages)
				consumers = int64(info.Consumers)
				inspected = true
			}
		}
		// inspect 失败时用 DB 近似积压
		if !inspected {
			cq := uc.data.DB.Model(&model.Problem{}).Where("status = ?", q.stat)
			// 分析队列仅近 6 个月（submit_logs）；爬取队列全量
			if q.name == "problem_analyze" {
				cutoff := time.Now().Add(-backfillWindow)
				rc, ra := sqlHasRecentSubmit(cutoff)
				cq = cq.Where(rc, ra...)
			}
			_ = cq.Count(&msgs).Error
			if q.name == "problem_fetch" {
				var fetching int64
				_ = uc.data.DB.Model(&model.Problem{}).Where("status = ?", model.ProblemStatusFetching).Count(&fetching).Error
				msgs += fetching
			}
		}
		out = append(out, struct {
			Name        string
			Messages    int64
			Consumers   int64
			Concurrency int64
		}{q.name, msgs, consumers, q.conc})
	}
	return out
}

func (uc *ProblemUseCase) purgeQueue(name string) (int, error) {
	if uc.mq == nil {
		return 0, fmt.Errorf("mq not ready")
	}
	_ = uc.declareProblemQueue(name)
	return uc.mq.QueuePurge(name, false)
}

func (uc *ProblemUseCase) purgeAnalyzeQueue() (purgedAnalyze int, err error) {
	return uc.purgeQueue("problem_analyze")
}

func (uc *ProblemUseCase) purgeFetchQueue() (purgedFetch int, err error) {
	return uc.purgeQueue("problem_fetch")
}

// PauseAnalyze 暂停 AI 分析（保留队列消息，恢复后继续消费）
func (uc *ProblemUseCase) PauseAnalyze() (purged int, err error) {
	pipelineControl.SetAnalyzePaused(true)
	return 0, nil
}

// ResumeAnalyze 恢复 AI 分析
func (uc *ProblemUseCase) ResumeAnalyze() {
	pipelineControl.SetAnalyzePaused(false)
}

// PauseFetch 暂停题面爬取（保留队列消息，恢复后继续消费）
func (uc *ProblemUseCase) PauseFetch() (purged int, err error) {
	pipelineControl.SetFetchPaused(true)
	return 0, nil
}

// ResumeFetch 恢复题面爬取
func (uc *ProblemUseCase) ResumeFetch() {
	pipelineControl.SetFetchPaused(false)
}

// EmergencyStop 兼容旧 API：暂停 AI（不再 purge；清队列请用 ResetQueues）
func (uc *ProblemUseCase) EmergencyStop() (purgedFetch, purgedAnalyze int, err error) {
	_, err = uc.PauseAnalyze()
	return 0, 0, err
}

// Resume 兼容旧 API：恢复 AI
func (uc *ProblemUseCase) Resume() {
	uc.ResumeAnalyze()
}

func (uc *ProblemUseCase) ProgressPausedAnalyze() bool {
	return pipelineControl.IsAnalyzePaused()
}

func (uc *ProblemUseCase) ProgressPausedFetch() bool {
	return pipelineControl.IsFetchPaused()
}

// ResetAll 仅重置 AI 分析结果（保留 content_md 题面），清空 AI 队列并可选重新入队分析
// 顺序必须是：暂停 → 清空队列 → 改 DB → 恢复暂停 → 再入队
// 若在暂停期间入队，消费者会把消息 Ack 丢掉（只剩碰巧在恢复后取出的少数任务）。
func (uc *ProblemUseCase) ResetAll(requeue bool) (reset, enqueued, purgedFetch, purgedAnalyze int, err error) {
	pipelineControl.SetAnalyzePaused(true)
	purgedAnalyze, err = uc.purgeAnalyzeQueue()
	if err != nil {
		pipelineControl.SetAnalyzePaused(false)
		return
	}
	// 清除分析字段，保留题面 content_md；有题面的回到 TAGGING，无题面保持 PENDING
	// 1) 有题面：清标签/难度/解法，状态 TAGGING
	res := uc.data.DB.Model(&model.Problem{}).
		Where("status IN ?", []string{
			model.ProblemStatusCompleted,
			model.ProblemStatusTagging,
			model.ProblemStatusFailed,
		}).
		Where("content_md IS NOT NULL AND content_md != ''").
		Where("platform != ?", spider.LeetCode).
		Updates(map[string]interface{}{
			"status":         model.ProblemStatusTagging,
			"problem_type":   "",
			"difficulty":     "",
			"tags":           model.StringArray{},
			"solutions_meta": model.SolutionsMeta{},
			"error_msg":      "",
		})
	if res.Error != nil {
		err = res.Error
		pipelineControl.SetAnalyzePaused(false)
		return
	}
	reset = int(res.RowsAffected)

	// 2) 无题面的失败/卡住：只清错误，不回删题面（本来就没有）
	res2 := uc.data.DB.Model(&model.Problem{}).
		Where("status IN ?", []string{model.ProblemStatusFailed, model.ProblemStatusFetching}).
		Where("(content_md IS NULL OR content_md = '')").
		Where("platform != ?", spider.LeetCode).
		Updates(map[string]interface{}{
			"status":    model.ProblemStatusPending,
			"error_msg": "",
		})
	if res2.Error == nil {
		reset += int(res2.RowsAffected)
	}

	// 先恢复再入队，避免 paused 期间消息被 Ack 丢弃
	pipelineControl.SetAnalyzePaused(false)

	if requeue {
		// 批量回写 last_submitted_at ← submit_logs.MAX(time)
		_ = uc.data.DB.Exec(`
			UPDATE problems p
			SET last_submitted_at = s.mx
			FROM (
				SELECT problem_id, MAX(time) AS mx
				FROM submit_logs
				WHERE problem_id IS NOT NULL AND problem_id <> 0
				GROUP BY problem_id
			) s
			WHERE p.id = s.problem_id
			  AND (p.last_submitted_at IS NULL OR p.last_submitted_at < s.mx)
		`).Error

		// 仅：有题面 + TAGGING + submit_logs 近 6 月有提交（禁止 NULL 虚入队）
		cutoff := time.Now().Add(-backfillWindow)
		recentClause, recentArgs := sqlHasRecentSubmit(cutoff)
		var list []model.Problem
		q := uc.data.DB.Where("status = ? AND platform != ?", model.ProblemStatusTagging, spider.LeetCode).
			Where("content_md IS NOT NULL AND content_md != ''").
			Where(recentClause, recentArgs...).
			Order("last_submitted_at DESC NULLS LAST, id DESC")
		_ = q.Find(&list).Error
		for _, p := range list {
			if e := uc.enqueueAnalyze(p.ID); e == nil {
				enqueued++
			}
		}
		log.Infof("ResetAll: reset=%d analyze_enqueued=%d (enqueue after unpause)", reset, enqueued)
	}
	return
}

func truncateErr(s string) string {
	if len(s) > 500 {
		return s[:500]
	}
	return s
}

// handleFetchError 爬取失败：瞬时错误退避重试，持续超 24h 或非瞬时满 3 次 → FAILED_PERM
func (uc *ProblemUseCase) handleFetchError(p *model.Problem, err error) error {
	msg := truncateErr(err.Error())
	attempts := p.FetchAttempts + 1
	st := model.ProblemStatusFailed
	updates := map[string]interface{}{
		"fetch_attempts": attempts,
		"error_msg":      msg,
	}

	if isPermanentFetchError(msg) {
		st = model.ProblemStatusFailedPerm
		updates["status"] = st
		updates["fetch_fail_since"] = nil
		_ = uc.data.DB.Model(p).Updates(updates).Error
		return nil
	}

	if isTransientFetchError(msg) {
		// 记录首次瞬时失败时间
		failSince := p.FetchFailSince
		if failSince == nil {
			now := time.Now()
			failSince = &now
			updates["fetch_fail_since"] = now
		}
		if time.Since(*failSince) >= transientFailWindow {
			st = model.ProblemStatusFailedPerm
			msg = fmt.Sprintf("瞬时失败超过24小时: %s", msg)
			updates["status"] = st
			updates["error_msg"] = truncateErr(msg)
			updates["fetch_fail_since"] = nil
			_ = uc.data.DB.Model(p).Updates(updates).Error
			return nil
		}
		// 退避等待后再让消费者 requeue，避免 405 热循环
		wait := transientBackoff(attempts)
		msg = fmt.Sprintf("瞬时失败(退避%v, 自%s起可重试至24h): %s",
			wait.Round(time.Second), failSince.Format("01-02 15:04"), msg)
		updates["status"] = st
		updates["error_msg"] = truncateErr(msg)
		_ = uc.data.DB.Model(p).Updates(updates).Error
		log.Warnf("problem %d fetch transient, sleep %v: %s", p.ID, wait, msg)
		time.Sleep(wait)
		return err
	}

	// 普通可恢复错误：满 3 次 → 永久
	if attempts >= maxFetchAttempts {
		st = model.ProblemStatusFailedPerm
		msg = fmt.Sprintf("爬取失败超过%d次: %s", maxFetchAttempts, msg)
		updates["status"] = st
		updates["error_msg"] = truncateErr(msg)
		updates["fetch_fail_since"] = nil
		_ = uc.data.DB.Model(p).Updates(updates).Error
		return nil
	}
	updates["status"] = st
	_ = uc.data.DB.Model(p).Updates(updates).Error
	return err
}

// transientBackoff 405/WAF 退避：30s → 1m → 2m → 5m → 10m（封顶）
func transientBackoff(attempts int) time.Duration {
	if attempts < 1 {
		attempts = 1
	}
	switch {
	case attempts <= 1:
		return 30 * time.Second
	case attempts == 2:
		return time.Minute
	case attempts == 3:
		return 2 * time.Minute
	case attempts == 4:
		return 5 * time.Minute
	default:
		return 10 * time.Minute
	}
}

// isTransientFetchError 瞬时/风控类错误：退避重试，满 24h 才升 FAILED_PERM
func isTransientFetchError(msg string) bool {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return false
	}
	return strings.Contains(msg, "Cloudflare") ||
		strings.Contains(msg, "请稍后重试") ||
		strings.Contains(msg, "WAF") ||
		strings.Contains(msg, "需要登录") ||
		strings.Contains(msg, "被拦截") ||
		strings.Contains(msg, "status 405") ||
		strings.Contains(msg, "status 403") ||
		strings.Contains(msg, "status 429") ||
		strings.Contains(msg, "status 503") ||
		strings.Contains(msg, "status 502") ||
		strings.Contains(msg, "timeout") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "瞬时失败")
}

// isPermanentFetchError 不可恢复的爬取错误：不再重试、不再入队（软黑名单 FAILED_PERM）
// 例如 CF/洛谷/牛客「未找到题面」、无 URL、不支持平台等
// 注意：WAF/登录墙/Cloudflare/405 等拦截类一律可重试，不进黑名单
func isPermanentFetchError(msg string) bool {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return false
	}
	if isTransientFetchError(msg) {
		return false
	}
	permanent := []string{
		"未找到题面",
		"未找到题面 DOM",
		"无法解析 CF external_id",
		"LeetCode 不支持爬取",
		"不支持的平台",
		"缺少题面 URL",
		"竞赛题无稳定题面 URL",
		"AtCoder 缺少 URL",
		"empty url",
		"题面为空",
		"JSON 无题面",
	}
	for _, p := range permanent {
		if strings.Contains(msg, p) {
			return true
		}
	}
	return false
}

// isACStatus 是否算通过（与 AC 数量统计同源）
// 覆盖：AC / OK / Accepted / 答案正确 / 通过 等
func isACStatus(s string) bool {
	s = strings.TrimSpace(s)
	if s == "" {
		return false
	}
	u := strings.ToUpper(s)
	if u == "AC" || u == "OK" || u == "ACCEPT" || u == "ACCEPTED" {
		return true
	}
	if strings.Contains(u, "ACCEPT") { // Accepted, Partially Accepted 等 — 全 AC 平台通常不带 Partially
		// CF: OK；部分平台写 Accepted
		if strings.Contains(u, "PARTIAL") || strings.Contains(u, "部分") {
			return false
		}
		return true
	}
	// 中文（牛客等）
	if strings.Contains(s, "答案正确") || s == "通过" || strings.Contains(s, "完全正确") {
		return true
	}
	// AtCoder 等
	if u == "AC" || strings.HasPrefix(u, "AC ") {
		return true
	}
	return false
}

// sqlACStatusCond 生成 SQL 片段，col 为列名（可带表别名，如 s.status）
func sqlACStatusCond(col string) string {
	return `(` +
		`UPPER(` + col + `) IN ('AC','OK','ACCEPT','ACCEPTED')` +
		` OR (UPPER(` + col + `) LIKE '%ACCEPT%' AND UPPER(` + col + `) NOT LIKE '%PARTIAL%')` +
		` OR ` + col + ` LIKE '%答案正确%'` +
		` OR ` + col + ` = '通过'` +
		` OR ` + col + ` LIKE '%完全正确%'` +
		`)`
}

func mapSubmitStatus(s string) string {
	if isACStatus(s) {
		return "AC"
	}
	if strings.TrimSpace(s) == "" {
		return "NONE"
	}
	return "TRIED"
}

func rankStatus(s string) int {
	switch s {
	case "AC":
		return 3
	case "TRIED":
		return 2
	case "NONE":
		return 1
	default:
		return 0
	}
}

func firstNonEmpty(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}
