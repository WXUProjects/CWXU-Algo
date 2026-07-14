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

// BindSubmitsAfterSpider 爬虫写入提交后绑定/创建题库
func (uc *ProblemUseCase) BindSubmitsAfterSpider(userId int64) {
	var logs []model.SubmitLog
	// 仅处理未绑定的
	if err := uc.data.DB.Where("user_id = ? AND (problem_id IS NULL OR problem_id = 0)", userId).
		Order("id desc").Limit(500).Find(&logs).Error; err != nil {
		log.Errorf("BindSubmitsAfterSpider query: %v", err)
		return
	}
	for i := range logs {
		if _, _, err := uc.resolveOne(&logs[i]); err != nil {
			log.Debugf("resolve submit %d: %v", logs[i].ID, err)
		}
	}
}

// resolveOne 解析并绑定单条提交；返回 (problem, isNew, err)
func (uc *ProblemUseCase) resolveOne(sl *model.SubmitLog) (*model.Problem, bool, error) {
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

	// 新题且可爬 → 入队抓题面 + AI
	if isNew && !parsed.SkipFetch && existing.Status == model.ProblemStatusPending {
		if err := uc.enqueueFetch(existing.ID, existing.Platform, existing.ExternalID, existing.URL); err != nil {
			log.Errorf("enqueue problem %d: %v", existing.ID, err)
		}
	}
	// 已存在但题面未完成：补入队（例如之前失败/卡住）
	if !isNew && !parsed.SkipFetch {
		switch existing.Status {
		case model.ProblemStatusPending, model.ProblemStatusFailed:
			if strings.TrimSpace(existing.ContentMD) == "" {
				if err := uc.enqueueFetch(existing.ID, existing.Platform, existing.ExternalID, existing.URL); err != nil {
					log.Errorf("re-enqueue fetch problem %d: %v", existing.ID, err)
				}
			} else if !pipelineControl.IsAnalyzePaused() {
				_ = uc.data.DB.Model(&existing).Update("status", model.ProblemStatusTagging).Error
				if err := uc.enqueueAnalyze(existing.ID); err != nil {
					log.Errorf("re-enqueue analyze problem %d: %v", existing.ID, err)
				}
			}
		case model.ProblemStatusTagging:
			if strings.TrimSpace(existing.ContentMD) != "" && !pipelineControl.IsAnalyzePaused() {
				if err := uc.enqueueAnalyze(existing.ID); err != nil {
					log.Errorf("re-enqueue analyze problem %d: %v", existing.ID, err)
				}
			}
		}
	}
	return &existing, isNew, nil
}

func (uc *ProblemUseCase) enqueueFetch(id uint, platform, externalID, url string) error {
	if uc.mq == nil || uc.mq.Ch == nil {
		return fmt.Errorf("mq not ready")
	}
	body, _ := json.Marshal(event.ProblemFetchEvent{
		ProblemID:  id,
		Platform:   platform,
		ExternalID: externalID,
		URL:        url,
	})
	if _, err := uc.mq.Ch.QueueDeclare("problem_fetch", true, false, false, false, nil); err != nil {
		return err
	}
	return uc.mq.Ch.Publish("", "problem_fetch", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
}

func (uc *ProblemUseCase) enqueueAnalyze(id uint) error {
	if uc.mq == nil || uc.mq.Ch == nil {
		return fmt.Errorf("mq not ready")
	}
	body, _ := json.Marshal(event.ProblemAnalyzeEvent{ProblemID: id})
	if _, err := uc.mq.Ch.QueueDeclare("problem_analyze", true, false, false, false, nil); err != nil {
		return err
	}
	return uc.mq.Ch.Publish("", "problem_analyze", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
	})
}

// ProcessFetch 仅爬取题面；成功后状态 TAGGING 并投递 AI 队列（不受 AI 紧急停止影响）
func (uc *ProblemUseCase) ProcessFetch(ctx context.Context, ev event.ProblemFetchEvent) error {
	var p model.Problem
	if err := uc.data.DB.First(&p, ev.ProblemID).Error; err != nil {
		return err
	}
	pipelineControl.TrackStart("fetch", p.ID, p.Platform, p.ExternalID, p.Title)
	defer pipelineControl.TrackEnd("fetch", p.ID)
	if p.Status == model.ProblemStatusCompleted || p.Status == model.ProblemStatusTagging {
		// 已有题面则直接补 AI
		if p.ContentMD != "" && p.Status != model.ProblemStatusCompleted {
			return uc.enqueueAnalyze(p.ID)
		}
		return nil
	}
	if p.Platform == spider.LeetCode || p.Status == model.ProblemStatusSkipped {
		_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
			"status": model.ProblemStatusSkipped,
		}).Error
		return nil
	}
	// 已有 content 的失败题：跳过爬取，只重试 AI
	if p.ContentMD != "" {
		_ = uc.data.DB.Model(&p).Update("status", model.ProblemStatusTagging).Error
		return uc.enqueueAnalyze(p.ID)
	}

	res := uc.data.DB.Model(&model.Problem{}).
		Where("id = ? AND status IN ?", p.ID, []string{model.ProblemStatusPending, model.ProblemStatusFailed, model.ProblemStatusFetching}).
		Update("status", model.ProblemStatusFetching)
	if res.Error != nil {
		return res.Error
	}

	url := p.URL
	if url == "" {
		url = ev.URL
	}
	fetched, err := problem_fetch.Fetch(p.Platform, p.ExternalID, url)
	if err != nil {
		_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
			"status":    model.ProblemStatusFailed,
			"error_msg": truncateErr(err.Error()),
		}).Error
		return err
	}

	title := p.Title
	if fetched.Title != "" {
		title = fetched.Title
	}
	updates := map[string]interface{}{
		"content_md": fetched.ContentMD,
		"title":      title,
		"error_msg":  "",
		"status":     model.ProblemStatusTagging,
	}
	if p.URL == "" && url != "" {
		updates["url"] = url
	}
	if err := uc.data.DB.Model(&p).Updates(updates).Error; err != nil {
		return err
	}
	return uc.enqueueAnalyze(p.ID)
}

// ProcessAnalyze 仅 AI 打标（不爬取、不送用户代码）
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
	if p.Status == model.ProblemStatusCompleted {
		return nil
	}
	if p.Status == model.ProblemStatusSkipped || p.Platform == spider.LeetCode {
		return nil
	}
	if strings.TrimSpace(p.ContentMD) == "" {
		// 无题面，退回爬取
		_ = uc.data.DB.Model(&p).Update("status", model.ProblemStatusPending).Error
		return uc.enqueueFetch(p.ID, p.Platform, p.ExternalID, p.URL)
	}

	if uc.tagger == nil {
		return uc.data.DB.Model(&p).Updates(map[string]interface{}{
			"status":    model.ProblemStatusFailed,
			"error_msg": "ai_analyze 未配置",
		}).Error
	}

	_ = uc.data.DB.Model(&p).Update("status", model.ProblemStatusTagging).Error

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

// Backfill 历史回填：绑定未关联提交 + 把所有未完成题目塞进 MQ（可紧急停止中断消费）
func (uc *ProblemUseCase) Backfill(limit int) (scanned, bound, created, enqueued int64, err error) {
	// 0 = 尽量全量（单次最多 5000 提交 + 全量未完成题）
	if limit <= 0 {
		limit = 5000
	}
	// 确保流水线在跑
	pipelineControl.SetPaused(false)

	var logs []model.SubmitLog
	err = uc.data.DB.Where("(problem_id IS NULL OR problem_id = 0) AND platform != ?", spider.LeetCode).
		Order("id desc").Limit(limit).Find(&logs).Error
	if err != nil {
		return
	}
	scanned = int64(len(logs))
	for i := range logs {
		p, isNew, rerr := uc.resolveOne(&logs[i])
		if rerr != nil {
			continue
		}
		bound++
		if isNew {
			created++
		}
		_ = p
	}

	// 把所有需要处理的题塞进队列（不只新建）
	var todos []model.Problem
	_ = uc.data.DB.Where("status IN ? AND platform != ?", []string{
		model.ProblemStatusPending,
		model.ProblemStatusFetching,
		model.ProblemStatusFailed,
		model.ProblemStatusTagging,
	}, spider.LeetCode).
		Order("id desc").Find(&todos).Error

	seen := map[uint]bool{}
	for _, p := range todos {
		if seen[p.ID] {
			continue
		}
		seen[p.ID] = true
		// 已有题面 → AI 队列；否则爬取队列
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

type ListProblemFilter struct {
	Page       int64
	PageSize   int64
	Sort       string
	Platforms  []string
	Tags       []string
	UserStatus string
	UserID     int64
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
		// jsonb 包含任一 tag（OR）
		ors := make([]string, 0, len(f.Tags))
		args := make([]interface{}, 0, len(f.Tags))
		for _, t := range f.Tags {
			ors = append(ors, "tags::jsonb ? ?")
			args = append(args, t)
		}
		q = q.Where(strings.Join(ors, " OR "), args...)
	}

	// 用户状态过滤需要 join
	userStatusMap := map[uint]string{}
	if f.UserID > 0 {
		type row struct {
			ProblemID uint
			Status    string
		}
		var rows []row
		_ = uc.data.DB.Model(&model.SubmitLog{}).
			Select("problem_id, status").
			Where("user_id = ? AND problem_id IS NOT NULL", f.UserID).
			Find(&rows).Error
		best := map[uint]string{}
		for _, r := range rows {
			if r.ProblemID == 0 {
				continue
			}
			cur := best[r.ProblemID]
			ns := mapSubmitStatus(r.Status)
			if rankStatus(ns) > rankStatus(cur) {
				best[r.ProblemID] = ns
			}
		}
		userStatusMap = best
		if f.UserStatus != "" {
			want := strings.ToUpper(f.UserStatus)
			ids := make([]uint, 0)
			for id, st := range best {
				if st == want {
					ids = append(ids, id)
				}
			}
			if want == "NONE" {
				// 无提交记录的题
				has := make([]uint, 0, len(best))
				for id := range best {
					has = append(has, id)
				}
				if len(has) > 0 {
					q = q.Where("id NOT IN ?", has)
				}
			} else if len(ids) == 0 {
				return []model.Problem{}, userStatusMap, 0, nil
			} else {
				q = q.Where("id IN ?", ids)
			}
		}
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, nil, 0, err
	}
	order := "last_submitted_at DESC NULLS LAST, id DESC"
	if f.Sort == "latest_asc" {
		order = "last_submitted_at ASC NULLS LAST, id ASC"
	}
	var list []model.Problem
	err := q.Order(order).Offset(int((f.Page - 1) * f.PageSize)).Limit(int(f.PageSize)).Find(&list).Error
	return list, userStatusMap, total, err
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

	// AC 判定：status 含 OK / Accepted / AC
	acCond := "(status ILIKE '%accept%' OR status = 'OK' OR status = 'AC' OR status ILIKE 'accepted%')"

	type tagRow struct {
		Tag   string
		Count int64
	}
	var tags []tagRow
	// jsonb_array_elements_text
	err = uc.data.DB.Raw(`
		SELECT tag, COUNT(DISTINCT p.id) AS count
		FROM submit_logs s
		JOIN problems p ON p.id = s.problem_id
		CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(p.tags::jsonb, '[]'::jsonb)) AS tag
		WHERE s.user_id = ? AND `+acCond+` AND p.status = ?
		GROUP BY tag
		ORDER BY count DESC
		LIMIT 20
	`, userID, model.ProblemStatusCompleted).Scan(&tags).Error
	if err != nil {
		// 兼容 tags 非 jsonb 的情况
		log.Errorf("radar sql: %v", err)
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
	_ = uc.data.DB.Raw(`
		SELECT p.platform AS name, COUNT(DISTINCT p.id) AS count
		FROM submit_logs s
		JOIN problems p ON p.id = s.problem_id
		WHERE s.user_id = ? AND `+acCond+`
		GROUP BY p.platform
	`, userID).Scan(&plats).Error
	for _, p := range plats {
		platforms = append(platforms, struct {
			Name  string
			Count int64
		}{p.Name, p.Count})
	}

	var diffs []nc
	_ = uc.data.DB.Raw(`
		SELECT COALESCE(NULLIF(p.difficulty,''),'Unknown') AS name, COUNT(DISTINCT p.id) AS count
		FROM submit_logs s
		JOIN problems p ON p.id = s.problem_id
		WHERE s.user_id = ? AND `+acCond+`
		GROUP BY 1
	`, userID).Scan(&diffs).Error
	for _, d := range diffs {
		difficulties = append(difficulties, struct {
			Name  string
			Count int64
		}{d.Name, d.Count})
	}

	_ = uc.data.DB.Raw(`
		SELECT COUNT(DISTINCT s.problem_id) FROM submit_logs s
		WHERE s.user_id = ? AND s.problem_id IS NOT NULL AND `+acCond+`
	`, userID).Scan(&totalAC).Error

	return
}

type ProgressSnapshot struct {
	Items      []struct {
		Status string
		Count  int64
	}
	Failed     []model.Problem
	InProgress []model.Problem
	Total      int64
	Paused     bool
	ActiveJobs []ActiveJob
	Queues     []struct {
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
	var rows []sc
	if err := uc.data.DB.Model(&model.Problem{}).Select("status, count(*) as count").Group("status").Scan(&rows).Error; err != nil {
		return snap, err
	}
	for _, r := range rows {
		snap.Items = append(snap.Items, struct {
			Status string
			Count  int64
		}{r.Status, r.Count})
		snap.Total += r.Count
	}
	_ = uc.data.DB.Where("status = ?", model.ProblemStatusFailed).
		Order("updated_at desc").Limit(20).Find(&snap.Failed).Error
	_ = uc.data.DB.Where("status IN ?", []string{model.ProblemStatusFetching, model.ProblemStatusTagging}).
		Order("updated_at desc").Limit(30).Find(&snap.InProgress).Error

	snap.Paused = pipelineControl.IsPaused()
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
	// amqp Channel 非并发安全，勿与 consumer 共用；仅返回配置并发，积压用 DB 近似
	for _, q := range []struct {
		name string
		conc int64
		stat string
	}{
		{"problem_fetch", problemFetchConcurrency, model.ProblemStatusPending},
		{"problem_analyze", problemAnalyzeConcurrency, model.ProblemStatusTagging},
	} {
		var msgs int64
		_ = uc.data.DB.Model(&model.Problem{}).Where("status = ?", q.stat).Count(&msgs).Error
		// FETCHING 也算爬取侧积压
		if q.name == "problem_fetch" {
			var fetching int64
			_ = uc.data.DB.Model(&model.Problem{}).Where("status = ?", model.ProblemStatusFetching).Count(&fetching).Error
			msgs += fetching
		}
		out = append(out, struct {
			Name        string
			Messages    int64
			Consumers   int64
			Concurrency int64
		}{q.name, msgs, 1, q.conc})
	}
	return out
}

// purgeAnalyzeQueue 仅清空 AI 分析队列，不动题面爬取队列
func (uc *ProblemUseCase) purgeAnalyzeQueue() (purgedAnalyze int, err error) {
	if uc.mq == nil || uc.mq.Ch == nil {
		return 0, fmt.Errorf("mq not ready")
	}
	_, _ = uc.mq.Ch.QueueDeclare("problem_analyze", true, false, false, false, nil)
	return uc.mq.Ch.QueuePurge("problem_analyze", false)
}

// EmergencyStop 仅暂停 AI 分析并清空 problem_analyze 队列；题面不删、爬取不停
func (uc *ProblemUseCase) EmergencyStop() (purgedFetch, purgedAnalyze int, err error) {
	pipelineControl.SetAnalyzePaused(true)
	purgedAnalyze, err = uc.purgeAnalyzeQueue()
	return 0, purgedAnalyze, err
}

// Resume 恢复 AI 分析
func (uc *ProblemUseCase) Resume() {
	pipelineControl.SetAnalyzePaused(false)
}

// ResetAll 仅重置 AI 分析结果（保留 content_md 题面），清空 AI 队列并可选重新入队分析
func (uc *ProblemUseCase) ResetAll(requeue bool) (reset, enqueued, purgedFetch, purgedAnalyze int, err error) {
	pipelineControl.SetAnalyzePaused(true)
	purgedAnalyze, err = uc.purgeAnalyzeQueue()
	if err != nil {
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

	if requeue {
		var list []model.Problem
		_ = uc.data.DB.Where("status = ? AND platform != ?", model.ProblemStatusTagging, spider.LeetCode).
			Where("content_md IS NOT NULL AND content_md != ''").
			Order("id desc").Limit(3000).Find(&list).Error
		for _, p := range list {
			if e := uc.enqueueAnalyze(p.ID); e == nil {
				enqueued++
			}
		}
	}
	pipelineControl.SetAnalyzePaused(false)
	return
}

func truncateErr(s string) string {
	if len(s) > 500 {
		return s[:500]
	}
	return s
}

func mapSubmitStatus(s string) string {
	u := strings.ToUpper(s)
	if strings.Contains(u, "ACCEPT") || u == "OK" || u == "AC" {
		return "AC"
	}
	if s == "" {
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
