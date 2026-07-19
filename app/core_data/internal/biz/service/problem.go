package service

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	data2 "cwxu-algo/app/common/data"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/event"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"
	"cwxu-algo/app/core_data/internal/spider/problem_fetch"
	"cwxu-algo/app/core_data/task"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/streadway/amqp"
	"gorm.io/gorm"
)

type ProblemUseCase struct {
	data   *data.Data
	mq     *event.RabbitMQ
	tagger *ProblemTagger
	reg    *registry.Registrar

	// profileTask 画像 MQ 入队（可选；nil 时只同步算小用户）
	profileTask *task.UserProfileTask

	orgUsersMu         sync.Mutex
	orgUsersCache      map[int64]struct{} // 兼容旧缓存（= fetch 集合）
	orgUsersAt         time.Time
	pipelineUsersCache *pipelineUserSets
	pipelineUsersAt    time.Time

	// adminOp 防止补全/重置/重试并发互踩
	adminOpMu   sync.Mutex
	adminOpName string
}

func NewProblemUseCase(data *data.Data, mq *event.RabbitMQ, tagger *ProblemTagger, reg *discovery.Register, profileTask *task.UserProfileTask) *ProblemUseCase {
	var r *registry.Registrar
	if reg != nil {
		r = &reg.Reg
	}
	return &ProblemUseCase{data: data, mq: mq, tagger: tagger, reg: r, profileTask: profileTask}
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
	// 已绑定但预聚合仍停在 e:/n: 的存量键一并升级（画像 JOIN 也兼容 e:）
	if err := dal.PromoteUserACFromBoundSubmits(context.Background(), uc.data.DB, userId); err != nil {
		log.Warnf("PromoteUserACFromBoundSubmits user=%d: %v", userId, err)
	}
}

// resolveOne 解析并绑定单条提交；返回 (problem, isNew, err)
// highPriority=true：增量爬虫路径，MQ 最高优先级
func (uc *ProblemUseCase) resolveOne(sl *model.SubmitLog, highPriority bool) (*model.Problem, bool, error) {
	parsed, err := ParseProblemIdentity(sl.Platform, sl.Contest, sl.Problem)
	if err != nil {
		return nil, false, err
	}
	// SkipBank：明确不进题库的平台/记录
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
	sl.ProblemID = &pid
	sl.ExternalID = parsed.ExternalID

	// 画像预聚合：绑题后把 e:/n: 键升级为 p:{id}（写路径在绑题前多写 e:/n:）
	if sl.IsAC {
		oldKeys := []string{
			model.ACProblemKey(sl.Platform, parsed.ExternalID, sl.Problem, nil),
			model.ACProblemKey(sl.Platform, sl.ExternalID, sl.Problem, nil),
			model.ACProblemKey(parsed.Platform, parsed.ExternalID, sl.Problem, nil),
		}
		if err := dal.PromoteUserACKeysToProblemID(context.Background(), uc.data.DB, sl.UserID, oldKeys, pid); err != nil {
			log.Warnf("PromoteUserACKeys user=%d pid=%d: %v", sl.UserID, pid, err)
		}
	}

	prio := mqPriorityBulk
	if highPriority {
		prio = mqPriorityIncremental
	}

	// 题面爬取：仅近窗有爬取资格用户提交才入队（默认非公共域组织，可个人覆盖）
	// AI：enqueueAnalyzePrio 内统一闸门（独立资格）
	allowFetch := !parsed.SkipFetch && uc.shouldEnqueueFetch(existing.ID)

	// 新题且可爬
	if isNew && allowFetch && existing.Status == model.ProblemStatusPending {
		if err := uc.enqueueFetchPrio(existing.ID, existing.Platform, existing.ExternalID, existing.URL, prio); err != nil {
			log.Errorf("enqueue problem %d: %v", existing.ID, err)
		}
	}
	// 永久失败：升级标记后不再入队
	if existing.Status == model.ProblemStatusFailed && isPermanentFetchError(existing.ErrorMsg) {
		_ = uc.data.DB.Model(&existing).Update("status", model.ProblemStatusFailedPerm).Error
		existing.Status = model.ProblemStatusFailedPerm
	}

	// 已存在但题面未完成：补入队；FAILED_PERM 永不重试；已 COMPLETED：不入队
	if !isNew && !parsed.SkipFetch {
		switch existing.Status {
		case model.ProblemStatusPending, model.ProblemStatusFailed:
			if strings.TrimSpace(existing.ContentMD) == "" {
				if allowFetch {
					if err := uc.enqueueFetchPrio(existing.ID, existing.Platform, existing.ExternalID, existing.URL, prio); err != nil {
						log.Errorf("re-enqueue fetch problem %d: %v", existing.ID, err)
					}
				}
			} else {
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
			} else if allowFetch {
				if err := uc.enqueueFetchPrio(existing.ID, existing.Platform, existing.ExternalID, existing.URL, prio); err != nil {
					log.Errorf("re-enqueue fetch problem %d: %v", existing.ID, err)
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

// enqueueAnalyzePrio 投递 AI 分析；统一闸门：
// 1) 近 6 个月有提交（submit_logs）
// 2) 近窗提交者中至少有一名「题面 AI 资格」用户（默认非公共域组织，可个人覆盖）
// 题面爬取由 shouldEnqueueFetch / problemHasFetchSubmitter 单独闸门。
func (uc *ProblemUseCase) enqueueAnalyzePrio(id uint, priority uint8) error {
	if uc.mq == nil {
		return fmt.Errorf("mq not ready")
	}
	var p model.Problem
	if err := uc.data.DB.First(&p, id).Error; err != nil {
		return err
	}
	// 自动爬虫 / 回填 / 爬取成功后入队：超过 6 个月（以 submit_logs 最近提交为准）不进 AI
	if !uc.withinAnalyzeWindow(&p) {
		log.Debugf("enqueueAnalyze skip out-of-window id=%d last=%v", id, p.LastSubmittedAt)
		return nil
	}
	// 无 AI 资格用户近窗提交：只保留题面，不跑 AI
	if !uc.problemHasAISubmitter(id) {
		log.Debugf("enqueueAnalyze skip no AI-eligible submitters id=%d", id)
		return nil
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
// Force=true 时忽略用户爬取资格；SkipAnalyze=true 时爬取成功后不入 AI。
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
	// 无爬取资格用户近窗提交：不爬题面（旧消息防御；前端显示「题面准备中」）
	// Force：题单加题等主动场景可忽略资格
	if !ev.Force && !uc.shouldEnqueueFetch(p.ID) {
		log.Infof("ProcessFetch skip no fetch-eligible submitters id=%d", p.ID)
		if strings.TrimSpace(p.ContentMD) == "" && p.Status != model.ProblemStatusSkipped {
			_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
				"status":    model.ProblemStatusPending,
				"error_msg": "无题面爬取资格用户提交，暂不爬取题面",
			}).Error
		}
		return nil
	}
	// 已有题面：不再爬取；入 AI（主动路径按 actor，否则窗口 + submitter 闸门）
	if strings.TrimSpace(p.ContentMD) != "" || p.Status == model.ProblemStatusTagging {
		if p.Status != model.ProblemStatusCompleted {
			_ = uc.data.DB.Model(&p).Update("status", model.ProblemStatusTagging).Error
			if !ev.SkipAnalyze {
				if ev.ActorUserID > 0 {
					return uc.enqueueAnalyzeForUser(p.ID, ev.ActorUserID)
				}
				if !pipelineControl.IsAnalyzePaused() {
					return uc.enqueueAnalyze(p.ID)
				}
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
	if p.Status == model.ProblemStatusSkipped {
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
	uc.BumpProblemDetailVer(p.ID)
	uc.progressMoveStatus(p.Status, model.ProblemStatusTagging)
	// 爬取成功后入 AI：
	// - SkipAnalyze：仅爬不分析
	// - ActorUserID>0：用户主动场景，按操作者 AI 资格（绕过 submitter/6 月窗）
	// - 否则：走 enqueueAnalyzePrio（近 6 月 + AI 资格提交者）
	if ev.SkipAnalyze {
		log.Infof("ProcessFetch skip analyze (fetch-only) id=%d", p.ID)
		return nil
	}
	if ev.ActorUserID > 0 {
		return uc.enqueueAnalyzeForUser(p.ID, ev.ActorUserID)
	}
	// 分析暂停时仍入队（暂停不清队列，恢复后继续）；高优先级延续当前已出队的爬取任务
	return uc.enqueueAnalyzePrio(p.ID, mqPriorityIncremental)
}

// ForceEnqueueFetchOnly 强制入队题面爬取，忽略用户资格，且不触发 AI 分析。
// 兼容旧调用；新代码请用 ForceEnqueueFetch(problemID, actorUID)。
func (uc *ProblemUseCase) ForceEnqueueFetchOnly(problemID uint) error {
	return uc.ForceEnqueueFetch(problemID, 0)
}

// ForceEnqueueFetch 强制入队题面爬取（忽略爬取资格）。
// actorUID>0 且具备 AI 资格时，爬取成功后按操作者入 AI；否则仅爬取。
// ContentMD 已有时：若可分析则直接 enqueueAnalyzeForUser，否则 no-op。
func (uc *ProblemUseCase) ForceEnqueueFetch(problemID uint, actorUID uint) error {
	if uc == nil || problemID == 0 {
		return nil
	}
	var p model.Problem
	if err := uc.data.DB.First(&p, problemID).Error; err != nil {
		return err
	}
	// 已有题面：尝试按操作者 AI 资格分析
	if strings.TrimSpace(p.ContentMD) != "" {
		if actorUID > 0 && len(nonEmptyTags(p.Tags)) == 0 {
			return uc.enqueueAnalyzeForUser(problemID, actorUID)
		}
		return nil
	}
	if p.Status == model.ProblemStatusCompleted || p.Status == model.ProblemStatusSkipped {
		return nil
	}
	if p.Status == model.ProblemStatusFailedPerm {
		return nil
	}
	// 若永久失败标记在 FAILED 上，仍允许用户主动再试一次：重置为 PENDING
	if p.Status == model.ProblemStatusFailed && isPermanentFetchError(p.ErrorMsg) {
		_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
			"status":    model.ProblemStatusPending,
			"error_msg": "",
		}).Error
		p.Status = model.ProblemStatusPending
	}
	skipAnalyze := actorUID == 0 || !uc.userHasAIEligibility(actorUID)
	return uc.enqueueFetchForced(p.ID, p.Platform, p.ExternalID, p.URL, skipAnalyze, actorUID)
}

func (uc *ProblemUseCase) enqueueFetchForced(id uint, platform, externalID, url string, skipAnalyze bool, actorUID uint) error {
	if uc.mq == nil {
		return fmt.Errorf("mq not ready")
	}
	body, _ := json.Marshal(event.ProblemFetchEvent{
		ProblemID:   id,
		Platform:    platform,
		ExternalID:  externalID,
		URL:         url,
		Force:       true,
		SkipAnalyze: skipAnalyze,
		ActorUserID: actorUID,
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

// enqueueAnalyzeForUser 用户主动场景入 AI：仅校验操作者 AI 资格，不校验 submitter/6 月窗。
// 标签非空或题面为空时跳过。
func (uc *ProblemUseCase) enqueueAnalyzeForUser(problemID uint, actorUID uint) error {
	if uc == nil || problemID == 0 {
		return nil
	}
	if actorUID == 0 || !uc.userHasAIEligibility(actorUID) {
		log.Debugf("enqueueAnalyzeForUser skip no AI eligibility actor=%d id=%d", actorUID, problemID)
		return nil
	}
	if uc.mq == nil {
		return fmt.Errorf("mq not ready")
	}
	var p model.Problem
	if err := uc.data.DB.First(&p, problemID).Error; err != nil {
		return err
	}
	if len(nonEmptyTags(p.Tags)) > 0 {
		if strings.TrimSpace(p.ContentMD) != "" && p.Status != model.ProblemStatusCompleted {
			_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
				"status":    model.ProblemStatusCompleted,
				"error_msg": "",
			}).Error
		}
		return nil
	}
	if strings.TrimSpace(p.ContentMD) == "" {
		return nil
	}
	if p.Status == model.ProblemStatusCompleted || p.Status == model.ProblemStatusSkipped {
		return nil
	}
	if pipelineControl.IsAnalyzePaused() {
		// 暂停时仍入队，恢复后继续
		log.Debugf("enqueueAnalyzeForUser analyze paused, still enqueue id=%d", problemID)
	}
	_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
		"status":    model.ProblemStatusTagging,
		"error_msg": "",
	}).Error
	body, _ := json.Marshal(event.ProblemAnalyzeEvent{ProblemID: problemID, Force: true})
	if err := uc.declareProblemQueue("problem_analyze"); err != nil {
		return err
	}
	return uc.mq.Publish("", "problem_analyze", false, false, amqp.Publishing{
		ContentType:  "application/json",
		Body:         body,
		DeliveryMode: amqp.Persistent,
		Priority:     mqPriorityIncremental,
	})
}

// CreateManualProblem 用户自主加题（无需审核）。platform=Manual。
// 有题面无标签且 actor 有 AI 资格时入分析队列。
func (uc *ProblemUseCase) CreateManualProblem(actorUID uint, title, contentMD, sourceURL string, tags []string) (*model.Problem, error) {
	if uc == nil {
		return nil, fmt.Errorf("usecase nil")
	}
	title = strings.TrimSpace(title)
	if title == "" {
		return nil, fmt.Errorf("请填写题目标题")
	}
	if utf8.RuneCountInString(title) > 200 {
		return nil, fmt.Errorf("标题过长")
	}
	contentMD = strings.TrimSpace(contentMD)
	if len(contentMD) > 200_000 {
		return nil, fmt.Errorf("题面过长")
	}
	tags = normalizeEditTags(tags)
	sourceURL = strings.TrimSpace(sourceURL)
	if len(sourceURL) > 1024 {
		sourceURL = sourceURL[:1024]
	}
	extID := "m_" + strings.ReplaceAll(uuidNew(), "-", "")
	hasContent := contentMD != ""
	hasTags := len(tags) > 0
	status := model.ProblemStatusCompleted
	if hasContent && !hasTags {
		status = model.ProblemStatusTagging
	}
	p := model.Problem{
		Platform:   "Manual",
		ExternalID: extID,
		Title:      title,
		URL:        sourceURL,
		ContentMD:  contentMD,
		Tags:       model.StringArray(tags),
		Status:     status,
	}
	if err := uc.data.DB.Create(&p).Error; err != nil {
		return nil, err
	}
	if hasContent && !hasTags && actorUID > 0 {
		if err := uc.enqueueAnalyzeForUser(p.ID, actorUID); err != nil {
			log.Warnf("CreateManualProblem enqueue analyze id=%d: %v", p.ID, err)
		}
	}
	return &p, nil
}

// uuidNew 生成无连字符前的标准 UUID 字符串（失败时用随机 hex）
func uuidNew() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return fmt.Sprintf("%d", time.Now().UnixNano())
	}
	// RFC 4122 version 4
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// UpsertProblemFromParsed 按 platform+external_id 幂等入库；缺题面时强制爬取（默认不 AI，兼容旧调用）
func (uc *ProblemUseCase) UpsertProblemFromParsed(parsed *ParsedProblem) (*model.Problem, error) {
	return uc.UpsertProblemFromParsedForUser(parsed, 0)
}

// UpsertProblemFromParsedForUser 同 UpsertProblemFromParsed，actorUID 用于条件 AI
func (uc *ProblemUseCase) UpsertProblemFromParsedForUser(parsed *ParsedProblem, actorUID uint) (*model.Problem, error) {
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
			// 并发冲突再查
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
		if existing.Title == "" && parsed.Title != "" {
			_ = uc.data.DB.Model(&existing).Update("title", parsed.Title).Error
			existing.Title = parsed.Title
		}
		if existing.URL == "" && parsed.URL != "" {
			_ = uc.data.DB.Model(&existing).Update("url", parsed.URL).Error
			existing.URL = parsed.URL
		}
	}
	if !parsed.SkipFetch {
		if err := uc.ForceEnqueueFetch(existing.ID, actorUID); err != nil {
			log.Warnf("ForceEnqueueFetch id=%d: %v", existing.ID, err)
		}
	}
	return &existing, nil
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
	if p.Status == model.ProblemStatusSkipped {
		log.Debugf("ProcessAnalyze skip skipped id=%d", p.ID)
		return nil
	}
	if p.Status == model.ProblemStatusFailedPerm {
		log.Debugf("ProcessAnalyze skip failed_perm id=%d", p.ID)
		return nil
	}
	// 标签已有（人工填写或历史分析）：跳过 AI，避免覆盖；标签为空仍继续分析
	if len(nonEmptyTags(p.Tags)) > 0 {
		if strings.TrimSpace(p.ContentMD) != "" {
			log.Infof("ProcessAnalyze skip tags-already-set id=%d tags=%v", p.ID, p.Tags)
			_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
				"status":    model.ProblemStatusCompleted,
				"error_msg": "",
			}).Error
			return nil
		}
		// 有标签无题面：不跑 AI，等题面
		log.Debugf("ProcessAnalyze skip tags-set-no-content id=%d", p.ID)
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

	// 用户主动（Force）：已在入队侧校验操作者 AI 资格，跳过 6 月窗与 submitter 检查
	if !ev.Force {
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
		// 无 AI 资格用户近窗提交：不跑题面 AI（题面已爬取可保留）
		if !uc.problemHasAISubmitter(p.ID) {
			log.Infof("ProcessAnalyze skip no AI-eligible submitters id=%d", p.ID)
			_ = uc.data.DB.Model(&p).Updates(map[string]interface{}{
				"status":    model.ProblemStatusTagging,
				"error_msg": "无题面AI资格用户提交，已跳过题面AI",
			}).Error
			return nil
		}
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
	oldStatus := p.Status
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
	if err := uc.data.DB.Model(&p).Updates(updates).Error; err != nil {
		return err
	}
	oldTags, newTags, e := dal.SyncProblemTags(ctx, uc.data.DB, p.ID, result.AlgorithmTags)
	if e != nil {
		log.Warnf("SyncProblemTags analyze id=%d: %v", p.ID, e)
	} else {
		uc.BumpProblemTagsVer()
		uc.BumpProblemListVer()
		if e2 := dal.AdjustUserTagACForProblemTagsChange(ctx, uc.data.DB, p.ID, oldTags, newTags); e2 != nil {
			log.Warnf("AdjustUserTagAC analyze id=%d: %v", p.ID, e2)
		}
	}
	uc.BumpProblemDetailVer(p.ID)
	uc.progressMoveStatus(oldStatus, model.ProblemStatusCompleted)
	return nil
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

// TryStartAdminOp 管理端重操作互斥（补全/重置/重试）
func (uc *ProblemUseCase) TryStartAdminOp(name string) (ok bool, running string) {
	uc.adminOpMu.Lock()
	defer uc.adminOpMu.Unlock()
	if uc.adminOpName != "" {
		return false, uc.adminOpName
	}
	uc.adminOpName = name
	return true, ""
}

func (uc *ProblemUseCase) FinishAdminOp() {
	uc.adminOpMu.Lock()
	uc.adminOpName = ""
	uc.adminOpMu.Unlock()
}

// Backfill 增量回填（近 6 个月提交）：
// 1) 绑定未关联提交
// 2) 无题面且有组织用户提交 → 入爬取；纯公共域/散户不爬
// 3) 有题面且未分析完 → 入分析（enqueueAnalyzePrio 跳过纯公共域）
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
	err = uc.data.DB.Where("problem_id IS NULL OR problem_id = 0").
		Where("time IS NULL OR time >= ?", cutoff).
		// 力扣合成行（无 titleSlug）resolve 会失败跳过；lc-prob 可入库
		Order("CASE WHEN platform = 'NowCoder' THEN 0 WHEN platform = 'LeetCode' THEN 1 ELSE 2 END, id DESC").
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

	// 2) 仅处理「近窗有资格用户提交」的题（批量集合，避免对纯公共域几千题逐题查）
	fetchSet, fetchOK := uc.recentPipelineProblemSet("fetch", cutoff)
	aiSet, aiOK := uc.recentPipelineProblemSet("ai", cutoff)
	if !fetchOK || !aiOK {
		// 名单不可用时保守：仍扫近窗未完成题，但加 limit 防止拖死
		log.Warnf("Backfill: pipeline set unavailable fetchOK=%v aiOK=%v, fallback limited scan", fetchOK, aiOK)
		recentClause, recentArgs := sqlHasRecentSubmit(cutoff)
		var todos []model.Problem
		_ = uc.data.DB.
			Where("status NOT IN ?", []string{
				model.ProblemStatusSkipped,
				model.ProblemStatusCompleted,
				model.ProblemStatusFailedPerm,
			}).
			Where(recentClause, recentArgs...).
			Order("last_submitted_at DESC NULLS LAST, id DESC").
			Limit(limit).
			Find(&todos).Error
		for _, p := range todos {
			ef, ea := uc.backfillOneProblem(p)
			enqueuedFetch += ef
			enqueuedAnalyze += ea
			enqueued += ef + ea
		}
	} else {
		// 合并资格题 id
		idSet := make(map[uint]struct{}, len(fetchSet)+len(aiSet))
		for id := range fetchSet {
			idSet[id] = struct{}{}
		}
		for id := range aiSet {
			idSet[id] = struct{}{}
		}
		if len(idSet) > 0 {
			ids := make([]uint, 0, len(idSet))
			for id := range idSet {
				ids = append(ids, id)
			}
			var todos []model.Problem
			_ = uc.data.DB.
				Where("id IN ?", ids).
				Where("status NOT IN ?", []string{
					model.ProblemStatusSkipped,
					model.ProblemStatusCompleted,
					model.ProblemStatusFailedPerm,
				}).
				Order("last_submitted_at DESC NULLS LAST, id DESC").
				Find(&todos).Error
			for _, p := range todos {
				_, hasFetch := fetchSet[p.ID]
				_, hasAI := aiSet[p.ID]
				ef, ea := uc.backfillOneProblemWithGate(p, hasFetch, hasAI)
				enqueuedFetch += ef
				enqueuedAnalyze += ea
				enqueued += ef + ea
			}
		}
	}
	log.Infof("Backfill: scanned=%d bound=%d created=%d fetch=%d analyze=%d",
		scanned, bound, created, enqueuedFetch, enqueuedAnalyze)
	return
}

// backfillOneProblem 单题回填入队（逐题资格检查）
func (uc *ProblemUseCase) backfillOneProblem(p model.Problem) (enqueuedFetch, enqueuedAnalyze int64) {
	return uc.backfillOneProblemWithGate(p, uc.problemHasFetchSubmitter(p.ID), uc.problemHasAISubmitter(p.ID))
}

func (uc *ProblemUseCase) backfillOneProblemWithGate(p model.Problem, hasFetch, hasAI bool) (enqueuedFetch, enqueuedAnalyze int64) {
	if !hasFetch && !hasAI {
		return 0, 0
	}
	if strings.TrimSpace(p.ContentMD) == "" {
		if !hasFetch {
			return 0, 0
		}
		_ = uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
			Updates(map[string]interface{}{
				"status":           model.ProblemStatusPending,
				"error_msg":        "",
				"fetch_attempts":   0,
				"fetch_fail_since": nil,
			}).Error
		if e := uc.enqueueFetchPrio(p.ID, p.Platform, p.ExternalID, p.URL, mqPriorityBulk); e == nil {
			return 1, 0
		}
		return 0, 0
	}
	if !hasAI {
		return 0, 0
	}
	_ = uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
		Updates(map[string]interface{}{
			"status":    model.ProblemStatusTagging,
			"error_msg": "",
		}).Error
	if e := uc.enqueueAnalyzePrio(p.ID, mqPriorityBulk); e == nil {
		return 0, 1
	}
	return 0, 0
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

	// 待爬取：PENDING / FETCHING；仅有组织用户提交的题才重灌
	var fetchTodos []model.Problem
	_ = uc.data.DB.
		Where("status IN ?", []string{model.ProblemStatusPending, model.ProblemStatusFetching}).
		Where("(content_md IS NULL OR content_md = '')").
		Order("last_submitted_at DESC NULLS LAST, id DESC").
		Find(&fetchTodos).Error
	for _, p := range fetchTodos {
		if !uc.shouldEnqueueFetch(p.ID) {
			continue
		}
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
	_ = uc.data.DB.
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
// 仅近 6 月有提交 + 有流水线资格用户提交的题才会真正入队（避免公共域假入队后立刻 Ack）
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

	// 近 6 月以 submit_logs 为准（与 Progress / ProcessAnalyze 一致）
	cutoff := time.Now().Add(-backfillWindow)
	recentClause, recentArgs := sqlHasRecentSubmit(cutoff)
	q := uc.data.DB.Where("status = ?", model.ProblemStatusFailed).
		Where(recentClause, recentArgs...).
		Order("last_submitted_at DESC NULLS LAST, id DESC")
	if limit > 0 {
		q = q.Limit(limit)
	}
	var todos []model.Problem
	if err = q.Find(&todos).Error; err != nil {
		return
	}
	scanned = int64(len(todos))

	fetchSet, fetchOK := uc.recentPipelineProblemSet("fetch", cutoff)
	aiSet, aiOK := uc.recentPipelineProblemSet("ai", cutoff)

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
		hasContent := strings.TrimSpace(p.ContentMD) != ""
		hasFetch := false
		hasAI := false
		if fetchOK {
			_, hasFetch = fetchSet[p.ID]
		} else {
			hasFetch = uc.problemHasFetchSubmitter(p.ID)
		}
		if aiOK {
			_, hasAI = aiSet[p.ID]
		} else {
			hasAI = uc.problemHasAISubmitter(p.ID)
		}
		if hasContent {
			if !hasAI {
				continue
			}
			_ = uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
				Updates(map[string]interface{}{
					"status":    model.ProblemStatusTagging,
					"error_msg": "",
				}).Error
			if e := uc.enqueueAnalyze(p.ID); e == nil {
				enqueued++
			}
		} else {
			if !hasFetch {
				continue
			}
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
	log.Infof("RetryFailed: scanned=%d enqueued=%d blacklisted=%d", scanned, enqueued, blacklisted)
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
		if !isPermanentFetchError(p.ErrorMsg) && !isQOJFailedForbidden(&p) {
			continue
		}
		if err := uc.data.DB.Model(&model.Problem{}).Where("id = ?", p.ID).
			Updates(map[string]interface{}{
				"status":    model.ProblemStatusFailedPerm,
				"error_msg": normalizeQOJForbiddenMsg(p.ErrorMsg, p.Platform),
			}).Error; err == nil {
			n++
		}
	}
	if n > 0 {
		log.Infof("markExistingPermanentFailures: %d → FAILED_PERM", n)
	}
	return n
}

// isQOJFailedForbidden 历史 QOJ 题 error_msg 含 403 / status 403 → 无权限
func isQOJFailedForbidden(p *model.Problem) bool {
	if p == nil {
		return false
	}
	if !strings.EqualFold(strings.TrimSpace(p.Platform), "QOJ") {
		return false
	}
	msg := p.ErrorMsg
	return strings.Contains(msg, "status 403") ||
		strings.Contains(msg, "QOJ 无权限") ||
		(strings.Contains(msg, "403") && strings.Contains(strings.ToLower(msg), "forbidden"))
}

func normalizeQOJForbiddenMsg(msg, platform string) string {
	if strings.EqualFold(strings.TrimSpace(platform), "QOJ") &&
		(strings.Contains(msg, "status 403") || isQOJForbiddenError(msg)) {
		return "QOJ 无权限访问题面(403)"
	}
	if isPermanentFetchError(msg) {
		return msg
	}
	return msg
}

type ListProblemFilter struct {
	Page          int64
	PageSize      int64
	Sort          string
	Platforms     []string
	Tags          []string
	UserStatus    string
	UserID        int64
	Keyword       string
	Difficulty    string
	FollowingIDs  []int64 // 非空：仅这些用户提交过的题
}

type listCachePayload struct {
	List  []model.Problem
	Total int64
}

func listFilterCacheable(f ListProblemFilter) bool {
	// 首屏、无关键词/状态筛选/关注过滤；platforms/tags/difficulty 写入 key
	if f.Page != 1 {
		return false
	}
	if strings.TrimSpace(f.Keyword) != "" {
		return false
	}
	if strings.TrimSpace(f.UserStatus) != "" {
		return false
	}
	if f.FollowingIDs != nil {
		return false
	}
	return true
}

func (uc *ProblemUseCase) List(f ListProblemFilter) ([]model.Problem, map[uint]string, int64, error) {
	if f.Page <= 0 {
		f.Page = 1
	}
	if f.PageSize <= 0 {
		f.PageSize = 20
	}

	// 默认列表短缓存（不含 userStatusMap）
	if listFilterCacheable(f) && uc.data != nil && uc.data.RDB != nil {
		ver := uc.redisVer(problemListVerKey)
		plats := strings.Join(f.Platforms, ",")
		tags := strings.Join(dal.NormalizeTags(f.Tags), ",")
		diff := strings.TrimSpace(f.Difficulty)
		key := fmt.Sprintf("problem:list:v%s:p%d:ps%d:plat{%s}:tag{%s}:diff{%s}",
			ver, f.Page, f.PageSize, plats, tags, diff)
		payload, _, err := data2.GetCacheDalTTL[listCachePayload](context.Background(), uc.data.RDB, key, problemListCacheTTL, func(data *listCachePayload) error {
			list, total, e := uc.listProblemsDB(f)
			if e != nil {
				return e
			}
			data.List = list
			data.Total = total
			return nil
		})
		if err == nil && payload != nil {
			userStatusMap := map[uint]string{}
			if f.UserID > 0 && len(payload.List) > 0 {
				ids := make([]uint, 0, len(payload.List))
				for i := range payload.List {
					ids = append(ids, payload.List[i].ID)
				}
				if m, e := dal.GetUserProblemStatuses(context.Background(), uc.data.DB, f.UserID, ids); e == nil {
					userStatusMap = m
				}
			}
			return payload.List, userStatusMap, payload.Total, nil
		}
	}

	list, total, err := uc.listProblemsDB(f)
	if err != nil {
		return nil, nil, 0, err
	}
	userStatusMap := map[uint]string{}
	if f.UserID > 0 && len(list) > 0 {
		ids := make([]uint, 0, len(list))
		for i := range list {
			ids = append(ids, list[i].ID)
		}
		if m, e := dal.GetUserProblemStatuses(context.Background(), uc.data.DB, f.UserID, ids); e == nil {
			userStatusMap = m
		}
	}
	return list, userStatusMap, total, nil
}

func (uc *ProblemUseCase) listProblemsDB(f ListProblemFilter) ([]model.Problem, int64, error) {
	q := uc.data.DB.Model(&model.Problem{})
	if len(f.Platforms) > 0 {
		q = q.Where("platform IN ?", f.Platforms)
	}
	if len(f.Tags) > 0 {
		clean := dal.NormalizeTags(f.Tags)
		if len(clean) > 0 {
			q = q.Where(`id IN (
				SELECT problem_id FROM problem_tags WHERE tag IN ?
			)`, clean)
		}
	}
	if kw := strings.TrimSpace(f.Keyword); kw != "" {
		like := "%" + kw + "%"
		q = q.Where("(title ILIKE ? OR external_id ILIKE ?)", like, like)
	}
	if d := strings.TrimSpace(f.Difficulty); d != "" {
		q = q.Where("difficulty = ?", d)
	}
	if len(f.FollowingIDs) > 0 {
		q = q.Where(`EXISTS (
			SELECT 1 FROM user_problem_status ups
			WHERE ups.problem_id = problems.id AND ups.user_id IN ?
		)`, f.FollowingIDs)
	} else if f.FollowingIDs != nil {
		q = q.Where("1 = 0")
	}

	if f.UserID > 0 && f.UserStatus != "" {
		want := strings.ToUpper(strings.TrimSpace(f.UserStatus))
		switch want {
		case "NONE":
			q = q.Where(`NOT EXISTS (
				SELECT 1 FROM user_problem_status ups
				WHERE ups.problem_id = problems.id AND ups.user_id = ?
			)`, f.UserID)
		case "AC":
			q = q.Where(`EXISTS (
				SELECT 1 FROM user_problem_status ups
				WHERE ups.problem_id = problems.id AND ups.user_id = ? AND ups.status = 'AC'
			)`, f.UserID)
		case "TRIED":
			q = q.Where(`EXISTS (
				SELECT 1 FROM user_problem_status ups
				WHERE ups.problem_id = problems.id AND ups.user_id = ? AND ups.status = 'TRIED'
			)`, f.UserID)
		}
	}

	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	order := `
		CASE
			WHEN content_md IS NULL OR btrim(content_md) = '' THEN 2
			WHEN tags IS NULL OR btrim(tags::text) IN ('', '[]', 'null') THEN 1
			ELSE 0
		END ASC,
		last_submitted_at DESC NULLS LAST,
		id DESC`
	var list []model.Problem
	err := q.Order(order).Offset(int((f.Page - 1) * f.PageSize)).Limit(int(f.PageSize)).Find(&list).Error
	return list, total, err
}

// TagCount 标签及题目数（用于筛选器）
type TagCount struct {
	Tag   string
	Count int64
}

// HotProblemRow 全站热题一行（含题库信息 + 近窗统计）
type HotProblemRow struct {
	Problem     model.Problem
	SubmitCount int64
	SolverCount int64
	AcCount     int64
	Score       float64
	LastTime    time.Time
}

type hotListCachePayload struct {
	Rows  []HotProblemRow
	Total int64
	Days  int
}

const (
	// problemHotCacheTTL 热题榜短缓存（窗口聚合较重）
	problemHotCacheTTL = 90 * time.Second
	// hotScore weights documented for API
	// score = submit*1 + solver*3 + ac*2
)

// ListHot 全站热题：近 days 天 submit/solver/ac 综合分排序。
// days 默认 2，夹紧到 [1,7]。
func (uc *ProblemUseCase) ListHot(page, pageSize int64, days int) ([]HotProblemRow, int64, int, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 20
	}
	if pageSize > 100 {
		pageSize = 100
	}
	if days <= 0 {
		days = 2
	}
	if days > 7 {
		days = 7
	}

	ctx := context.Background()
	if uc.data != nil && uc.data.RDB != nil {
		key := fmt.Sprintf("problem:hot:d%d:p%d:ps%d", days, page, pageSize)
		payload, _, err := data2.GetCacheDalTTL[hotListCachePayload](ctx, uc.data.RDB, key, problemHotCacheTTL, func(data *hotListCachePayload) error {
			rows, total, e := uc.listHotDB(page, pageSize, days)
			if e != nil {
				return e
			}
			data.Rows = rows
			data.Total = total
			data.Days = days
			return nil
		})
		if err == nil && payload != nil {
			return payload.Rows, payload.Total, payload.Days, nil
		}
	}
	rows, total, err := uc.listHotDB(page, pageSize, days)
	return rows, total, days, err
}

func (uc *ProblemUseCase) listHotDB(page, pageSize int64, days int) ([]HotProblemRow, int64, error) {
	since := time.Now().Add(-time.Duration(days) * 24 * time.Hour)
	aggs, total, err := dal.ListHotProblems(context.Background(), uc.data.DB, since, page, pageSize)
	if err != nil {
		return nil, 0, err
	}
	if len(aggs) == 0 {
		return []HotProblemRow{}, total, nil
	}
	ids := make([]uint, 0, len(aggs))
	for _, a := range aggs {
		ids = append(ids, a.ProblemID)
	}
	var problems []model.Problem
	if err := uc.data.DB.Where("id IN ?", ids).Find(&problems).Error; err != nil {
		return nil, 0, err
	}
	byID := make(map[uint]model.Problem, len(problems))
	for i := range problems {
		byID[problems[i].ID] = problems[i]
	}
	out := make([]HotProblemRow, 0, len(aggs))
	for _, a := range aggs {
		p, ok := byID[a.ProblemID]
		if !ok {
			// 题库行缺失时跳过（孤儿 problem_id）
			continue
		}
		out = append(out, HotProblemRow{
			Problem:     p,
			SubmitCount: a.SubmitCount,
			SolverCount: a.SolverCount,
			AcCount:     a.AcCount,
			Score:       a.Score,
			LastTime:    a.LastTime,
		})
	}
	return out, total, nil
}

// ListTags 从 problem_tags 倒排聚合（Redis 缓存）
func (uc *ProblemUseCase) ListTags(limit int) ([]TagCount, error) {
	if limit <= 0 {
		limit = 100
	}
	ctx := context.Background()
	if uc.data != nil && uc.data.RDB != nil {
		ver := uc.redisVer(problemTagsVerKey)
		key := fmt.Sprintf("problem:tags:count:v%s:lim%d", ver, limit)
		cached, _, err := data2.GetCacheDalTTL[[]TagCount](ctx, uc.data.RDB, key, problemTagsCacheTTL, func(data *[]TagCount) error {
			list, e := uc.listTagsDB(limit)
			if e != nil {
				return e
			}
			*data = list
			return nil
		})
		if err == nil && cached != nil {
			return *cached, nil
		}
	}
	return uc.listTagsDB(limit)
}

func (uc *ProblemUseCase) listTagsDB(limit int) ([]TagCount, error) {
	rows, err := dal.ListTagCounts(context.Background(), uc.data.DB, limit)
	if err != nil {
		return nil, err
	}
	out := make([]TagCount, 0, len(rows))
	for _, r := range rows {
		out = append(out, TagCount{Tag: r.Tag, Count: r.Count})
	}
	// 表空时回退 jsonb（启动竞态）
	if len(out) == 0 {
		var fb []TagCount
		_ = uc.data.DB.Raw(`
			SELECT tag, COUNT(DISTINCT p.id) AS count
			FROM problems p
			CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(p.tags::jsonb, '[]'::jsonb)) AS tag
			WHERE p.tags IS NOT NULL AND p.tags::text NOT IN ('', '[]', 'null') AND BTRIM(tag) <> ''
			GROUP BY tag ORDER BY count DESC, tag ASC LIMIT ?
		`, limit).Scan(&fb).Error
		return fb, nil
	}
	return out, nil
}

func (uc *ProblemUseCase) Get(id uint) (*model.Problem, error) {
	return uc.getProblemCached(id)
}

func (uc *ProblemUseCase) ListSubmissions(problemID uint, userID, page, pageSize int64, followingIDs []int64, status string) ([]model.SubmitLog, int64, error) {
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
	if followingIDs != nil {
		if len(followingIDs) == 0 {
			return []model.SubmitLog{}, 0, nil
		}
		q = q.Where("user_id IN ?", followingIDs)
	}
	if strings.EqualFold(strings.TrimSpace(status), "AC") {
		q = q.Where(sqlACStatusCond("status"))
	}
	var total int64
	if err := q.Count(&total).Error; err != nil {
		return nil, 0, err
	}
	var list []model.SubmitLog
	err := q.Order("time desc").Offset(int((page - 1) * pageSize)).Limit(int(pageSize)).Find(&list).Error
	return list, total, err
}

// FollowingProblemStatus 关注用户对本题 AC/TRIED/NONE（上限 200，读预聚合）
func (uc *ProblemUseCase) FollowingProblemStatus(problemID uint, followingIDs []int64) ([]struct {
	UserID int64
	Status string
}, error) {
	out := make([]struct {
		UserID int64
		Status string
	}, 0)
	if problemID == 0 || len(followingIDs) == 0 {
		return out, nil
	}
	if len(followingIDs) > 200 {
		followingIDs = followingIDs[:200]
	}
	statusMap := make(map[int64]string, len(followingIDs))
	for _, id := range followingIDs {
		statusMap[id] = "NONE"
	}
	m, err := dal.GetFollowingProblemStatuses(context.Background(), uc.data.DB, problemID, followingIDs)
	if err != nil {
		return nil, err
	}
	for uid, st := range m {
		if st == model.UserProblemStatusAC || st == model.UserProblemStatusTried {
			statusMap[uid] = st
		}
	}
	for _, id := range followingIDs {
		out = append(out, struct {
			UserID int64
			Status string
		}{UserID: id, Status: statusMap[id]})
	}
	return out, nil
}

// UserProfile 见 user_profile.go（缓存 + MQ 预计算）

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

	// 优先 Redis 全量状态计数（P8）；近窗状态仍 SQL
	var rows []sc
	if m, ok := uc.progressCountersFromRedis(); ok {
		for st, c := range m {
			// 全量三类直接用 hash；其余仍走近窗 SQL
			isFull := false
			for _, fs := range fullStatuses {
				if st == fs {
					isFull = true
					break
				}
			}
			if isFull {
				rows = append(rows, sc{Status: st, Count: c})
			}
		}
	} else {
		if err := uc.data.DB.Model(&model.Problem{}).
			Select("status, count(*) as count").
			Where("status IN ?", fullStatuses).
			Group("status").Scan(&rows).Error; err != nil {
			return snap, err
		}
		// 异步回填 hash
		go uc.rebuildProgressCounters()
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
		{"problem_fetch", int64(problemFetchConcurrency), model.ProblemStatusPending},
		{"problem_analyze", int64(problemAnalyzeConcurrency), model.ProblemStatusTagging},
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
		q := uc.data.DB.Where("status = ?", model.ProblemStatusTagging).
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

	// QOJ 403 = 无权限：直接永久失效（即使文案仍是旧的 "status 403"）
	if isPermanentFetchError(msg) || isQOJFailedForbidden(&model.Problem{Platform: p.Platform, ErrorMsg: msg}) {
		if isQOJFailedForbidden(&model.Problem{Platform: p.Platform, ErrorMsg: msg}) {
			msg = "QOJ 无权限访问题面(403)"
			updates["error_msg"] = msg
		}
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
// 例外：QOJ 403 = 无权限（比赛/私有题），直接永久失效
func isPermanentFetchError(msg string) bool {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return false
	}
	// QOJ 无权限：优先判定，避免被通用「status 403」瞬时规则吞掉
	if isQOJForbiddenError(msg) {
		return true
	}
	if isTransientFetchError(msg) {
		return false
	}
	permanent := []string{
		"未找到题面",
		"未找到题面 DOM",
		"无法解析 CF external_id",
		"力扣付费题/无公开题面",
		"LeetCode 缺少 titleSlug",
		"leetcode 题目不存在",
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

// isQOJForbiddenError 错误文案本身已标明 QOJ 无权限/403
func isQOJForbiddenError(msg string) bool {
	msg = strings.TrimSpace(msg)
	if msg == "" {
		return false
	}
	if strings.Contains(msg, "QOJ 无权限访问题面") {
		return true
	}
	// 带 QOJ 前缀的 status 403（新路径返回 "QOJ status 403" 时的兜底）
	if strings.Contains(msg, "QOJ") && strings.Contains(msg, "status 403") {
		return true
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
