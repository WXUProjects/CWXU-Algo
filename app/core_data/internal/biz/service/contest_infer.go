package service

import (
	"strings"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"
	"cwxu-algo/app/core_data/internal/spider"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// 平台默认赛长（无日历/官方 duration 时用）；宁可略宽避免漏 AC。
var defaultContestDuration = map[string]time.Duration{
	spider.CodeForces: 2 * time.Hour,
	spider.AtCoder:    100 * time.Minute,
	spider.NowCoder:   3 * time.Hour,
	spider.LeetCode:   90 * time.Minute,
	spider.LuoGu:      3 * time.Hour,
	spider.QOJ:        5 * time.Hour,
}

const (
	// contestInferEndBuffer 赛后缓冲：排队/延迟交题
	contestInferEndBuffer = 15 * time.Minute
	// contestInferSubmitLimit 单次反推最多扫提交条数
	contestInferSubmitLimit = 8000
)

// lookupContestCalendar 按 platform+external_id（及模糊）查赛程日历。
func lookupContestCalendar(db *gorm.DB, platform, contestID string) (model.ContestCalendar, bool) {
	var cal model.ContestCalendar
	if db == nil || contestID == "" {
		return cal, false
	}
	platform = strings.TrimSpace(platform)
	contestID = strings.TrimSpace(contestID)
	err := db.Where("platform = ? AND external_id = ?", platform, contestID).First(&cal).Error
	if err != nil {
		_ = db.Where("platform = ? AND (external_id LIKE ? OR url LIKE ?)",
			platform, "%"+contestID+"%", "%"+contestID+"%").
			Order("start_time DESC").First(&cal).Error
	}
	if cal.ID > 0 && cal.StartTime > 0 {
		return cal, true
	}
	return cal, false
}

// ResolveContestDisplayWindow 给人看的起止时间（无赛后缓冲）。
// 优先日历；否则用 hint + 平台默认赛长估算（hint 当作开赛）。
// 返回的 ok 表示至少有可信开赛时间。
func ResolveContestDisplayWindow(db *gorm.DB, platform, contestID string, hintTime time.Time) (start, end time.Time, ok bool) {
	platform = strings.TrimSpace(platform)
	contestID = strings.TrimSpace(contestID)
	if cal, found := lookupContestCalendar(db, platform, contestID); found {
		start = time.Unix(cal.StartTime, 0)
		if cal.EndTime > cal.StartTime {
			end = time.Unix(cal.EndTime, 0)
		}
	}
	if start.IsZero() && !hintTime.IsZero() {
		start = hintTime
	}
	dur := defaultContestDuration[platform]
	if dur <= 0 {
		dur = 5 * time.Hour
	}
	if end.IsZero() && !start.IsZero() {
		end = start.Add(dur)
	}
	if start.IsZero() {
		return time.Time{}, time.Time{}, false
	}
	if !end.After(start) {
		end = start.Add(dur)
	}
	return start, end, true
}

// ResolveContestWindow 解析比赛时间窗 [start, end]（end 含赛后缓冲，供 Infer 扫提交）。
// hintTime：contest_logs.time 等提示；零值则仅靠日历/默认。
func ResolveContestWindow(db *gorm.DB, platform, contestID string, hintTime time.Time) (start, end time.Time) {
	platform = strings.TrimSpace(platform)
	contestID = strings.TrimSpace(contestID)

	// 1) 日历
	if cal, found := lookupContestCalendar(db, platform, contestID); found {
		start = time.Unix(cal.StartTime, 0)
		if cal.EndTime > cal.StartTime {
			end = time.Unix(cal.EndTime, 0)
		}
	}

	// 2) contest_logs 提示 start
	if start.IsZero() && !hintTime.IsZero() {
		start = hintTime
	}
	if start.IsZero() && db != nil && contestID != "" {
		var cl model.ContestLog
		if db.Where("platform = ? AND contest_id = ?", platform, contestID).
			Order("time ASC").First(&cl).Error == nil && !cl.Time.IsZero() {
			start = cl.Time
		}
	}

	// 3) 默认时长补 end
	dur := defaultContestDuration[platform]
	if dur <= 0 {
		dur = 5 * time.Hour
	}
	if end.IsZero() && !start.IsZero() {
		end = start.Add(dur)
	}
	// 仍无 start：用 now-dur 宽窗（兜底，尽量少用）
	if start.IsZero() {
		end = time.Now()
		start = end.Add(-dur)
	}
	if !end.After(start) {
		end = start.Add(dur)
	}
	// 缓冲
	end = end.Add(contestInferEndBuffer)
	return start, end
}

// BatchContestDisplayTimes 批量解析 (platform, contestId) → (start, end) unix。
// 先一次查出相关日历行，缺的再按默认赛长用 hint 估算。
func BatchContestDisplayTimes(db *gorm.DB, logs []model.ContestLog) map[string][2]int64 {
	out := map[string][2]int64{}
	if len(logs) == 0 {
		return out
	}
	type key struct{ p, c string }
	need := map[key]time.Time{}
	for _, l := range logs {
		k := key{p: l.Platform, c: l.ContestId}
		if _, ok := need[k]; !ok {
			need[k] = l.Time
		}
	}
	// 日历批量：按平台分组查
	byPlat := map[string][]string{}
	for k := range need {
		byPlat[k.p] = append(byPlat[k.p], k.c)
	}
	calMap := map[key]model.ContestCalendar{}
	if db != nil {
		for plat, ids := range byPlat {
			var cals []model.ContestCalendar
			_ = db.Where("platform = ? AND external_id IN ?", plat, ids).Find(&cals).Error
			for _, cal := range cals {
				calMap[key{p: cal.Platform, c: cal.ExternalID}] = cal
			}
		}
	}
	for k, hint := range need {
		mapKey := k.p + "\x00" + k.c
		if cal, ok := calMap[k]; ok && cal.StartTime > 0 {
			end := cal.EndTime
			if end <= cal.StartTime {
				dur := defaultContestDuration[k.p]
				if dur <= 0 {
					dur = 5 * time.Hour
				}
				end = cal.StartTime + int64(dur.Seconds())
			}
			out[mapKey] = [2]int64{cal.StartTime, end}
			continue
		}
		start, end, ok := ResolveContestDisplayWindow(db, k.p, k.c, hint)
		if ok {
			out[mapKey] = [2]int64{start.Unix(), end.Unix()}
		}
	}
	return out
}

// loadContestProblemSet 本场 external_id → label
func loadContestProblemSet(db *gorm.DB, platform, contestID string) map[string]string {
	out := map[string]string{}
	if db == nil {
		return out
	}
	var items []model.ContestProblem
	_ = db.Where("platform = ? AND contest_id = ?", platform, contestID).Find(&items).Error
	for _, it := range items {
		ext := strings.TrimSpace(it.ExternalID)
		if ext == "" {
			continue
		}
		label := strings.TrimSpace(it.Label)
		if label == "" {
			label = ext
		}
		out[ext] = label
		// 大小写变体
		out[strings.ToLower(ext)] = label
	}
	return out
}

// InferContestUserProblems 通用兜底：submit_logs ∩ 题目集 ∩ 时间窗 → contest_user_problems。
// 任意平台原生明细缺失时使用；有题目目录时更准，无目录时用 contest 字段匹配。
// 返回写入/更新行数（近似）。
func InferContestUserProblems(db *gorm.DB, platform, contestID string, userIDs []int64, hintTime time.Time) (int, error) {
	if db == nil || len(userIDs) == 0 {
		return 0, nil
	}
	platform = strings.TrimSpace(platform)
	contestID = strings.TrimSpace(contestID)
	if platform == "" || contestID == "" {
		return 0, nil
	}

	start, end := ResolveContestWindow(db, platform, contestID, hintTime)
	probSet := loadContestProblemSet(db, platform, contestID)
	hasProbSet := len(probSet) > 0

	type row struct {
		UserID     int64     `gorm:"column:user_id"`
		Contest    string    `gorm:"column:contest"`
		Problem    string    `gorm:"column:problem"`
		Status     string    `gorm:"column:status"`
		SubmitID   string    `gorm:"column:submit_id"`
		ExternalID string    `gorm:"column:external_id"`
		Time       time.Time `gorm:"column:time"`
	}
	var rows []row
	q := db.Model(&model.SubmitLog{}).
		Select("user_id, contest, problem, status, submit_id, external_id, time").
		Where("platform = ? AND user_id IN ?", platform, userIDs).
		Where("time >= ? AND time <= ?", start, end).
		Where("problem <> '' AND problem IS NOT NULL")

	// 力扣排除合成日历/补齐/合成 AC
	if platform == spider.LeetCode {
		q = q.Where("submit_id LIKE ?", "lc-prob-%")
	}

	if err := q.Order("time ASC").Limit(contestInferSubmitLimit).Find(&rows).Error; err != nil {
		return 0, err
	}
	if len(rows) == 0 {
		return 0, nil
	}

	type agg struct {
		label    string
		ext      string
		attempts int
		ac       bool
		firstAC  *time.Time
	}
	byUser := map[int64]map[string]*agg{}

	for _, r := range rows {
		// 力扣合成再防一层
		if platform == spider.LeetCode && model.IsLeetCodeSyntheticSubmit(platform, r.SubmitID) {
			continue
		}

		ext, label := resolveSubmitExternal(platform, contestID, r.Contest, r.Problem, r.ExternalID)
		if ext == "" {
			continue
		}

		// 归属：题目集 或 contest 字段 或（无题目集时）contest 匹配
		belong := false
		if hasProbSet {
			if _, ok := probSet[ext]; ok {
				belong = true
			} else if _, ok := probSet[strings.ToLower(ext)]; ok {
				belong = true
				// 用集合里的规范 label
				if lb := probSet[ext]; lb != "" {
					label = lb
				} else if lb := probSet[strings.ToLower(ext)]; lb != "" {
					label = lb
				}
			}
		}
		// contest 字段精确/模糊
		cField := strings.TrimSpace(r.Contest)
		if !belong && cField != "" {
			if cField == contestID || cField == "-"+contestID ||
				strings.EqualFold(cField, contestID) ||
				strings.Contains(cField, contestID) {
				belong = true
			}
		}
		// 无题目集且 contest 空：无法安全归属（避免全库同题误算）
		if !belong {
			continue
		}
		if label == "" {
			if lb, ok := probSet[ext]; ok {
				label = lb
			} else {
				label = ext
			}
		}

		m := byUser[r.UserID]
		if m == nil {
			m = map[string]*agg{}
			byUser[r.UserID] = m
		}
		a := m[ext]
		if a == nil {
			a = &agg{label: label, ext: ext}
			m[ext] = a
		}
		if a.ac {
			continue
		}
		if model.IsAcceptedStatus(r.Status) {
			a.ac = true
			t := r.Time
			a.firstAC = &t
			continue
		}
		st := strings.ToUpper(strings.TrimSpace(r.Status))
		if st == "" || st == "TESTING" || st == "PENDING" || st == "JUDGING" || st == "IN_QUEUE" ||
			st == "CE" || st == "COMPILATION_ERROR" || st == "编译错误" || st == "SUBMIT" {
			continue
		}
		a.attempts++
	}

	var upserts []model.ContestUserProblem
	for uid, m := range byUser {
		for _, a := range m {
			st := model.ContestCellTried
			var rel *int
			if a.ac {
				st = model.ContestCellAC
				if a.firstAC != nil && !start.IsZero() {
					sec := int(a.firstAC.Sub(start).Seconds())
					if sec < 0 {
						sec = 0
					}
					rel = &sec
				}
			}
			upserts = append(upserts, model.ContestUserProblem{
				Platform:    platform,
				ContestID:   contestID,
				UserID:      uid,
				Label:       a.label,
				ExternalID:  a.ext,
				Status:      st,
				Attempts:    a.attempts,
				FirstACAt:   a.firstAC,
				RelativeSec: rel,
			})
		}
	}
	if len(upserts) == 0 {
		return 0, nil
	}
	err := db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "platform"}, {Name: "contest_id"}, {Name: "user_id"}, {Name: "external_id"},
		},
		DoUpdates: clause.AssignmentColumns([]string{
			"label", "status", "attempts", "first_ac_at", "relative_sec", "updated_at",
		}),
	}).CreateInBatches(&upserts, 100).Error
	if err != nil {
		return 0, err
	}
	return len(upserts), nil
}

// resolveSubmitExternal 从提交行得到 external_id + 展示 label。
func resolveSubmitExternal(platform, contestID, contestField, problem, storedExt string) (ext, label string) {
	storedExt = strings.TrimSpace(storedExt)
	if storedExt != "" && !strings.HasPrefix(storedExt, "ac-") {
		ext = storedExt
		label = storedExt
		// 仍尝试 parse 拿更好 label
		if parsed, err := ParseProblemIdentity(platform, firstNonEmpty(contestField, contestID), problem); err == nil && parsed != nil {
			if parsed.ExternalID != "" {
				ext = parsed.ExternalID
			}
			if parsed.Title != "" && len(parsed.Title) < 40 {
				// label 优先短 index
			}
		}
		label = shortLabelFromExt(platform, ext, problem)
		return ext, label
	}

	contestForParse := contestField
	if contestForParse == "" || contestForParse == "leetcode" {
		contestForParse = contestID
	}
	parsed, err := ParseProblemIdentity(platform, contestForParse, problem)
	if err == nil && parsed != nil && parsed.ExternalID != "" {
		return parsed.ExternalID, shortLabelFromExt(platform, parsed.ExternalID, problem)
	}

	// 牛客：题目前缀数字
	if platform == spider.NowCoder {
		if id := leadingDigits(problem); id != "" {
			return id, id
		}
	}
	// 力扣：problem 以 slug 开头
	if platform == spider.LeetCode {
		parts := strings.Fields(problem)
		if len(parts) > 0 && reLeetCodeSlug.MatchString(parts[0]) {
			return parts[0], parts[0]
		}
	}
	return "", ""
}

func shortLabelFromExt(platform, ext, problem string) string {
	ext = strings.TrimSpace(ext)
	if platform == spider.CodeForces || platform == "Codeforces" {
		// 2247A / gym102861A → A
		e := ext
		if strings.HasPrefix(strings.ToLower(e), "gym") {
			e = e[3:]
		}
		for i := 0; i < len(e); i++ {
			if (e[i] >= 'A' && e[i] <= 'Z') || (e[i] >= 'a' && e[i] <= 'z') {
				return e[i:]
			}
		}
	}
	if platform == spider.AtCoder {
		if i := strings.LastIndex(ext, "_"); i >= 0 && i+1 < len(ext) {
			return strings.ToUpper(ext[i+1:])
		}
	}
	if platform == spider.NowCoder || platform == spider.QOJ {
		return ext
	}
	if platform == spider.LuoGu {
		return ext
	}
	// 从 problem 取首段
	if p := strings.TrimSpace(problem); p != "" {
		if i := strings.IndexAny(p, " \t-"); i > 0 && i <= 8 {
			return p[:i]
		}
	}
	if len(ext) > 12 {
		return ext[:12]
	}
	return ext
}

func leadingDigits(s string) string {
	s = strings.TrimSpace(s)
	i := 0
	for i < len(s) && s[i] >= '0' && s[i] <= '9' {
		i++
	}
	if i == 0 {
		return ""
	}
	return s[:i]
}

// InferContestUserProblemsForUser 爬虫路径：单用户反推（原生明细失败或补洞）。
func InferContestUserProblemsForUser(db *gorm.DB, platform, contestID string, userID int64, hintTime time.Time) (int, error) {
	if userID == 0 {
		return 0, nil
	}
	return InferContestUserProblems(db, platform, contestID, []int64{userID}, hintTime)
}

// ContestCellSubmit 站内榜格子弹窗：单条赛时提交。
type ContestCellSubmit struct {
	ID          uint
	SubmitID    string
	Status      string
	Lang        string
	Time        time.Time
	RelativeSec *int
	Problem     string
	Contest     string
	ExternalID  string
	ProblemID   *uint
}

const contestCellSubmitLimit = 200

// resolveCellSubmitWindow 站内榜格子弹窗专用时间窗。
//
// 注意 contest_logs.time 语义因平台而异，不能一律当「开赛」：
//   - AtCoder history：结束时间
//   - Codeforces rating：出分/结算时间（赛后）
//   - 日历：可信 start/end
//   - 格子 FirstACAt − RelativeSec：可反推开赛
//
// 返回 start 供相对赛时展示；end 含短缓冲。
func resolveCellSubmitWindow(db *gorm.DB, platform, contestID string, hintTime time.Time) (start, end time.Time) {
	platform = strings.TrimSpace(platform)
	contestID = strings.TrimSpace(contestID)
	dur := defaultContestDuration[platform]
	if dur <= 0 {
		dur = 5 * time.Hour
	}

	// 1) 日历最可信
	if cal, found := lookupContestCalendar(db, platform, contestID); found {
		start = time.Unix(cal.StartTime, 0)
		if cal.EndTime > cal.StartTime {
			end = time.Unix(cal.EndTime, 0).Add(contestInferEndBuffer)
		} else {
			end = start.Add(dur + contestInferEndBuffer)
		}
		return start, end
	}

	// 2) 用本场格子 FirstACAt − RelativeSec 反推开赛
	if db != nil && contestID != "" {
		var cells []model.ContestUserProblem
		_ = db.Where("platform = ? AND contest_id = ? AND first_ac_at IS NOT NULL AND relative_sec IS NOT NULL",
			platform, contestID).
			Limit(50).Find(&cells).Error
		var derivedStart time.Time
		for _, c := range cells {
			if c.FirstACAt == nil || c.RelativeSec == nil || *c.RelativeSec < 0 {
				continue
			}
			s := c.FirstACAt.Add(-time.Duration(*c.RelativeSec) * time.Second)
			if s.IsZero() {
				continue
			}
			if derivedStart.IsZero() || s.Before(derivedStart) {
				derivedStart = s
			}
		}
		if !derivedStart.IsZero() {
			// 略放宽开赛前 2min，避免边界
			start = derivedStart.Add(-2 * time.Minute)
			end = derivedStart.Add(dur + contestInferEndBuffer)
			// 若 hint 更晚且像结束/出分时间，把 end 拉到 hint+buffer
			if !hintTime.IsZero() && hintTime.After(end) {
				end = hintTime.Add(contestInferEndBuffer)
			}
			return start, end
		}
	}

	// 3) 无日历：按平台理解 hint
	if !hintTime.IsZero() {
		switch platform {
		case spider.AtCoder:
			// history JSON 的 EndTime → contest_logs.time
			end = hintTime.Add(contestInferEndBuffer)
			start = hintTime.Add(-dur)
			return start, end
		case spider.CodeForces, "Codeforces":
			// rating 结算时间在赛后；向前多看一段覆盖赛时
			// 默认赛长 2h，结算可能再晚几小时
			start = hintTime.Add(-(dur + 3*time.Hour))
			end = hintTime.Add(contestInferEndBuffer)
			return start, end
		default:
			// 多数平台 hint 当开赛
			start = hintTime
			end = hintTime.Add(dur + contestInferEndBuffer)
			return start, end
		}
	}

	// 4) 兜底
	return ResolveContestWindow(db, platform, contestID, hintTime)
}

// collectWantExternalIDs 格子 externalId + 题号 label → 要匹配的 external_id 集合。
func collectWantExternalIDs(db *gorm.DB, platform, contestID, label, externalID string) (want []string, outLabel, outExt string) {
	label = strings.TrimSpace(label)
	externalID = strings.TrimSpace(externalID)
	outLabel, outExt = label, externalID
	seen := map[string]struct{}{}
	add := func(s string) {
		s = strings.TrimSpace(s)
		if s == "" {
			return
		}
		if _, ok := seen[s]; ok {
			return
		}
		seen[s] = struct{}{}
		want = append(want, s)
	}
	add(externalID)

	probSet := loadContestProblemSet(db, platform, contestID)
	if externalID == "" && label != "" {
		for ext, lb := range probSet {
			if strings.EqualFold(lb, label) || strings.EqualFold(ext, label) {
				add(ext)
				if outExt == "" {
					outExt = ext
				}
				if outLabel == "" {
					outLabel = lb
				}
			}
		}
	}
	if label != "" {
		for ext, lb := range probSet {
			if strings.EqualFold(lb, label) {
				add(ext)
			}
		}
	}
	if outLabel == "" && externalID != "" {
		if lb, ok := probSet[externalID]; ok {
			outLabel = lb
		} else if lb, ok := probSet[strings.ToLower(externalID)]; ok {
			outLabel = lb
		}
	}
	// 无目录时 CF 常见 external = contestId+label
	if externalID == "" && label != "" && contestID != "" {
		if platform == spider.CodeForces || platform == "Codeforces" {
			add(contestID + label)
			if outExt == "" {
				outExt = contestID + label
			}
		}
		if platform == spider.AtCoder {
			// abc467 + A → abc467_a
			add(strings.ToLower(contestID + "_" + label))
			if outExt == "" {
				outExt = strings.ToLower(contestID + "_" + label)
			}
		}
	}
	return want, outLabel, outExt
}

// ListContestCellSubmits 按 external_id 反查 submit_logs，再筛赛时窗口。
// label / externalID 至少一个非空；优先 externalID（与 contest_user_problems 一致）。
func ListContestCellSubmits(
	db *gorm.DB,
	platform, contestID string,
	userID int64,
	label, externalID string,
	hintTime time.Time,
) (list []ContestCellSubmit, start, end time.Time, err error) {
	if db == nil || userID == 0 {
		return nil, time.Time{}, time.Time{}, nil
	}
	platform = strings.TrimSpace(platform)
	contestID = strings.TrimSpace(contestID)
	if platform == "" || contestID == "" {
		return nil, time.Time{}, time.Time{}, nil
	}
	wantExt, _, _ := collectWantExternalIDs(db, platform, contestID, label, externalID)
	if len(wantExt) == 0 && strings.TrimSpace(label) == "" {
		return nil, time.Time{}, time.Time{}, nil
	}

	start, end = resolveCellSubmitWindow(db, platform, contestID, hintTime)

	// 大小写不敏感匹配 external_id
	lowerExt := make([]string, 0, len(wantExt))
	for _, e := range wantExt {
		lowerExt = append(lowerExt, strings.ToLower(e))
	}

	var rows []model.SubmitLog
	q := db.Model(&model.SubmitLog{}).
		Where("platform = ? AND user_id = ?", platform, userID).
		Where("time >= ? AND time <= ?", start, end)
	if platform == spider.LeetCode {
		q = q.Where("submit_id LIKE ?", "lc-prob-%")
	}

	// 主路径：external_id 反查；辅：本场 contest + 题号（历史行未写 external_id）
	// 用 LOWER/LIKE 兼容 Postgres 与单测 SQLite
	label = strings.TrimSpace(label)
	if len(wantExt) > 0 && label != "" {
		q = q.Where(
			`(LOWER(external_id) IN ?)
			 OR (
			   (contest = ? OR contest = ?)
			   AND (external_id = '' OR external_id IS NULL)
			   AND (problem = ? OR problem LIKE ? OR problem LIKE ? OR problem LIKE ?)
			 )`,
			lowerExt,
			contestID, "-"+contestID,
			label, label+"-%", label+" %", label+".%",
		)
	} else if len(wantExt) > 0 {
		q = q.Where("LOWER(external_id) IN ?", lowerExt)
	} else {
		q = q.Where(
			`(contest = ? OR contest = ?)
			 AND (problem = ? OR problem LIKE ? OR problem LIKE ? OR problem LIKE ?)`,
			contestID, "-"+contestID,
			label, label+"-%", label+" %", label+".%",
		)
	}

	if err = q.Order("time ASC").Limit(contestCellSubmitLimit).Find(&rows).Error; err != nil {
		return nil, start, end, err
	}

	// 二次确认：external_id / 解析后的 external 必须落在 wantExt（防 contest ILIKE 误伤）
	wantSet := map[string]struct{}{}
	for _, e := range lowerExt {
		wantSet[e] = struct{}{}
	}

	out := make([]ContestCellSubmit, 0, len(rows))
	for _, r := range rows {
		if model.IsLeetCodeSyntheticSubmit(platform, r.SubmitID) {
			continue
		}
		ext, _ := resolveSubmitExternal(platform, contestID, r.Contest, r.Problem, r.ExternalID)
		extKey := strings.ToLower(strings.TrimSpace(firstNonEmpty(ext, r.ExternalID)))
		if len(wantSet) > 0 {
			if _, ok := wantSet[extKey]; !ok {
				// 允许 problem 前缀匹配 label 且 contest 精确为本场
				p := strings.TrimSpace(r.Problem)
				cField := strings.TrimSpace(r.Contest)
				labelOK := label != "" && (
					strings.EqualFold(p, label) ||
						strings.HasPrefix(p, label+"-") ||
						strings.HasPrefix(p, label+" ") ||
						strings.HasPrefix(strings.ToUpper(p), strings.ToUpper(label)+"."))
				contestOK := cField == contestID || cField == "-"+contestID || strings.EqualFold(cField, contestID)
				if !(labelOK && contestOK) {
					continue
				}
			}
		}
		item := ContestCellSubmit{
			ID:         r.ID,
			SubmitID:   r.SubmitID,
			Status:     r.Status,
			Lang:       r.Lang,
			Time:       r.Time,
			Problem:    r.Problem,
			Contest:    r.Contest,
			ExternalID: firstNonEmpty(ext, r.ExternalID),
			ProblemID:  r.ProblemID,
		}
		if !start.IsZero() && !r.Time.IsZero() {
			sec := int(r.Time.Sub(start).Seconds())
			if sec < 0 {
				sec = 0
			}
			item.RelativeSec = &sec
		}
		out = append(out, item)
	}
	return out, start, end, nil
}
