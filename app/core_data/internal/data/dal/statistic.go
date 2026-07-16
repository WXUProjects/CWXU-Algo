package dal

import (
	"context"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
	"github.com/redis/go-redis/v9"
	"gorm.io/gorm"
)

// StatisticDal 统计数据访问层
type StatisticDal struct {
	db  *gorm.DB
	rdb *redis.Client
}

// NewStatisticDal 创建统计数据访问层
func NewStatisticDal(db *gorm.DB, rdb *redis.Client) *StatisticDal {
	return &StatisticDal{
		db:  db,
		rdb: rdb,
	}
}

// DailyCount 热力图每日统计
type DailyCount struct {
	Day time.Time
	Cnt int64
}

// HeatmapQuery 查询热力图数据
func (d *StatisticDal) HeatmapQuery(ctx context.Context, startDate, endDate string, userId int64, isAc bool) ([]DailyCount, error) {
	return d.HeatmapQueryScoped(ctx, startDate, endDate, userId, isAc, nil)
}

// HeatmapQueryScoped userId=0 时 memberIDs 限制组织；nil 表示不限制（全站）
// 优先读 daily_user_stats（1w 日活友好）；表无数据时回退 submit_logs。
func (d *StatisticDal) HeatmapQueryScoped(ctx context.Context, startDate, endDate string, userId int64, isAc bool, memberIDs []int64) ([]DailyCount, error) {
	if userId == 0 && memberIDs != nil && len(memberIDs) == 0 {
		return []DailyCount{}, nil
	}

	// 日汇总：行数 = 用户×活跃天，比明细少 1–2 个数量级
	cntCol := "submit_cnt"
	if isAc {
		cntCol = "ac_cnt"
	}
	agg := d.db.WithContext(ctx).
		Table("daily_user_stats").
		Select("day, SUM("+cntCol+") AS cnt").
		Where("day >= ?::date AND day <= ?::date", startDate, endDate)
	if userId != 0 {
		agg = agg.Where("user_id = ?", userId)
	} else if memberIDs != nil {
		agg = agg.Where("user_id IN ?", memberIDs)
	}
	agg = agg.Group("day")

	var result []DailyCount
	err := d.db.WithContext(ctx).Raw(`
		SELECT days.day, COALESCE(s.cnt, 0) AS cnt
		FROM (
			SELECT generate_series(
				?::date,
				?::date,
				INTERVAL '1 day'
			)::date AS day
		) days
		LEFT JOIN (?) s ON s.day = days.day
		ORDER BY days.day
	`, startDate, endDate, agg).Scan(&result).Error
	if err != nil {
		return nil, err
	}
	// 汇总表尚未回填时可能全 0：仅当该用户/范围完全无预聚合行时回退热表
	// （热表仅 6 个月，回退不能当全历史真相）
	if heatmapAllZero(result) {
		var n int64
		probe := d.db.WithContext(ctx).Table("daily_user_stats")
		if userId != 0 {
			probe = probe.Where("user_id = ?", userId)
		} else if memberIDs != nil {
			probe = probe.Where("user_id IN ?", memberIDs)
		}
		_ = probe.Limit(1).Count(&n).Error
		if n == 0 {
			return d.heatmapFromSubmitLogs(ctx, startDate, endDate, userId, isAc, memberIDs)
		}
	}
	return result, nil
}

func heatmapAllZero(rows []DailyCount) bool {
	for _, r := range rows {
		if r.Cnt != 0 {
			return false
		}
	}
	return true
}

// heatmapFromSubmitLogs 回退路径（汇总表空）
func (d *StatisticDal) heatmapFromSubmitLogs(ctx context.Context, startDate, endDate string, userId int64, isAc bool, memberIDs []int64) ([]DailyCount, error) {
	agg := d.db.WithContext(ctx).
		Table("submit_logs").
		Select("date_trunc('day', time)::date AS day, COUNT(*) AS cnt").
		Where("time >= ?::date AND time < (?::date + INTERVAL '1 day')", startDate, endDate)
	if isAc {
		agg = agg.Where("is_ac = true")
	} else {
		agg = agg.Where(model.SQLExcludeLeetCodeNonSubmit)
	}
	if userId != 0 {
		agg = agg.Where("user_id = ?", userId)
	} else if memberIDs != nil {
		agg = agg.Where("user_id IN ?", memberIDs)
	}
	agg = agg.Group("date_trunc('day', time)::date")

	var result []DailyCount
	err := d.db.WithContext(ctx).Raw(`
		SELECT days.day, COALESCE(s.cnt, 0) AS cnt
		FROM (
			SELECT generate_series(?::date, ?::date, INTERVAL '1 day')::date AS day
		) days
		LEFT JOIN (?) s ON s.day = days.day
		ORDER BY days.day
	`, startDate, endDate, agg).Scan(&result).Error
	return result, err
}

// PeriodSubmitCount 提交次数统计
type PeriodSubmitCount struct {
	Today     int64
	ThisWeek  int64
	LastWeek  int64
	ThisMonth int64
	LastMonth int64
	ThisYear  int64
	LastYear  int64
	Total     int64
}

// PeriodAcCount AC 统计
// 个人(userId>0)：时段+Total=按题去重题数；TotalRaw=累计 AC 次数
// 组织/全站(userId=-1)：全部为 AC 条数（is_ac），不做 DISTINCT
type PeriodAcCount struct {
	Today     int64
	ThisWeek  int64
	LastWeek  int64
	ThisMonth int64
	LastMonth int64
	ThisYear  int64
	LastYear  int64
	Total     int64 // 个人=去重题数；组织/全站=AC 条数
	TotalRaw  int64 // 个人=累计 AC 次数；组织/全站=同 Total
}

// acProblemKeySQL 同一用户下 AC 去重键：优先 problem_id，其次 external_id，最后 problem 文本。
// 力扣合成 AC 无 problem_id，依赖 external_id / problem（每题一条，本身已去重）。
const acProblemKeySQL = `COALESCE(
	CASE WHEN problem_id IS NOT NULL AND problem_id <> 0 THEN 'p:' || problem_id::text END,
	CASE WHEN external_id IS NOT NULL AND btrim(external_id) <> '' THEN 'e:' || platform || ':' || external_id END,
	'n:' || platform || ':' || COALESCE(problem, '')
)`

// GetPeriodCount 获取时间段统计数据
func (d *StatisticDal) GetPeriodCount(userId int64) (PeriodSubmitCount, PeriodAcCount, error) {
	return d.GetPeriodCountScoped(userId, nil)
}

// GetPeriodCountScoped userId=-1 时 memberIDs 限制组织
// 提交 / 组织 AC → daily_user_stats；个人 AC 去重 → user_ac_problem_* 预聚合
func (d *StatisticDal) GetPeriodCountScoped(userId int64, memberIDs []int64) (PeriodSubmitCount, PeriodAcCount, error) {
	if userId == -1 && memberIDs != nil && len(memberIDs) == 0 {
		return PeriodSubmitCount{}, PeriodAcCount{}, nil
	}
	now := time.Now()

	thisWeekStart := getWeekStart(now)
	lastWeekStart := thisWeekStart.Add(-7 * 24 * time.Hour)
	thisMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	lastMonthStart := thisMonthStart.AddDate(0, -1, 0)
	thisYearStart := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
	lastYearStart := thisYearStart.AddDate(-1, 0, 0)

	// 日汇总按「自然日」
	todayDay := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	weekDay := time.Date(thisWeekStart.Year(), thisWeekStart.Month(), thisWeekStart.Day(), 0, 0, 0, 0, now.Location())
	lastWeekDay := time.Date(lastWeekStart.Year(), lastWeekStart.Month(), lastWeekStart.Day(), 0, 0, 0, 0, now.Location())
	monthDay := time.Date(thisMonthStart.Year(), thisMonthStart.Month(), thisMonthStart.Day(), 0, 0, 0, 0, now.Location())
	lastMonthDay := time.Date(lastMonthStart.Year(), lastMonthStart.Month(), lastMonthStart.Day(), 0, 0, 0, 0, now.Location())
	yearDay := time.Date(thisYearStart.Year(), thisYearStart.Month(), thisYearStart.Day(), 0, 0, 0, 0, now.Location())
	lastYearDay := time.Date(lastYearStart.Year(), lastYearStart.Month(), lastYearStart.Day(), 0, 0, 0, 0, now.Location())

	applyDailyScope := func(q *gorm.DB) *gorm.DB {
		if userId != -1 {
			return q.Where("user_id = ?", userId)
		}
		if memberIDs != nil {
			return q.Where("user_id IN ?", memberIDs)
		}
		return q
	}

	// 提交：SUM(submit_cnt) 按日 FILTER
	dailySelect := func(col string) string {
		return `
		COALESCE(SUM(` + col + `) FILTER (WHERE day = ?::date), 0) AS today,
		COALESCE(SUM(` + col + `) FILTER (WHERE day >= ?::date AND day <= ?::date), 0) AS this_week,
		COALESCE(SUM(` + col + `) FILTER (WHERE day >= ?::date AND day < ?::date), 0) AS last_week,
		COALESCE(SUM(` + col + `) FILTER (WHERE day >= ?::date AND day <= ?::date), 0) AS this_month,
		COALESCE(SUM(` + col + `) FILTER (WHERE day >= ?::date AND day < ?::date), 0) AS last_month,
		COALESCE(SUM(` + col + `) FILTER (WHERE day >= ?::date AND day <= ?::date), 0) AS this_year,
		COALESCE(SUM(` + col + `) FILTER (WHERE day >= ?::date AND day < ?::date), 0) AS last_year,
		COALESCE(SUM(` + col + `), 0) AS total`
	}
	// 今日单日；本周/本月/本年：day ∈ [start, today]；上周/上月/上年：半开区间
	dailyArgs := []interface{}{
		todayDay.Format("2006-01-02"),
		weekDay.Format("2006-01-02"), todayDay.Format("2006-01-02"),
		lastWeekDay.Format("2006-01-02"), weekDay.Format("2006-01-02"),
		monthDay.Format("2006-01-02"), todayDay.Format("2006-01-02"),
		lastMonthDay.Format("2006-01-02"), monthDay.Format("2006-01-02"),
		yearDay.Format("2006-01-02"), todayDay.Format("2006-01-02"),
		lastYearDay.Format("2006-01-02"), yearDay.Format("2006-01-02"),
	}

	var submit PeriodSubmitCount
	if err := applyDailyScope(d.db.Table("daily_user_stats")).
		Select(dailySelect("submit_cnt"), dailyArgs...).
		Scan(&submit).Error; err != nil {
		return PeriodSubmitCount{}, PeriodAcCount{}, err
	}

	// 组织/全站 AC 条数：日汇总 ac_cnt
	if userId == -1 {
		var ac PeriodAcCount
		if err := applyDailyScope(d.db.Table("daily_user_stats")).
			Select(dailySelect("ac_cnt"), dailyArgs...).
			Scan(&ac).Error; err != nil {
			return PeriodSubmitCount{}, PeriodAcCount{}, err
		}
		ac.TotalRaw = ac.Total
		return submit, ac, nil
	}

	// 个人 AC 去重：预聚合表（写入时维护）；表空则回退明细 DISTINCT
	ac, err := PeriodAcDistinctFromPreagg(d.db, userId, now)
	if err != nil {
		return PeriodSubmitCount{}, PeriodAcCount{}, err
	}
	if ac.Total == 0 && ac.Today == 0 && ac.ThisWeek == 0 {
		// 预聚合尚未回填时回退一次明细，避免启动竞态显示全 0
		var n int64
		_ = d.db.Table("user_ac_problems").Where("user_id = ?", userId).Limit(1).Count(&n).Error
		if n == 0 {
			var hasAC int64
			_ = d.db.Table("submit_logs").Where("user_id = ? AND is_ac = true", userId).Limit(1).Count(&hasAC).Error
			if hasAC > 0 {
				ac, err = d.periodAcDistinctFromSubmitLogs(userId, now)
				if err != nil {
					return PeriodSubmitCount{}, PeriodAcCount{}, err
				}
			}
		}
	}
	return submit, ac, nil
}

// periodAcDistinctFromSubmitLogs 回退：与历史 COUNT(DISTINCT) 语义一致
func (d *StatisticDal) periodAcDistinctFromSubmitLogs(userId int64, now time.Time) (PeriodAcCount, error) {
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	thisWeekStart := getWeekStart(now)
	lastWeekStart := thisWeekStart.Add(-7 * 24 * time.Hour)
	thisMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	lastMonthStart := thisMonthStart.AddDate(0, -1, 0)
	thisYearStart := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
	lastYearStart := thisYearStart.AddDate(-1, 0, 0)
	periodArgs := []interface{}{
		todayStart, now, thisWeekStart, now, lastWeekStart, thisWeekStart,
		thisMonthStart, now, lastMonthStart, thisMonthStart,
		thisYearStart, now, lastYearStart, thisYearStart,
	}
	distinctSelect := `
		COUNT(DISTINCT CASE WHEN time >= ? AND time < ? THEN ` + acProblemKeySQL + ` END) AS today,
		COUNT(DISTINCT CASE WHEN time >= ? AND time < ? THEN ` + acProblemKeySQL + ` END) AS this_week,
		COUNT(DISTINCT CASE WHEN time >= ? AND time < ? THEN ` + acProblemKeySQL + ` END) AS last_week,
		COUNT(DISTINCT CASE WHEN time >= ? AND time < ? THEN ` + acProblemKeySQL + ` END) AS this_month,
		COUNT(DISTINCT CASE WHEN time >= ? AND time < ? THEN ` + acProblemKeySQL + ` END) AS last_month,
		COUNT(DISTINCT CASE WHEN time >= ? AND time < ? THEN ` + acProblemKeySQL + ` END) AS this_year,
		COUNT(DISTINCT CASE WHEN time >= ? AND time < ? THEN ` + acProblemKeySQL + ` END) AS last_year,
		COUNT(DISTINCT ` + acProblemKeySQL + `) AS total,
		COUNT(*) AS total_raw`
	var ac PeriodAcCount
	err := d.db.Table("submit_logs").Where("user_id = ? AND is_ac = true", userId).
		Select(distinctSelect, periodArgs...).Scan(&ac).Error
	return ac, err
}

// RankItem 排行榜项
type RankItem struct {
	Rank   int64
	UserID int64
	Name   string
	Score  int64
}

// GetRank 获取排行榜数据（相对时间：日/周/月）
func (d *StatisticDal) GetRank(ctx context.Context, userId int64, timeType, scoreType string, groupId int64, page, pageSize int64) ([]RankItem, int64, error) {
	now := time.Now()
	var startTime time.Time
	var endTime = now

	switch timeType {
	case "日":
		startTime = time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	case "周":
		startTime = getWeekStart(now)
	case "月":
		startTime = time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	default:
		startTime = time.Time{}
		endTime = time.Now().Add(100 * 365 * 24 * time.Hour)
	}
	return d.GetRankByRange(ctx, startTime, endTime, scoreType, groupId, page, pageSize)
}

// GetRankByRange 按绝对时间区间排行（end 为开区间上界）
func (d *StatisticDal) GetRankByRange(ctx context.Context, startTime, endTime time.Time, scoreType string, groupId int64, page, pageSize int64) ([]RankItem, int64, error) {
	return d.GetRankByRangeScoped(ctx, startTime, endTime, scoreType, groupId, page, pageSize, nil)
}

// GetRankByRangeScoped memberIDs 非 nil 时限制组织成员
// scoreType=submit：日汇总 SUM(submit_cnt)；scoreType=ac：仍 DISTINCT 题（语义）
func (d *StatisticDal) GetRankByRangeScoped(ctx context.Context, startTime, endTime time.Time, scoreType string, groupId int64, page, pageSize int64, memberIDs []int64) ([]RankItem, int64, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	if pageSize > 50 {
		// 2c4g：排行页默认上限收紧
		pageSize = 50
	}
	if memberIDs != nil && len(memberIDs) == 0 {
		return []RankItem{}, 0, nil
	}

	type RankQueryResult struct {
		UserID int64
		Score  int64
	}

	offset := (page - 1) * pageSize

	// 提交排行：走日汇总
	if scoreType != "ac" {
		startDay := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, startTime.Location())
		// endTime 为开区间上界 → 日汇总用 endDay 的前一天
		endExclusive := endTime
		endDay := endExclusive.Add(-time.Nanosecond)
		endDay = time.Date(endDay.Year(), endDay.Month(), endDay.Day(), 0, 0, 0, 0, endDay.Location())

		base := d.db.WithContext(ctx).Table("daily_user_stats").
			Where("day >= ?::date AND day <= ?::date", startDay.Format("2006-01-02"), endDay.Format("2006-01-02")).
			Where("submit_cnt > 0")
		if memberIDs != nil {
			base = base.Where("user_id IN ?", memberIDs)
		}
		// group_id 在日汇总中不存在：忽略（原 submit_logs 也未必有可靠 group_id）
		_ = groupId

		var total int64
		countSub := base.Select("user_id").Group("user_id")
		if err := d.db.WithContext(ctx).Table("(?) AS t", countSub).Count(&total).Error; err != nil {
			return nil, 0, err
		}

		var results []RankQueryResult
		err := base.Select("user_id, SUM(submit_cnt) AS score").
			Group("user_id").
			Order("score DESC").
			Offset(int(offset)).
			Limit(int(pageSize)).
			Scan(&results).Error
		if err != nil {
			return nil, 0, err
		}
		items := make([]RankItem, len(results))
		for i, r := range results {
			items[i] = RankItem{Rank: offset + int64(i+1), UserID: r.UserID, Score: r.Score}
		}
		return items, total, nil
	}

	// AC 去重排行：user_ac_problem_days（窗内 AC 过的题数）
	startDay := time.Date(startTime.Year(), startTime.Month(), startTime.Day(), 0, 0, 0, 0, startTime.Location())
	endDay := endTime.Add(-time.Nanosecond)
	endDay = time.Date(endDay.Year(), endDay.Month(), endDay.Day(), 0, 0, 0, 0, endDay.Location())
	// 全时段：start 为零值时不限制下界
	base := d.db.WithContext(ctx).Table("user_ac_problem_days")
	if !startTime.IsZero() {
		base = base.Where("day >= ?::date AND day <= ?::date", startDay.Format("2006-01-02"), endDay.Format("2006-01-02"))
	}
	if memberIDs != nil {
		base = base.Where("user_id IN ?", memberIDs)
	}

	var total int64
	countSub := base.Select("user_id").Group("user_id")
	if err := d.db.WithContext(ctx).Table("(?) AS t", countSub).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	var results []RankQueryResult
	err := base.Select("user_id, COUNT(DISTINCT problem_key) AS score").
		Group("user_id").
		Order("score DESC").
		Offset(int(offset)).
		Limit(int(pageSize)).
		Scan(&results).Error
	if err != nil {
		return nil, 0, err
	}

	items := make([]RankItem, len(results))
	for i, r := range results {
		items[i] = RankItem{
			Rank:   offset + int64(i+1),
			UserID: r.UserID,
			Name:   "",
			Score:  r.Score,
		}
	}
	return items, total, nil
}

func (d *StatisticDal) countQuery(userId int64, start, end time.Time) int64 {
	return d.countQueryScoped(userId, start, end, nil)
}

func (d *StatisticDal) countQueryScoped(userId int64, start, end time.Time, memberIDs []int64) int64 {
	if userId == -1 && memberIDs != nil && len(memberIDs) == 0 {
		return 0
	}
	var count int64
	query := d.db.Table("submit_logs").
		Where("time >= ? AND time < ?", start, end).
		Where(model.SQLExcludeLeetCodeNonSubmit)
	if userId != -1 {
		query = query.Where("user_id = ?", userId)
	} else if memberIDs != nil {
		query = query.Where("user_id IN ?", memberIDs)
	}
	if err := query.Count(&count).Error; err != nil {
		log.Errorf("countQuery error: %v", err)
	}
	return count
}

func (d *StatisticDal) countQueryTotal(userId int64) int64 {
	return d.countQueryTotalScoped(userId, nil)
}

func (d *StatisticDal) countQueryTotalScoped(userId int64, memberIDs []int64) int64 {
	if userId == -1 && memberIDs != nil && len(memberIDs) == 0 {
		return 0
	}
	var count int64
	query := d.db.Table("submit_logs").
		Where(model.SQLExcludeLeetCodeNonSubmit)
	if userId != -1 {
		query = query.Where("user_id = ?", userId)
	} else if memberIDs != nil {
		query = query.Where("user_id IN ?", memberIDs)
	}
	if err := query.Count(&count).Error; err != nil {
		log.Errorf("countQueryTotal error: %v", err)
	}
	return count
}

func (d *StatisticDal) countAcDistinctQuery(userId int64, start, end time.Time) int64 {
	return d.countAcDistinctQueryScoped(userId, start, end, nil)
}

func (d *StatisticDal) countAcDistinctQueryScoped(userId int64, start, end time.Time, memberIDs []int64) int64 {
	if userId == -1 && memberIDs != nil && len(memberIDs) == 0 {
		return 0
	}
	return d.countAcDistinct(userId, memberIDs, start, end, true)
}

func (d *StatisticDal) countAcDistinctTotal(userId int64) int64 {
	return d.countAcDistinctTotalScoped(userId, nil)
}

func (d *StatisticDal) countAcDistinctTotalScoped(userId int64, memberIDs []int64) int64 {
	if userId == -1 && memberIDs != nil && len(memberIDs) == 0 {
		return 0
	}
	return d.countAcDistinct(userId, memberIDs, time.Time{}, time.Time{}, false)
}

// countAcDistinct 统计 AC 题数：按题去重。
// 时间窗内：该题在窗内有过 AC 即计 1（同一题多次提交只算一题）。
func (d *StatisticDal) countAcDistinct(userId int64, memberIDs []int64, start, end time.Time, useRange bool) int64 {
	var count int64
	// 组织合计：user_id + 题键；个人：仅题键
	key := acProblemKeySQL
	if userId == -1 {
		key = `(user_id::text || '|' || ` + acProblemKeySQL + `)`
	}
	query := d.db.Table("submit_logs").
		Select("COUNT(DISTINCT "+key+")").
		Where("is_ac = true")
	if useRange {
		query = query.Where("time >= ? AND time < ?", start, end)
	}
	if userId != -1 {
		query = query.Where("user_id = ?", userId)
	} else if memberIDs != nil {
		query = query.Where("user_id IN ?", memberIDs)
	}
	if err := query.Scan(&count).Error; err != nil {
		log.Errorf("countAcDistinct error: %v", err)
	}
	return count
}

// countAcRawQueryScoped 时段内 AC 次数（不去重）
func (d *StatisticDal) countAcRawQueryScoped(userId int64, start, end time.Time, memberIDs []int64) int64 {
	if userId == -1 && memberIDs != nil && len(memberIDs) == 0 {
		return 0
	}
	var count int64
	query := d.db.Table("submit_logs").
		Where("is_ac = true").
		Where("time >= ? AND time < ?", start, end)
	if userId != -1 {
		query = query.Where("user_id = ?", userId)
	} else if memberIDs != nil {
		query = query.Where("user_id IN ?", memberIDs)
	}
	if err := query.Count(&count).Error; err != nil {
		log.Errorf("countAcRawQuery error: %v", err)
	}
	return count
}

// countAcRawTotalScoped 累计 AC 次数（不去重，每条 AC 记录计 1）
func (d *StatisticDal) countAcRawTotalScoped(userId int64, memberIDs []int64) int64 {
	if userId == -1 && memberIDs != nil && len(memberIDs) == 0 {
		return 0
	}
	var count int64
	query := d.db.Table("submit_logs").
		Where("is_ac = true")
	if userId != -1 {
		query = query.Where("user_id = ?", userId)
	} else if memberIDs != nil {
		query = query.Where("user_id IN ?", memberIDs)
	}
	if err := query.Count(&count).Error; err != nil {
		log.Errorf("countAcRawTotal error: %v", err)
	}
	return count
}

// getWeekStart 获取本周周一 00:00:00
func getWeekStart(t time.Time) time.Time {
	weekday := t.Weekday()
	if weekday == time.Sunday {
		weekday = 7
	}
	days := int(weekday - time.Monday)
	return t.AddDate(0, 0, -days).Truncate(24 * time.Hour)
}
