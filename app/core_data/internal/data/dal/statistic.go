package dal

import (
	"context"
	"time"

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
func (d *StatisticDal) HeatmapQueryScoped(ctx context.Context, startDate, endDate string, userId int64, isAc bool, memberIDs []int64) ([]DailyCount, error) {
	sub := d.db.
		Table("submit_logs").
		Select("id, time")
	if isAc {
		sub = sub.Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
	} else {
		// 力扣合成 AC 只用于做题数，不进入提交热力图（真实提交由 lc-cal 记录承担）
		sub = sub.Where("NOT (platform = ? AND submit_id LIKE ?)", "LeetCode", "lc-ac-%")
	}
	if userId != 0 {
		sub = sub.Where("user_id = ?", userId)
	} else if memberIDs != nil {
		if len(memberIDs) == 0 {
			return []DailyCount{}, nil
		}
		sub = sub.Where("user_id IN ?", memberIDs)
	}

	var result []DailyCount
	err := d.db.Raw(`
		SELECT days.day, COUNT(s.id) AS cnt
		FROM (
			SELECT generate_series(
				?::date,
				?::date,
				INTERVAL '1 day'
			) AS day
		) days
		LEFT JOIN (?) s
		ON s.time >= days.day
		AND s.time < days.day + INTERVAL '1 day'
		GROUP BY days.day
		ORDER BY days.day
	`, startDate, endDate, sub).Scan(&result).Error

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
// 组织/全站(userId=-1)：全部为 AC 条数（status 含 AC/正确/OK），不做 DISTINCT
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
func (d *StatisticDal) GetPeriodCountScoped(userId int64, memberIDs []int64) (PeriodSubmitCount, PeriodAcCount, error) {
	now := time.Now()

	// 日期范围计算
	todayStart := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	thisWeekStart := getWeekStart(now)
	lastWeekStart := thisWeekStart.Add(-7 * 24 * time.Hour)
	thisMonthStart := time.Date(now.Year(), now.Month(), 1, 0, 0, 0, 0, now.Location())
	lastMonthStart := thisMonthStart.AddDate(0, -1, 0)
	thisYearStart := time.Date(now.Year(), 1, 1, 0, 0, 0, 0, now.Location())
	lastYearStart := thisYearStart.AddDate(-1, 0, 0)

	// 提交次数统计
	submit := PeriodSubmitCount{
		Today:     d.countQueryScoped(userId, todayStart, now, memberIDs),
		ThisWeek:  d.countQueryScoped(userId, thisWeekStart, now, memberIDs),
		LastWeek:  d.countQueryScoped(userId, lastWeekStart, thisWeekStart, memberIDs),
		ThisMonth: d.countQueryScoped(userId, thisMonthStart, now, memberIDs),
		LastMonth: d.countQueryScoped(userId, lastMonthStart, thisMonthStart, memberIDs),
		ThisYear:  d.countQueryScoped(userId, thisYearStart, now, memberIDs),
		LastYear:  d.countQueryScoped(userId, lastYearStart, thisYearStart, memberIDs),
		Total:     d.countQueryTotalScoped(userId, memberIDs),
	}

	// 组织/全站：只数 AC 条数，快且简单；个人：时段+Total 去重题数，TotalRaw 为 AC 次数
	if userId == -1 {
		ac := PeriodAcCount{
			Today:     d.countAcRawQueryScoped(userId, todayStart, now, memberIDs),
			ThisWeek:  d.countAcRawQueryScoped(userId, thisWeekStart, now, memberIDs),
			LastWeek:  d.countAcRawQueryScoped(userId, lastWeekStart, thisWeekStart, memberIDs),
			ThisMonth: d.countAcRawQueryScoped(userId, thisMonthStart, now, memberIDs),
			LastMonth: d.countAcRawQueryScoped(userId, lastMonthStart, thisMonthStart, memberIDs),
			ThisYear:  d.countAcRawQueryScoped(userId, thisYearStart, now, memberIDs),
			LastYear:  d.countAcRawQueryScoped(userId, lastYearStart, thisYearStart, memberIDs),
			Total:     d.countAcRawTotalScoped(userId, memberIDs),
		}
		ac.TotalRaw = ac.Total
		return submit, ac, nil
	}

	ac := PeriodAcCount{
		Today:     d.countAcDistinctQueryScoped(userId, todayStart, now, memberIDs),
		ThisWeek:  d.countAcDistinctQueryScoped(userId, thisWeekStart, now, memberIDs),
		LastWeek:  d.countAcDistinctQueryScoped(userId, lastWeekStart, thisWeekStart, memberIDs),
		ThisMonth: d.countAcDistinctQueryScoped(userId, thisMonthStart, now, memberIDs),
		LastMonth: d.countAcDistinctQueryScoped(userId, lastMonthStart, thisMonthStart, memberIDs),
		ThisYear:  d.countAcDistinctQueryScoped(userId, thisYearStart, now, memberIDs),
		LastYear:  d.countAcDistinctQueryScoped(userId, lastYearStart, thisYearStart, memberIDs),
		Total:     d.countAcDistinctTotalScoped(userId, memberIDs),
		TotalRaw:  d.countAcRawTotalScoped(userId, memberIDs),
	}
	return submit, ac, nil
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
func (d *StatisticDal) GetRankByRangeScoped(ctx context.Context, startTime, endTime time.Time, scoreType string, groupId int64, page, pageSize int64, memberIDs []int64) ([]RankItem, int64, error) {
	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 {
		pageSize = 10
	}
	if pageSize > 100 {
		pageSize = 100
	}
	if memberIDs != nil && len(memberIDs) == 0 {
		return []RankItem{}, 0, nil
	}

	type RankQueryResult struct {
		UserID int64
		Score  int64
	}

	applyFilters := func(q *gorm.DB) *gorm.DB {
		q = q.Where("time >= ? AND time < ?", startTime, endTime)
		if groupId != -1 && groupId != 0 {
			q = q.Where("group_id = ?", groupId)
		}
		if memberIDs != nil {
			q = q.Where("user_id IN ?", memberIDs)
		}
		if scoreType == "ac" {
			q = q.Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
		} else {
			q = q.Where("NOT (platform = ? AND submit_id LIKE ?)", "LeetCode", "lc-ac-%")
		}
		return q
	}

	var total int64
	countSub := applyFilters(d.db.WithContext(ctx).Table("submit_logs")).Select("user_id").Group("user_id")
	if err := d.db.WithContext(ctx).Table("(?) AS t", countSub).Count(&total).Error; err != nil {
		return nil, 0, err
	}

	offset := (page - 1) * pageSize
	var selectClause string
	if scoreType == "ac" {
		// 按题去重，不按提交条数
		selectClause = "COUNT(DISTINCT " + acProblemKeySQL + ")"
	} else {
		selectClause = "COUNT(*)"
	}

	var results []RankQueryResult
	// 名称由上层按当前组织 org_display_name 填充，不读 submit_logs 上的 name
	err := applyFilters(d.db.WithContext(ctx).Table("submit_logs")).
		Select("user_id, " + selectClause + " as score").
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
		Where("NOT (platform = ? AND submit_id LIKE ?)", "LeetCode", "lc-ac-%")
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
		Where("NOT (platform = ? AND submit_id LIKE ?)", "LeetCode", "lc-ac-%")
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
		Select("COUNT(DISTINCT " + key + ")").
		Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
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
		Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%").
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
		Where("status ILIKE ? OR status ILIKE ? OR status ILIKE ?", "%AC%", "%正确%", "%OK%")
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