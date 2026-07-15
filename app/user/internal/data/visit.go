package data

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cwxu-algo/app/user/internal/data/model"

	"gorm.io/gorm/clause"
)

const visitTTL = 72 * time.Hour
const visitThrottle = 30 * time.Second

var visitLoc *time.Location

func init() {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		loc = time.FixedZone("CST", 8*3600)
	}
	visitLoc = loc
}

func visitDayKey(t time.Time) string {
	return t.In(visitLoc).Format("20060102")
}

func visitDayDate(t time.Time) time.Time {
	tt := t.In(visitLoc)
	return time.Date(tt.Year(), tt.Month(), tt.Day(), 0, 0, 0, 0, visitLoc)
}

// VisitRecord 一次访问上报结果
type VisitRecord struct {
	Counted bool
	Day     string // yyyy-MM-dd
}

// RecordVisit 记录访问：PV 节流去重；DAU 按 userId；UV 按 visitor/ip
func (d *Data) RecordVisit(ctx context.Context, userID uint, visitorID, clientIP, path string) (*VisitRecord, error) {
	if d == nil || d.RDB == nil {
		return &VisitRecord{}, nil
	}
	now := time.Now()
	day := visitDayKey(now)
	path = normalizeVisitPath(path)

	// 节流键：登录用户按 user，否则 visitor/ip
	throttleID := visitorThrottleID(userID, visitorID, clientIP)
	thKey := fmt.Sprintf("visit:th:%s:%s:%s", day, throttleID, path)
	ok, err := d.RDB.SetNX(ctx, thKey, "1", visitThrottle).Result()
	if err != nil {
		return nil, err
	}
	out := &VisitRecord{Day: now.In(visitLoc).Format("2006-01-02")}
	if !ok {
		// 节流内仍保证 DAU/UV 集合写入（首次以外）
		_ = d.touchUniques(ctx, day, userID, visitorID, clientIP)
		return out, nil
	}
	out.Counted = true

	pipe := d.RDB.Pipeline()
	pvKey := fmt.Sprintf("visit:pv:%s", day)
	pipe.Incr(ctx, pvKey)
	pipe.Expire(ctx, pvKey, visitTTL)
	if path != "" {
		pKey := fmt.Sprintf("visit:path:%s", day)
		pipe.HIncrBy(ctx, pKey, path, 1)
		pipe.Expire(ctx, pKey, visitTTL)
	}
	if userID > 0 {
		dauKey := fmt.Sprintf("visit:dau:%s", day)
		pipe.SAdd(ctx, dauKey, strconv.FormatUint(uint64(userID), 10))
		pipe.Expire(ctx, dauKey, visitTTL)
	}
	uvMember := uvMember(userID, visitorID, clientIP)
	if uvMember != "" {
		uvKey := fmt.Sprintf("visit:uv:%s", day)
		pipe.PFAdd(ctx, uvKey, uvMember)
		pipe.Expire(ctx, uvKey, visitTTL)
	}
	if clientIP != "" {
		ipKey := fmt.Sprintf("visit:ip:%s", day)
		pipe.PFAdd(ctx, ipKey, clientIP)
		pipe.Expire(ctx, ipKey, visitTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return nil, err
	}
	return out, nil
}

func (d *Data) touchUniques(ctx context.Context, day string, userID uint, visitorID, clientIP string) error {
	pipe := d.RDB.Pipeline()
	if userID > 0 {
		dauKey := fmt.Sprintf("visit:dau:%s", day)
		pipe.SAdd(ctx, dauKey, strconv.FormatUint(uint64(userID), 10))
		pipe.Expire(ctx, dauKey, visitTTL)
	}
	if m := uvMember(userID, visitorID, clientIP); m != "" {
		uvKey := fmt.Sprintf("visit:uv:%s", day)
		pipe.PFAdd(ctx, uvKey, m)
		pipe.Expire(ctx, uvKey, visitTTL)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// DayVisitStat 单日指标
type DayVisitStat struct {
	Date string
	PV   int64
	DAU  int64
	UV   int64
}

// GetDayVisitStat 优先 Redis 今日/昨日，否则 PG
func (d *Data) GetDayVisitStat(ctx context.Context, day time.Time) DayVisitStat {
	key := visitDayKey(day)
	dateStr := day.In(visitLoc).Format("2006-01-02")
	st := DayVisitStat{Date: dateStr}

	if d.RDB != nil {
		if pv, err := d.RDB.Get(ctx, "visit:pv:"+key).Int64(); err == nil {
			st.PV = pv
		}
		if n, err := d.RDB.SCard(ctx, "visit:dau:"+key).Result(); err == nil {
			st.DAU = n
		}
		if n, err := d.RDB.PFCount(ctx, "visit:uv:"+key).Result(); err == nil {
			st.UV = n
		}
		// Redis 有任一键则认为热数据可用
		if st.PV > 0 || st.DAU > 0 || st.UV > 0 {
			return st
		}
		// 今日即使为 0 也以 Redis 为准（避免被旧 PG 覆盖）
		if visitDayKey(time.Now()) == key {
			return st
		}
	}

	var row model.SiteVisitDaily
	dayDate := visitDayDate(day)
	if d.DB != nil {
		if err := d.DB.WithContext(ctx).Where("day = ?", dayDate).First(&row).Error; err == nil {
			st.PV = row.PV
			st.DAU = row.DAU
			st.UV = row.UV
		}
	}
	return st
}

// FlushVisitDay Redis → PG 固化某日
func (d *Data) FlushVisitDay(ctx context.Context, day time.Time) error {
	if d == nil || d.DB == nil || d.RDB == nil {
		return nil
	}
	key := visitDayKey(day)
	st := DayVisitStat{Date: day.In(visitLoc).Format("2006-01-02")}
	if pv, err := d.RDB.Get(ctx, "visit:pv:"+key).Int64(); err == nil {
		st.PV = pv
	}
	if n, err := d.RDB.SCard(ctx, "visit:dau:"+key).Result(); err == nil {
		st.DAU = n
	}
	if n, err := d.RDB.PFCount(ctx, "visit:uv:"+key).Result(); err == nil {
		st.UV = n
	}
	if st.PV == 0 && st.DAU == 0 && st.UV == 0 {
		return nil
	}
	row := model.SiteVisitDaily{
		Day: visitDayDate(day),
		PV:  st.PV,
		DAU: st.DAU,
		UV:  st.UV,
	}
	return d.DB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "day"}},
		DoUpdates: clause.AssignmentColumns([]string{"pv", "dau", "uv", "updated_at"}),
	}).Create(&row).Error
}

// ListVisitSeries 近 days 天（含今天）
func (d *Data) ListVisitSeries(ctx context.Context, days int) []DayVisitStat {
	if days < 1 {
		days = 30
	}
	if days > 90 {
		days = 90
	}
	// 固化昨天（幂等）
	_ = d.FlushVisitDay(ctx, time.Now().AddDate(0, 0, -1))

	now := time.Now()
	out := make([]DayVisitStat, 0, days)
	for i := days - 1; i >= 0; i-- {
		day := now.AddDate(0, 0, -i)
		out = append(out, d.GetDayVisitStat(ctx, day))
	}
	return out
}

func visitorThrottleID(userID uint, visitorID, clientIP string) string {
	if userID > 0 {
		return "u:" + strconv.FormatUint(uint64(userID), 10)
	}
	if v := strings.TrimSpace(visitorID); v != "" {
		if len(v) > 64 {
			v = v[:64]
		}
		return "v:" + v
	}
	if clientIP != "" {
		return "ip:" + clientIP
	}
	return "anon"
}

func uvMember(userID uint, visitorID, clientIP string) string {
	if userID > 0 {
		return "u:" + strconv.FormatUint(uint64(userID), 10)
	}
	if v := strings.TrimSpace(visitorID); v != "" {
		if len(v) > 64 {
			v = v[:64]
		}
		return "v:" + v
	}
	if clientIP != "" {
		return "ip:" + clientIP
	}
	return ""
}

func normalizeVisitPath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return "/"
	}
	if i := strings.IndexAny(p, "?#"); i >= 0 {
		p = p[:i]
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if len(p) > 200 {
		p = p[:200]
	}
	// 简单拒绝明显异常
	if strings.Contains(p, "..") {
		return "/"
	}
	return p
}

