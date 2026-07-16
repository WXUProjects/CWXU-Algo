package service

import (
	"context"
	"fmt"
	"math"
	"time"

	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/log"
)

// 画像缓存：ver 精确失效 + latest 兜底（爬虫后仍可读旧画像，同时 MQ 刷新）
const (
	userProfileCacheSchema = "1"
	userProfileLatestTTL   = 30 * 24 * time.Hour
	userProfileVerTTL      = 7 * 24 * time.Hour
	// 小用户同步算；大用户只入队避免 HTTP 504
	userProfileSyncACMax = 300
)

// UserProfileSnapshot 可 gob 缓存的画像快照
type UserProfileSnapshot struct {
	Radar []struct {
		Tag     string
		Score   float64
		ACCount int64
	}
	Platforms []struct {
		Name  string
		Count int64
	}
	Difficulties []struct {
		Name  string
		Count int64
	}
	TotalAC int64
	BuiltAt int64 // unix sec
}

func userProfileVerKey(userID int64) string {
	return fmt.Sprintf("statistic:user:%d:ver", userID)
}

func userProfileCacheKey(userID int64, ver string) string {
	if ver == "" {
		ver = "0"
	}
	return fmt.Sprintf("problem:user_profile:s%s:u%d:v%s", userProfileCacheSchema, userID, ver)
}

func userProfileLatestKey(userID int64) string {
	return fmt.Sprintf("problem:user_profile:s%s:u%d:latest", userProfileCacheSchema, userID)
}

func (uc *ProblemUseCase) profileVer(ctx context.Context, userID int64) string {
	if uc.data == nil || uc.data.RDB == nil {
		return "0"
	}
	v, err := uc.data.RDB.Get(ctx, userProfileVerKey(userID)).Result()
	if err != nil || v == "" {
		return "0"
	}
	return v
}

func (uc *ProblemUseCase) readProfileCache(ctx context.Context, key string) (*UserProfileSnapshot, bool) {
	if uc.data == nil || uc.data.RDB == nil || key == "" {
		return nil, false
	}
	b, err := uc.data.RDB.Get(ctx, key).Bytes()
	if err != nil || len(b) == 0 {
		return nil, false
	}
	var snap UserProfileSnapshot
	if err := utils.GobDecoder(b, &snap); err != nil {
		return nil, false
	}
	return &snap, true
}

func (uc *ProblemUseCase) writeProfileCache(ctx context.Context, userID int64, snap *UserProfileSnapshot) {
	if uc.data == nil || uc.data.RDB == nil || snap == nil || userID <= 0 {
		return
	}
	if snap.BuiltAt == 0 {
		snap.BuiltAt = time.Now().Unix()
	}
	b, err := utils.GobEncoder(snap)
	if err != nil {
		log.Warnf("user_profile gob encode user=%d: %v", userID, err)
		return
	}
	ver := uc.profileVer(ctx, userID)
	_ = uc.data.RDB.Set(ctx, userProfileCacheKey(userID, ver), b, userProfileVerTTL).Err()
	_ = uc.data.RDB.Set(ctx, userProfileLatestKey(userID), b, userProfileLatestTTL).Err()
}

// EnqueueUserProfileRebuild 异步重建（爬虫成功 / 读 miss / cron）
func (uc *ProblemUseCase) EnqueueUserProfileRebuild(userID int64) {
	if userID <= 0 || uc.profileTask == nil {
		return
	}
	_ = uc.profileTask.Do(userID)
}

// BuildAndCacheUserProfile 同步计算并写缓存（consumer 调用）
func (uc *ProblemUseCase) BuildAndCacheUserProfile(userID int64) error {
	if userID <= 0 {
		return fmt.Errorf("invalid user_id")
	}
	snap, err := uc.computeUserProfile(userID)
	if err != nil {
		return err
	}
	uc.writeProfileCache(context.Background(), userID, snap)
	return nil
}

// UserProfile 读路径：优先缓存；miss 时小用户同步、大用户只入队
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
	ctx := context.Background()
	ver := uc.profileVer(ctx, userID)

	if snap, ok := uc.readProfileCache(ctx, userProfileCacheKey(userID, ver)); ok {
		return unpackProfile(snap)
	}
	// ver 失效后仍可用 latest，同时后台刷新
	if snap, ok := uc.readProfileCache(ctx, userProfileLatestKey(userID)); ok {
		uc.EnqueueUserProfileRebuild(userID)
		return unpackProfile(snap)
	}

	// 冷启动：小用户同步算；大用户入队避免 504
	var acCount int64
	if uc.data != nil && uc.data.DB != nil {
		_ = uc.data.DB.Raw(`SELECT COUNT(*) FROM user_ac_problems WHERE user_id = ?`, userID).Scan(&acCount).Error
	}
	if acCount <= userProfileSyncACMax {
		snap, e := uc.computeUserProfile(userID)
		if e != nil {
			err = e
			return
		}
		uc.writeProfileCache(ctx, userID, snap)
		return unpackProfile(snap)
	}

	uc.EnqueueUserProfileRebuild(userID)
	// 大用户无缓存时返回空壳（total 可先给 count，雷达等 MQ 完成后有）
	totalAC = acCount
	return
}

func unpackProfile(snap *UserProfileSnapshot) (radar []struct {
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
	if snap == nil {
		return
	}
	radar = snap.Radar
	platforms = snap.Platforms
	difficulties = snap.Difficulties
	totalAC = snap.TotalAC
	return
}

// computeUserProfile 重 JOIN 聚合（仅后台 / 小用户同步路径）
func (uc *ProblemUseCase) computeUserProfile(userID int64) (*UserProfileSnapshot, error) {
	snap := &UserProfileSnapshot{BuiltAt: time.Now().Unix()}

	const userACJoinProblems = `
		FROM user_ac_problems u
		JOIN problems p ON (
			u.problem_key = 'p:' || p.id::text
			OR (
				p.external_id IS NOT NULL AND btrim(p.external_id) <> ''
				AND u.problem_key = 'e:' || p.platform || ':' || p.external_id
			)
		)
	`

	type tagRow struct {
		Tag   string
		Count int64
	}
	var tags []tagRow
	if err := uc.data.DB.Raw(`
		SELECT tag, COUNT(DISTINCT p.id) AS count
		`+userACJoinProblems+`
		CROSS JOIN LATERAL jsonb_array_elements_text(COALESCE(p.tags::jsonb, '[]'::jsonb)) AS tag
		WHERE u.user_id = ? AND p.status = ?
		  AND p.tags IS NOT NULL AND p.tags::text NOT IN ('', '[]', 'null')
		GROUP BY tag
		ORDER BY count DESC
		LIMIT 20
	`, userID, model.ProblemStatusCompleted).Scan(&tags).Error; err != nil {
		log.Errorf("radar sql user=%d: %v", userID, err)
	}

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
		snap.Radar = append(snap.Radar, struct {
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
		SELECT COALESCE(NULLIF(btrim(u.platform), ''), p.platform) AS name, COUNT(DISTINCT p.id) AS count
		`+userACJoinProblems+`
		WHERE u.user_id = ?
		GROUP BY 1
	`, userID).Scan(&plats).Error; e != nil {
		log.Errorf("platforms sql user=%d: %v", userID, e)
	}
	for _, p := range plats {
		snap.Platforms = append(snap.Platforms, struct {
			Name  string
			Count int64
		}{p.Name, p.Count})
	}

	var diffs []nc
	if e := uc.data.DB.Raw(`
		SELECT p.difficulty AS name, COUNT(DISTINCT p.id) AS count
		`+userACJoinProblems+`
		WHERE u.user_id = ?
		  AND p.difficulty IS NOT NULL AND BTRIM(p.difficulty) <> ''
		  AND UPPER(BTRIM(p.difficulty)) NOT IN ('UNKNOWN','NULL','NONE')
		GROUP BY p.difficulty
	`, userID).Scan(&diffs).Error; e != nil {
		log.Errorf("difficulties sql user=%d: %v", userID, e)
	}
	for _, d := range diffs {
		snap.Difficulties = append(snap.Difficulties, struct {
			Name  string
			Count int64
		}{d.Name, d.Count})
	}

	_ = uc.data.DB.Raw(`
		SELECT COUNT(*) FROM user_ac_problems WHERE user_id = ?
	`, userID).Scan(&snap.TotalAC).Error

	return snap, nil
}
