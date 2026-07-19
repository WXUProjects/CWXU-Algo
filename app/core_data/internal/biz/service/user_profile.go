package service

import (
	"context"
	"fmt"
	"math"
	"time"

	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/core_data/internal/data/dal"

	"github.com/go-kratos/kratos/v2/log"
	"golang.org/x/sync/singleflight"
)

// 画像缓存：ver 精确失效 + latest 兜底（爬虫后仍可读旧画像，同时 MQ 刷新）
const (
	// s5：平台过题三段查询（牛客失败不拖垮其它平台）+ 安全 JOIN
	userProfileCacheSchema = "5"
	userProfileLatestTTL   = 30 * 24 * time.Hour
	userProfileVerTTL      = 7 * 24 * time.Hour
)

// profileBuildSF 同一用户并发请求只算一次（HTTP 即时处理 + MQ consumer 共用）
var profileBuildSF singleflight.Group

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

// BuildAndCacheUserProfile 同步计算并写缓存（HTTP 即时 / MQ consumer）
// 同一 user 并发只跑一遍（singleflight）。
func (uc *ProblemUseCase) BuildAndCacheUserProfile(userID int64) error {
	if userID <= 0 {
		return fmt.Errorf("invalid user_id")
	}
	_, err, _ := profileBuildSF.Do(fmt.Sprintf("up:%d", userID), func() (interface{}, error) {
		snap, e := uc.computeUserProfile(userID)
		if e != nil {
			return nil, e
		}
		uc.writeProfileCache(context.Background(), userID, snap)
		return snap, nil
	})
	return err
}

// buildUserProfileNow 即时计算；成功返回快照，失败返回 error
func (uc *ProblemUseCase) buildUserProfileNow(userID int64) (*UserProfileSnapshot, error) {
	if userID <= 0 {
		return nil, fmt.Errorf("invalid user_id")
	}
	v, err, _ := profileBuildSF.Do(fmt.Sprintf("up:%d", userID), func() (interface{}, error) {
		snap, e := uc.computeUserProfile(userID)
		if e != nil {
			return nil, e
		}
		uc.writeProfileCache(context.Background(), userID, snap)
		return snap, nil
	})
	if err != nil {
		return nil, err
	}
	snap, _ := v.(*UserProfileSnapshot)
	return snap, nil
}

// UserProfile 读路径：缓存优先；从未处理过则本次请求立即计算并落缓存
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
	// ver 失效：先返回 latest，并立即后台重算（不挡本次响应）
	if snap, ok := uc.readProfileCache(ctx, userProfileLatestKey(userID)); ok {
		go func(uid int64) {
			if e := uc.BuildAndCacheUserProfile(uid); e != nil {
				log.Warnf("user_profile refresh user=%d: %v", uid, e)
				// 失败再入队，由 MQ 重试
				uc.EnqueueUserProfileRebuild(uid)
			} else if uc.profileTask != nil {
				uc.profileTask.ClearPending(uid)
			}
		}(userID)
		return unpackProfile(snap)
	}

	// 从未处理：有人请求就立刻算完再返回（singleflight 防打穿）
	start := time.Now()
	snap, e := uc.buildUserProfileNow(userID)
	if e != nil {
		log.Errorf("user_profile on-demand user=%d: %v", userID, e)
		// 同步失败则入队，下次可命中
		uc.EnqueueUserProfileRebuild(userID)
		err = e
		return
	}
	if uc.profileTask != nil {
		uc.profileTask.ClearPending(userID)
	}
	log.Infof("user_profile on-demand user=%d cost=%s", userID, time.Since(start).Round(time.Millisecond))
	return unpackProfile(snap)
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

// computeUserProfile 雷达读 user_tag_ac；平台过题力扣走官方 acTotal；难度仍 JOIN 题库
func (uc *ProblemUseCase) computeUserProfile(userID int64) (*UserProfileSnapshot, error) {
	snap := &UserProfileSnapshot{BuiltAt: time.Now().Unix()}

	// 雷达：写时预聚合 user_tag_ac
	if tags, err := dal.ListUserTagAC(context.Background(), uc.data.DB, userID, 20); err != nil {
		log.Errorf("radar preagg user=%d: %v", userID, err)
	} else {
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
	}

	// 平台过题：读 user_ac_problems；力扣优先官方 acTotal 合成键（e:LeetCode:ac-*）
	// 牛客统一为 NowCoder（不拆竞赛站 / Tracker）
	if plats, e := dal.ListUserPlatformAC(uc.data.DB, userID); e != nil {
		log.Errorf("platforms sql user=%d: %v", userID, e)
	} else {
		for _, p := range plats {
			snap.Platforms = append(snap.Platforms, struct {
				Name  string
				Count int64
			}{p.Name, p.Count})
		}
	}

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

	type nc struct {
		Name  string
		Count int64
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

	// 生涯 total 与 period.ac.total / 平台合计一致：力扣用官方合成键
	if n, e := dal.CountUserLifetimeAC(uc.data.DB, userID); e != nil {
		log.Errorf("totalAC sql user=%d: %v", userID, e)
	} else {
		snap.TotalAC = n
	}

	return snap, nil
}
