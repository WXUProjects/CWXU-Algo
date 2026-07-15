package service

import (
	"context"
	"cwxu-algo/api/core/v1/submit_log"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/utils"
	"cwxu-algo/app/core_data/internal/data"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"
	"fmt"
	"strconv"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"github.com/redis/go-redis/v9"
	grpc2 "google.golang.org/grpc"
	"gorm.io/gorm"
)

type SubmitLogService struct {
	submit_log.UnimplementedSubmitServer
	sbDal *dal.SpiderDal
	db    *gorm.DB
	rdb   *redis.Client
	reg   *registry.Registrar
}

func (s SubmitLogService) userRPC() (*grpc2.ClientConn, error) {
	if s.reg == nil {
		return nil, fmt.Errorf("registry not configured")
	}
	return grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*s.reg).(registry.Discovery)),
		grpc.WithTimeout(20*time.Second),
	)
}

func (s SubmitLogService) GetSubmitLog(ctx context.Context, req *submit_log.GetSubmitLogReq) (*submit_log.GetSubmitLogRes, error) {
	// 多取一些，过滤掉力扣合成记录后仍尽量凑满 limit
	limit := req.Limit
	if limit <= 0 {
		limit = 10
	}
	fetchLimit := limit * 3
	if fetchLimit < 30 {
		fetchLimit = 30
	}
	var memberIDs []int64
	if req.UserId == -1 {
		// 组织聚合动态：仅当前组织成员
		ids, _, _, err := ResolveOrgMemberIDs(ctx, s.reg, 0, false)
		if err != nil {
			log.Warnf("org members for submit feed: %v", err)
			ids = []int64{}
		}
		memberIDs = ids
	}
	d, err := s.sbDal.GetByUserIdScoped(ctx, req.UserId, req.Cursor, fetchLimit, memberIDs)
	if err != nil {
		return nil, errors.InternalServer("内部服务器错误", err.Error())
	}
	r := make([]*submit_log.SubmitLog, 0, limit)
	for _, v := range d {
		// 力扣只参与热力图/统计，不进活动流
		if v.Platform == "LeetCode" {
			continue
		}
		var problemID uint32
		if v.ProblemID != nil {
			problemID = uint32(*v.ProblemID)
		}
		r = append(r, &submit_log.SubmitLog{
			Id:        uint32(v.ID),
			UserId:    v.UserID,
			Platform:  v.Platform,
			SubmitId:  v.SubmitID,
			Contest:   v.Contest,
			Problem:   v.Problem,
			Lang:      v.Lang,
			Status:    v.Status,
			Time:      v.Time.Unix(),
			ProblemId: problemID,
		})
		if int64(len(r)) >= limit {
			break
		}
	}

	// 一次 RPC + 一次 SQL，补齐展示字段，避免前端 N+1
	nameMap := s.fetchUserNames(ctx, r)
	metaMap := s.fetchProblemMeta(ctx, r)
	for _, item := range r {
		if n, ok := nameMap[item.UserId]; ok {
			item.UserName = n
		}
		if item.ProblemId > 0 {
			if m, ok := metaMap[item.ProblemId]; ok {
				item.ProblemTitle = m.Title
				if len(m.Tags) > 0 {
					item.ProblemTags = m.Tags
				}
				item.ProblemDifficulty = m.Difficulty
			}
		}
	}

	return &submit_log.GetSubmitLogRes{
		Data: r,
	}, nil
}

// fetchUserNames 批量获取用户展示名（user 服务 GetByIds）
func (s SubmitLogService) fetchUserNames(ctx context.Context, logs []*submit_log.SubmitLog) map[int64]string {
	result := map[int64]string{}
	if len(logs) == 0 {
		return result
	}
	idSet := map[int64]struct{}{}
	for _, v := range logs {
		if v.UserId != 0 {
			idSet[v.UserId] = struct{}{}
		}
	}
	if len(idSet) == 0 {
		return result
	}
	userIds := make([]int64, 0, len(idSet))
	for id := range idSet {
		userIds = append(userIds, id)
	}

	conn, err := s.userRPC()
	if err != nil {
		log.Errorf("submit_log userRPC: %v", err)
		return result
	}
	defer conn.Close()

	client := profile.NewProfileClient(conn)
	res, err := client.GetByIds(ctx, &profile.GetByIdsReq{UserIds: userIds})
	if err != nil {
		log.Errorf("submit_log GetByIds: %v", err)
		return result
	}
	for _, p := range res.Profiles {
		name := p.Name
		if name == "" {
			name = fmt.Sprintf("用户%d", p.UserId)
		}
		result[p.UserId] = name
	}
	return result
}

type problemMeta struct {
	Title      string
	Tags       []string
	Difficulty string
}

// fetchProblemMeta 批量取题库标题、标签与难度（本库 problems）
func (s SubmitLogService) fetchProblemMeta(ctx context.Context, logs []*submit_log.SubmitLog) map[uint32]problemMeta {
	result := map[uint32]problemMeta{}
	if len(logs) == 0 || s.db == nil {
		return result
	}
	idSet := map[uint32]struct{}{}
	for _, v := range logs {
		if v.ProblemId > 0 {
			idSet[v.ProblemId] = struct{}{}
		}
	}
	if len(idSet) == 0 {
		return result
	}
	ids := make([]uint32, 0, len(idSet))
	for id := range idSet {
		ids = append(ids, id)
	}

	var rows []struct {
		ID         uint              `gorm:"column:id"`
		Title      string            `gorm:"column:title"`
		Tags       model.StringArray `gorm:"column:tags"`
		Difficulty string            `gorm:"column:difficulty"`
	}
	if err := s.db.WithContext(ctx).
		Table("problems").
		Select("id, title, tags, difficulty").
		Where("id IN ?", ids).
		Find(&rows).Error; err != nil {
		log.Errorf("submit_log fetchProblemMeta: %v", err)
		return result
	}
	for _, row := range rows {
		tags := []string(row.Tags)
		if tags == nil {
			tags = []string{}
		}
		// 最多展示 6 个，避免动态列表过宽
		if len(tags) > 6 {
			tags = tags[:6]
		}
		result[uint32(row.ID)] = problemMeta{
			Title:      row.Title,
			Tags:       tags,
			Difficulty: row.Difficulty,
		}
	}
	return result
}

func (s SubmitLogService) LastSubmitTime(ctx context.Context, req *submit_log.LastSubmitTimeReq) (*submit_log.LastSubmitTimeRes, error) {
	var d []model.SubmitLog
	timesMap := make(map[int64]int64)
	pipe := s.rdb.Pipeline()
	keys := make([]string, 0)
	for _, v := range req.UserIds {
		keys = append(keys, fmt.Sprintf("user:%d:lastSubmitTime", v))
	}
	// 到缓存查
	rVal, _ := s.rdb.MGet(ctx, keys...).Result()
	missUser := make([]int64, 0)
	for i, v := range rVal {
		if v == nil {
			missUser = append(missUser, req.UserIds[i])
			continue
		}
		in, ok := v.(string)
		if !ok {
			continue
		}
		val, _ := strconv.ParseInt(in, 10, 64)
		timesMap[req.UserIds[i]] = val
	}
	// 回源
	if len(missUser) > 0 {
		err := s.db.
			Table("submit_logs").
			Select("DISTINCT ON (user_id) user_id, time").
			Where("user_id IN ?", missUser).
			Order("user_id, time DESC").
			Scan(&d).Error
		if err != nil {
			return nil, errors.InternalServer("内部错误", "数据库查询错误")
		}
		for _, v := range d {
			timesMap[v.UserID] = v.Time.Unix()
			// 塞入缓存
			pipe.Set(ctx, fmt.Sprintf("user:%d:lastSubmitTime", v.UserID), v.Time.Unix(), 1*time.Hour)
		}
		if _, err := pipe.Exec(ctx); err != nil {
			log.Errorf("LastSubmitTime: pipeline exec failed: %v", err)
		}
	}
	encoded, err := utils.GobEncoder(timesMap)
	if err != nil {
		return nil, errors.InternalServer("内部错误", "编码错误")
	}
	return &submit_log.LastSubmitTimeRes{TimeMap: encoded}, nil
}

func NewSubmitLogService(sbDal *dal.SpiderDal, data *data.Data, reg *discovery.Register) *SubmitLogService {
	return &SubmitLogService{
		sbDal: sbDal,
		db:    data.DB,
		rdb:   data.RDB,
		reg:   &reg.Reg,
	}
}
