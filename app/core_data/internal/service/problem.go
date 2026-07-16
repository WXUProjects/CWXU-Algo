package service

import (
	"context"
	"cwxu-algo/api/core/v1/problem"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/permission"
	"cwxu-algo/app/common/utils/auth"
	biz "cwxu-algo/app/core_data/internal/biz/service"
	"cwxu-algo/app/core_data/internal/data/model"
	"strings"
	"time"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	grpc2 "google.golang.org/grpc"
)

type ProblemService struct {
	problem.UnimplementedProblemServer
	uc  *biz.ProblemUseCase
	reg *registry.Registrar
}

func NewProblemService(uc *biz.ProblemUseCase, reg *discovery.Register) *ProblemService {
	return &ProblemService{uc: uc, reg: &reg.Reg}
}

func (s *ProblemService) userRPC() (*grpc2.ClientConn, error) {
	if s.reg == nil {
		return nil, errors.InternalServer("no registry", "registry nil")
	}
	return grpc.DialInsecure(
		context.Background(),
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery((*s.reg).(registry.Discovery)),
		grpc.WithTimeout(10*time.Second),
	)
}

func (s *ProblemService) fetchUserNames(ctx context.Context, userIDs []int64) map[int64]string {
	out := map[int64]string{}
	if len(userIDs) == 0 {
		return out
	}
	conn, err := s.userRPC()
	if err != nil {
		log.Errorf("problem userRPC: %v", err)
		return out
	}
	defer conn.Close()
	var orgID int64
	if pd := auth.GetCurrentUser(ctx); pd != nil {
		orgID = int64(pd.OrgID)
	}
	client := profile.NewProfileClient(conn)
	res, err := client.GetByIds(ctx, &profile.GetByIdsReq{UserIds: userIDs, OrgId: orgID})
	if err != nil {
		log.Errorf("problem GetByIds: %v", err)
		return out
	}
	for _, p := range res.Profiles {
		out[p.UserId] = p.Name
	}
	return out
}

// cleanDisplayTitle 列表/详情展示用：去掉 AtCoder 页头夹带的 Editorial 与空白行
func cleanDisplayTitle(title string) string {
	title = strings.ReplaceAll(title, "\r", "\n")
	for _, line := range strings.Split(title, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "Editorial") || strings.EqualFold(line, "解説") {
			continue
		}
		for _, suf := range []string{"Editorial", "解説"} {
			if i := strings.LastIndex(line, suf); i > 0 {
				line = strings.TrimSpace(line[:i])
			}
		}
		line = strings.Join(strings.Fields(line), " ")
		if line != "" {
			return line
		}
	}
	return strings.Join(strings.Fields(title), " ")
}

func (s *ProblemService) toInfo(p *model.Problem, userStatus string) *problem.ProblemInfo {
	if p == nil {
		return nil
	}
	tags := []string(p.Tags)
	if tags == nil {
		tags = []string{}
	}
	sols := make([]*problem.SolutionMeta, 0, len(p.SolutionsMeta))
	for _, sol := range p.SolutionsMeta {
		sols = append(sols, &problem.SolutionMeta{
			Name:             sol.Name,
			TimeComplexity:   sol.TimeComplexity,
			SpaceComplexity:  sol.SpaceComplexity,
			BriefExplanation: sol.BriefExplanation,
		})
	}
	var last int64
	if p.LastSubmittedAt != nil {
		last = p.LastSubmittedAt.Unix()
	}
	return &problem.ProblemInfo{
		Id:              uint32(p.ID),
		Platform:        p.Platform,
		ExternalId:      p.ExternalID,
		Title:           cleanDisplayTitle(p.Title),
		Url:             p.URL,
		ContentMd:       p.ContentMD,
		ProblemType:     p.ProblemType,
		Tags:            tags,
		Solutions:       sols,
		Difficulty:      p.Difficulty,
		Status:          p.Status,
		ErrorMsg:        p.ErrorMsg,
		LastSubmittedAt: last,
		UserStatus:      userStatus,
	}
}

func splitCSV(s string) []string {
	if strings.TrimSpace(s) == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func (s *ProblemService) ListTags(ctx context.Context, req *problem.ListTagsReq) (*problem.ListTagsRes, error) {
	rows, err := s.uc.ListTags(int(req.Limit))
	if err != nil {
		return nil, errors.InternalServer("list tags failed", "service unavailable")
	}
	data := make([]*problem.TagCount, 0, len(rows))
	for _, r := range rows {
		data = append(data, &problem.TagCount{Tag: r.Tag, Count: r.Count})
	}
	return &problem.ListTagsRes{Code: 0, Message: "success", Data: data}, nil
}

func (s *ProblemService) List(ctx context.Context, req *problem.ListProblemReq) (*problem.ListProblemRes, error) {
	list, statusMap, total, err := s.uc.List(biz.ListProblemFilter{
		Page:       req.Page,
		PageSize:   req.PageSize,
		Sort:       req.Sort,
		Platforms:  splitCSV(req.Platforms),
		Tags:       splitCSV(req.Tags),
		UserStatus: req.UserStatus,
		UserID:     req.UserId,
		Keyword:    req.Keyword,
		Difficulty: req.Difficulty,
	})
	if err != nil {
		return nil, errors.InternalServer("list failed", "service unavailable")
	}
	data := make([]*problem.ProblemInfo, 0, len(list))
	for i := range list {
		us := statusMap[list[i].ID]
		if us == "" {
			us = "NONE"
		}
		// list 不返回完整 content
		info := s.toInfo(&list[i], us)
		info.ContentMd = ""
		data = append(data, info)
	}
	page := req.Page
	if page <= 0 {
		page = 1
	}
	ps := req.PageSize
	if ps <= 0 {
		ps = 20
	}
	return &problem.ListProblemRes{
		Code:     0,
		Message:  "success",
		Data:     data,
		Total:    total,
		Page:     page,
		PageSize: ps,
	}, nil
}

func (s *ProblemService) Get(ctx context.Context, req *problem.GetProblemReq) (*problem.GetProblemRes, error) {
	p, err := s.uc.Get(uint(req.Id))
	if err != nil {
		return &problem.GetProblemRes{Code: 1, Message: "题目不存在"}, nil
	}
	return &problem.GetProblemRes{
		Code:    0,
		Message: "success",
		Data:    s.toInfo(p, ""),
	}, nil
}

func (s *ProblemService) ListSubmissions(ctx context.Context, req *problem.ListSubmissionsReq) (*problem.ListSubmissionsRes, error) {
	list, total, err := s.uc.ListSubmissions(uint(req.ProblemId), req.UserId, req.Page, req.PageSize)
	if err != nil {
		return nil, errors.InternalServer("query failed", "service unavailable")
	}
	ids := make([]int64, 0, len(list))
	seen := map[int64]bool{}
	for _, v := range list {
		if v.UserID != 0 && !seen[v.UserID] {
			seen[v.UserID] = true
			ids = append(ids, v.UserID)
		}
	}
	names := s.fetchUserNames(ctx, ids)
	data := make([]*problem.SubmissionInfo, 0, len(list))
	for _, v := range list {
		name := names[v.UserID]
		if name == "" {
			name = ""
		}
		data = append(data, &problem.SubmissionInfo{
			Id:       uint32(v.ID),
			UserId:   v.UserID,
			Platform: v.Platform,
			SubmitId: v.SubmitID,
			Lang:     v.Lang,
			Status:   v.Status,
			Time:     v.Time.Unix(),
			Contest:  v.Contest,
			UserName: name,
		})
	}
	return &problem.ListSubmissionsRes{
		Code:    0,
		Message: "success",
		Data:    data,
		Total:   total,
	}, nil
}

func (s *ProblemService) UserProfile(ctx context.Context, req *problem.UserProfileReq) (*problem.UserProfileRes, error) {
	if req.UserId <= 0 {
		return &problem.UserProfileRes{Code: 1, Message: "user_id 无效"}, nil
	}
	radar, plats, diffs, totalAC, err := s.uc.UserProfile(req.UserId)
	if err != nil {
		return nil, errors.InternalServer("profile failed", "service unavailable")
	}
	r := make([]*problem.TagScore, 0, len(radar))
	for _, v := range radar {
		r = append(r, &problem.TagScore{Tag: v.Tag, Score: v.Score, AcCount: v.ACCount})
	}
	p := make([]*problem.NamedCount, 0, len(plats))
	for _, v := range plats {
		p = append(p, &problem.NamedCount{Name: v.Name, Count: v.Count})
	}
	d := make([]*problem.NamedCount, 0, len(diffs))
	for _, v := range diffs {
		d = append(d, &problem.NamedCount{Name: v.Name, Count: v.Count})
	}
	return &problem.UserProfileRes{
		Code:         0,
		Message:      "success",
		Radar:        r,
		Platforms:    p,
		Difficulties: d,
		TotalAc:      totalAC,
	}, nil
}

func toFailedProto(list []model.Problem) []*problem.FailedProblem {
	ff := make([]*problem.FailedProblem, 0, len(list))
	for _, f := range list {
		title := cleanDisplayTitle(f.Title)
		if title == "" {
			title = f.ExternalID
		}
		if title == "" {
			title = f.Status
		}
		ff = append(ff, &problem.FailedProblem{
			Id:            uint32(f.ID),
			Platform:      f.Platform,
			ExternalId:    f.ExternalID,
			Title:         title,
			ErrorMsg:      firstNonEmptyTitle(f.ErrorMsg, f.Status),
			UpdatedAt:     f.UpdatedAt.Unix(),
			Status:        f.Status,
			FetchAttempts: int32(f.FetchAttempts),
		})
	}
	return ff
}

func firstNonEmptyTitle(a, b string) string {
	if strings.TrimSpace(a) != "" {
		return a
	}
	return b
}

func (s *ProblemService) Progress(ctx context.Context, req *problem.ProgressReq) (*problem.ProgressRes, error) {
	// 管理端可查看进度；运维写操作仍仅管理员
	if !auth.VerifyStaff(ctx) {
		return &problem.ProgressRes{Code: 1, Message: "权限不足"}, nil
	}
	snap, err := s.uc.Progress()
	if err != nil {
		// 不因 MQ 等附属信息失败而整页不可用
		log.Errorf("problem progress: %v", err)
		snap = biz.ProgressSnapshot{}
	}
	pi := make([]*problem.ProgressItem, 0, len(snap.Items))
	for _, v := range snap.Items {
		pi = append(pi, &problem.ProgressItem{Status: v.Status, Count: v.Count})
	}
	jobs := make([]*problem.ActiveJob, 0, len(snap.ActiveJobs))
	for _, j := range snap.ActiveJobs {
		t := cleanDisplayTitle(j.Title)
		if t == "" {
			t = j.ExternalID
		}
		jobs = append(jobs, &problem.ActiveJob{
			ProblemId:  uint32(j.ProblemID),
			Platform:   j.Platform,
			ExternalId: j.ExternalID,
			Title:      t,
			Stage:      j.Stage,
			StartedAt:  j.StartedAt.Unix(),
		})
	}
	qs := make([]*problem.QueueStatus, 0, len(snap.Queues))
	for _, q := range snap.Queues {
		qs = append(qs, &problem.QueueStatus{
			Name:        q.Name,
			Messages:    q.Messages,
			Consumers:   q.Consumers,
			Concurrency: q.Concurrency,
		})
	}
	return &problem.ProgressRes{
		Code:             0,
		Message:          "success",
		Items:            pi,
		RecentFailed:     toFailedProto(snap.Failed),
		Total:            snap.Total,
		Paused:           snap.Paused,
		ActiveJobs:       jobs,
		Queues:           qs,
		InProgress:       toFailedProto(snap.InProgress),
		FetchPaused:      snap.FetchPaused,
		AnalyzePaused:    snap.AnalyzePaused,
		RecentFailedPerm: toFailedProto(snap.FailedPerm),
	}, nil
}

func (s *ProblemService) Backfill(ctx context.Context, req *problem.BackfillReq) (*problem.BackfillRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleAdmin) {
		return &problem.BackfillRes{Code: 1, Message: "仅管理员可触发回填"}, nil
	}
	// 回填不清空队列、不强制恢复 pause；仅补近 6 月提交入队
	scanned, bound, created, enqueued, enqFetch, enqAnalyze, err := s.uc.Backfill(int(req.Limit))
	if err != nil {
		return nil, errors.InternalServer("backfill failed", "service unavailable")
	}
	return &problem.BackfillRes{
		Code:            0,
		Message:         "success",
		Scanned:         scanned,
		Bound:           bound,
		Created:         created,
		Enqueued:        enqueued,
		EnqueuedFetch:   enqFetch,
		EnqueuedAnalyze: enqAnalyze,
	}, nil
}

func (s *ProblemService) ResetQueues(ctx context.Context, req *problem.ResetQueuesReq) (*problem.ResetQueuesRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleAdmin) {
		return &problem.ResetQueuesRes{Code: 1, Message: "仅管理员可操作"}, nil
	}
	pf, pa, ef, ea, err := s.uc.ResetQueues()
	if err != nil {
		return nil, errors.InternalServer("reset queues failed", "service unavailable")
	}
	return &problem.ResetQueuesRes{
		Code:            0,
		Message:         "已清空 MQ 并按 DB 待爬取/待分析重灌",
		PurgedFetch:     int64(pf),
		PurgedAnalyze:   int64(pa),
		EnqueuedFetch:   int64(ef),
		EnqueuedAnalyze: int64(ea),
	}, nil
}

func (s *ProblemService) EmergencyStop(ctx context.Context, req *problem.EmergencyStopReq) (*problem.EmergencyStopRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleAdmin) {
		return &problem.EmergencyStopRes{Code: 1, Message: "仅管理员可操作"}, nil
	}
	pf, pa, err := s.uc.EmergencyStop()
	if err != nil {
		return nil, errors.InternalServer("emergency stop failed", "service unavailable")
	}
	return &problem.EmergencyStopRes{
		Code:          0,
		Message:       "已暂停 AI 分析（队列保留；清队列请用重置队列）",
		PurgedFetch:   int64(pf),
		PurgedAnalyze: int64(pa),
	}, nil
}

func (s *ProblemService) ResetAll(ctx context.Context, req *problem.ResetAllReq) (*problem.ResetAllRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleAdmin) {
		return &problem.ResetAllRes{Code: 1, Message: "仅管理员可操作"}, nil
	}
	requeue := true
	if req != nil && req.RequeueSet {
		requeue = req.Requeue
	}
	reset, enqueued, pf, pa, err := s.uc.ResetAll(requeue)
	if err != nil {
		return nil, errors.InternalServer("reset failed", "service unavailable")
	}
	return &problem.ResetAllRes{
		Code:          0,
		Message:       "已重置 AI 分析（题面保留）",
		Reset_:        int64(reset),
		Enqueued:      int64(enqueued),
		PurgedFetch:   int64(pf),
		PurgedAnalyze: int64(pa),
	}, nil
}

func (s *ProblemService) Resume(ctx context.Context, req *problem.ResumeReq) (*problem.ResumeRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleAdmin) {
		return &problem.ResumeRes{Code: 1, Message: "仅管理员可操作"}, nil
	}
	s.uc.Resume()
	return &problem.ResumeRes{Code: 0, Message: "AI 分析已恢复"}, nil
}

func (s *ProblemService) RetryFailed(ctx context.Context, req *problem.RetryFailedReq) (*problem.RetryFailedRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleAdmin) {
		return &problem.RetryFailedRes{Code: 1, Message: "仅管理员可操作"}, nil
	}
	if s.uc != nil {
		s.uc.ResumeAnalyze()
		s.uc.ResumeFetch()
	}
	limit := 0
	if req != nil {
		limit = int(req.Limit)
	}
	scanned, enqueued, blacklisted, err := s.uc.RetryFailed(limit)
	if err != nil {
		return nil, errors.InternalServer("retry failed", "service unavailable")
	}
	return &problem.RetryFailedRes{
		Code:        0,
		Message:     "success",
		Scanned:     scanned,
		Enqueued:    enqueued,
		Blacklisted: blacklisted,
	}, nil
}

func (s *ProblemService) ToggleAnalyze(ctx context.Context, req *problem.TogglePipelineReq) (*problem.TogglePipelineRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleAdmin) {
		return &problem.TogglePipelineRes{Code: 1, Message: "仅管理员可操作"}, nil
	}
	pause := true
	if req != nil && req.PauseSet {
		pause = req.Pause
	} else {
		// 翻转
		pause = !s.uc.ProgressPausedAnalyze()
	}
	if pause {
		n, err := s.uc.PauseAnalyze()
		if err != nil {
			return nil, errors.InternalServer("pause analyze failed", "service unavailable")
		}
		return &problem.TogglePipelineRes{Code: 0, Message: "AI 分析已暂停（队列保留）", Paused: true, Purged: int64(n)}, nil
	}
	s.uc.ResumeAnalyze()
	return &problem.TogglePipelineRes{Code: 0, Message: "AI 分析已恢复", Paused: false}, nil
}

func (s *ProblemService) ToggleFetch(ctx context.Context, req *problem.TogglePipelineReq) (*problem.TogglePipelineRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleAdmin) {
		return &problem.TogglePipelineRes{Code: 1, Message: "仅管理员可操作"}, nil
	}
	pause := true
	if req != nil && req.PauseSet {
		pause = req.Pause
	} else {
		pause = !s.uc.ProgressPausedFetch()
	}
	if pause {
		n, err := s.uc.PauseFetch()
		if err != nil {
			return nil, errors.InternalServer("pause fetch failed", "service unavailable")
		}
		return &problem.TogglePipelineRes{Code: 0, Message: "题面爬取已暂停（队列保留）", Paused: true, Purged: int64(n)}, nil
	}
	s.uc.ResumeFetch()
	return &problem.TogglePipelineRes{Code: 0, Message: "题面爬取已恢复", Paused: false}, nil
}
