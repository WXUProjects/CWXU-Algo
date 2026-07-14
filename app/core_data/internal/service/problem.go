package service

import (
	"context"
	"cwxu-algo/api/core/v1/problem"
	"cwxu-algo/app/common/permission"
	"cwxu-algo/app/common/utils/auth"
	biz "cwxu-algo/app/core_data/internal/biz/service"
	"cwxu-algo/app/core_data/internal/data/model"
	"strings"

	"github.com/go-kratos/kratos/v2/errors"
)

type ProblemService struct {
	problem.UnimplementedProblemServer
	uc *biz.ProblemUseCase
}

func NewProblemService(uc *biz.ProblemUseCase) *ProblemService {
	return &ProblemService{uc: uc}
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
		Title:           p.Title,
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

func (s *ProblemService) List(ctx context.Context, req *problem.ListProblemReq) (*problem.ListProblemRes, error) {
	list, statusMap, total, err := s.uc.List(biz.ListProblemFilter{
		Page:       req.Page,
		PageSize:   req.PageSize,
		Sort:       req.Sort,
		Platforms:  splitCSV(req.Platforms),
		Tags:       splitCSV(req.Tags),
		UserStatus: req.UserStatus,
		UserID:     req.UserId,
	})
	if err != nil {
		return nil, errors.InternalServer("list failed", err.Error())
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
		return nil, errors.InternalServer("query failed", err.Error())
	}
	data := make([]*problem.SubmissionInfo, 0, len(list))
	for _, v := range list {
		data = append(data, &problem.SubmissionInfo{
			Id:       uint32(v.ID),
			UserId:   v.UserID,
			Platform: v.Platform,
			SubmitId: v.SubmitID,
			Lang:     v.Lang,
			Status:   v.Status,
			Time:     v.Time.Unix(),
			Contest:  v.Contest,
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
		return nil, errors.InternalServer("profile failed", err.Error())
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
		Code:          0,
		Message:       "success",
		Radar:         r,
		Platforms:     p,
		Difficulties:  d,
		TotalAc:       totalAC,
	}, nil
}

func (s *ProblemService) Progress(ctx context.Context, req *problem.ProgressReq) (*problem.ProgressRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleCoach) {
		return &problem.ProgressRes{Code: 1, Message: "权限不足"}, nil
	}
	items, failed, total, err := s.uc.Progress()
	if err != nil {
		return nil, errors.InternalServer("progress failed", err.Error())
	}
	pi := make([]*problem.ProgressItem, 0, len(items))
	for _, v := range items {
		pi = append(pi, &problem.ProgressItem{Status: v.Status, Count: v.Count})
	}
	ff := make([]*problem.FailedProblem, 0, len(failed))
	for _, f := range failed {
		ff = append(ff, &problem.FailedProblem{
			Id:         uint32(f.ID),
			Platform:   f.Platform,
			ExternalId: f.ExternalID,
			Title:      f.Title,
			ErrorMsg:   f.ErrorMsg,
			UpdatedAt:  f.UpdatedAt.Unix(),
		})
	}
	return &problem.ProgressRes{
		Code:          0,
		Message:       "success",
		Items:         pi,
		RecentFailed:  ff,
		Total:         total,
	}, nil
}

func (s *ProblemService) Backfill(ctx context.Context, req *problem.BackfillReq) (*problem.BackfillRes, error) {
	if !auth.VerifyMinRole(ctx, permission.RoleAdmin) {
		return &problem.BackfillRes{Code: 1, Message: "仅管理员可触发回填"}, nil
	}
	scanned, bound, created, enqueued, err := s.uc.Backfill(int(req.Limit))
	if err != nil {
		return nil, errors.InternalServer("backfill failed", err.Error())
	}
	return &problem.BackfillRes{
		Code:     0,
		Message:  "success",
		Scanned:  scanned,
		Bound:    bound,
		Created:  created,
		Enqueued: enqueued,
	}, nil
}
