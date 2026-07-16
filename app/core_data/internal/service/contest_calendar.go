package service

import (
	"context"
	"strings"
	"time"

	"cwxu-algo/api/core/v1/contest_calendar"
	"cwxu-algo/api/user/v1/profile"
	"cwxu-algo/app/common/discovery"
	"cwxu-algo/app/common/utils/auth"
	"cwxu-algo/app/core_data/internal/data/dal"
	"cwxu-algo/app/core_data/internal/data/model"

	"github.com/go-kratos/kratos/v2/errors"
	"github.com/go-kratos/kratos/v2/log"
	"github.com/go-kratos/kratos/v2/registry"
	"github.com/go-kratos/kratos/v2/transport/grpc"
	"gorm.io/gorm"
)

type ContestCalendarService struct {
	contest_calendar.UnimplementedContestCalendarServer
	dal *dal.ContestCalendarDal
	reg *discovery.Register
}

func NewContestCalendarService(calDal *dal.ContestCalendarDal, reg *discovery.Register) *ContestCalendarService {
	return &ContestCalendarService{dal: calDal, reg: reg}
}

func (s *ContestCalendarService) toItem(m *model.ContestCalendar, subscribed bool) *contest_calendar.CalendarItem {
	return &contest_calendar.CalendarItem{
		Id:           uint32(m.ID),
		Platform:     m.Platform,
		PlatformName: m.PlatformName,
		ExternalId:   m.ExternalID,
		Name:         m.Name,
		Url:          m.URL,
		StartTime:    m.StartTime,
		EndTime:      m.EndTime,
		Source:       m.Source,
		IconUrl:      m.IconURL,
		Subscribed:   subscribed,
	}
}

func (s *ContestCalendarService) ListCalendar(ctx context.Context, req *contest_calendar.ListCalendarReq) (*contest_calendar.ListCalendarRes, error) {
	status := strings.TrimSpace(req.GetStatus())
	if status == "" {
		status = "upcoming"
	}
	list, total, err := s.dal.List(dal.CalendarListQuery{
		Platform: req.GetPlatform(),
		Keyword:  req.GetKeyword(),
		Status:   status,
		Limit:    int(req.GetLimit()),
		Offset:   int(req.GetOffset()),
	})
	if err != nil {
		log.Errorf("ListCalendar: %v", err)
		return nil, errors.InternalServer("内部服务器错误", "服务暂时不可用")
	}

	// 登录用户：标记是否已订阅（平台或单场）
	subPlatforms := map[string]bool{}
	subContests := map[uint]bool{}
	if user := auth.GetCurrentUser(ctx); user != nil {
		if subs, err := s.dal.ListSubsByUser(int64(user.UserID)); err == nil {
			for _, sub := range subs {
				if !sub.Enabled {
					continue
				}
				if sub.Scope == model.CalScopePlatform {
					subPlatforms[sub.Platform] = true
				} else if sub.Scope == model.CalScopeContest {
					subContests[sub.CalendarID] = true
				}
			}
		}
	}

	items := make([]*contest_calendar.CalendarItem, 0, len(list))
	for i := range list {
		m := &list[i]
		subbed := subContests[m.ID] || subPlatforms[m.Platform]
		items = append(items, s.toItem(m, subbed))
	}
	return &contest_calendar.ListCalendarRes{
		Code:    0,
		Message: "OK",
		Data:    items,
		Total:   total,
	}, nil
}

func (s *ContestCalendarService) ListPlatforms(ctx context.Context, req *contest_calendar.ListPlatformsReq) (*contest_calendar.ListPlatformsRes, error) {
	_ = ctx
	_ = req
	// 固定统计即将开始的场次（前端平台 chips 用）
	stats, err := s.dal.ListPlatforms(true)
	if err != nil {
		log.Errorf("ListPlatforms: %v", err)
		return nil, errors.InternalServer("内部服务器错误", "服务暂时不可用")
	}
	items := make([]*contest_calendar.PlatformItem, 0, len(stats))
	for _, st := range stats {
		items = append(items, &contest_calendar.PlatformItem{
			Platform:     st.Platform,
			PlatformName: st.PlatformName,
			IconUrl:      st.IconURL,
			Count:        st.Count,
		})
	}
	return &contest_calendar.ListPlatformsRes{Code: 0, Message: "OK", Data: items}, nil
}

func (s *ContestCalendarService) subToProto(sub *model.ContestCalendarSub) *contest_calendar.SubItem {
	item := &contest_calendar.SubItem{
		Id:             uint32(sub.ID),
		Scope:          sub.Scope,
		Platform:       sub.Platform,
		CalendarId:     uint32(sub.CalendarID),
		AdvanceMinutes: int32(sub.AdvanceMinutes),
		Enabled:        sub.Enabled,
	}
	if sub.Scope == model.CalScopeContest && sub.CalendarID > 0 {
		if cal, err := s.dal.GetByID(sub.CalendarID); err == nil && cal != nil {
			item.ContestName = cal.Name
			item.ContestUrl = cal.URL
			item.StartTime = cal.StartTime
		}
	}
	return item
}

func (s *ContestCalendarService) GetMySubs(ctx context.Context, _ *contest_calendar.GetMySubsReq) (*contest_calendar.GetMySubsRes, error) {
	user := auth.GetCurrentUser(ctx)
	if user == nil {
		return &contest_calendar.GetMySubsRes{Code: 1, Message: "未登录"}, nil
	}
	subs, err := s.dal.ListSubsByUser(int64(user.UserID))
	if err != nil {
		return nil, errors.InternalServer("内部服务器错误", "服务暂时不可用")
	}
	items := make([]*contest_calendar.SubItem, 0, len(subs))
	for i := range subs {
		items = append(items, s.subToProto(&subs[i]))
	}
	return &contest_calendar.GetMySubsRes{Code: 0, Message: "OK", Data: items}, nil
}

func (s *ContestCalendarService) UpsertSub(ctx context.Context, req *contest_calendar.UpsertSubReq) (*contest_calendar.UpsertSubRes, error) {
	user := auth.GetCurrentUser(ctx)
	if user == nil {
		return &contest_calendar.UpsertSubRes{Code: 1, Message: "未登录"}, nil
	}
	scope := strings.ToLower(strings.TrimSpace(req.GetScope()))
	if scope != model.CalScopePlatform && scope != model.CalScopeContest {
		return &contest_calendar.UpsertSubRes{Code: 2, Message: "scope 必须是 platform 或 contest"}, nil
	}
	adv := int(req.GetAdvanceMinutes())
	if adv <= 0 {
		adv = 1440
	}
	if !model.ValidCalendarAdvance(adv) {
		return &contest_calendar.UpsertSubRes{Code: 3, Message: "提前时间不在允许范围内"}, nil
	}

	// 校验邮箱
	email, err := s.lookupUserEmail(ctx, int64(user.UserID))
	if err != nil {
		log.Warnf("lookup email: %v", err)
	}
	if strings.TrimSpace(email) == "" {
		return &contest_calendar.UpsertSubRes{Code: 4, Message: "请先在个人资料中绑定邮箱后再订阅"}, nil
	}

	sub := &model.ContestCalendarSub{
		UserID:         int64(user.UserID),
		Scope:          scope,
		AdvanceMinutes: adv,
		Enabled:        req.GetEnabled(),
	}
	// proto3 bool 默认 false：若客户端想 enabled=true 会传 true；
	// 新建默认应开启。约定：未传 enabled 时当 true 无法区分。
	// 前端总是显式传 enabled。若 false 则为关闭订阅。
	// 首次创建时如果 enabled=false 也允许（等于预留关闭）。

	if scope == model.CalScopePlatform {
		plat := strings.ToLower(strings.TrimSpace(req.GetPlatform()))
		if plat == "" {
			return &contest_calendar.UpsertSubRes{Code: 5, Message: "platform 不能为空"}, nil
		}
		sub.Platform = plat
		sub.CalendarID = 0
	} else {
		cid := uint(req.GetCalendarId())
		if cid == 0 {
			return &contest_calendar.UpsertSubRes{Code: 6, Message: "calendarId 不能为空"}, nil
		}
		cal, err := s.dal.GetByID(cid)
		if err != nil {
			if err == gorm.ErrRecordNotFound {
				return &contest_calendar.UpsertSubRes{Code: 7, Message: "比赛不存在"}, nil
			}
			return nil, errors.InternalServer("内部服务器错误", "服务暂时不可用")
		}
		if cal.StartTime <= time.Now().Unix() {
			return &contest_calendar.UpsertSubRes{Code: 8, Message: "比赛已开始或已结束，无法订阅"}, nil
		}
		sub.CalendarID = cid
		sub.Platform = cal.Platform
	}

	if err := s.dal.UpsertSub(sub); err != nil {
		log.Errorf("UpsertSub: %v", err)
		return nil, errors.InternalServer("保存失败", "服务暂时不可用")
	}
	// 重新加载以拿 ID
	subs, _ := s.dal.ListSubsByUser(int64(user.UserID))
	var saved *model.ContestCalendarSub
	for i := range subs {
		if subs[i].Scope == sub.Scope && subs[i].Platform == sub.Platform && subs[i].CalendarID == sub.CalendarID {
			saved = &subs[i]
			break
		}
	}
	if saved == nil {
		saved = sub
	}
	return &contest_calendar.UpsertSubRes{
		Code:    0,
		Message: "OK",
		Data:    s.subToProto(saved),
	}, nil
}

func (s *ContestCalendarService) DeleteSub(ctx context.Context, req *contest_calendar.DeleteSubReq) (*contest_calendar.DeleteSubRes, error) {
	user := auth.GetCurrentUser(ctx)
	if user == nil {
		return &contest_calendar.DeleteSubRes{Code: 1, Message: "未登录"}, nil
	}
	scope := strings.ToLower(strings.TrimSpace(req.GetScope()))
	if scope != model.CalScopePlatform && scope != model.CalScopeContest {
		return &contest_calendar.DeleteSubRes{Code: 2, Message: "scope 无效"}, nil
	}
	plat := strings.ToLower(strings.TrimSpace(req.GetPlatform()))
	if err := s.dal.DeleteSub(int64(user.UserID), scope, plat, uint(req.GetCalendarId())); err != nil {
		return nil, errors.InternalServer("删除失败", "服务暂时不可用")
	}
	return &contest_calendar.DeleteSubRes{Code: 0, Message: "OK"}, nil
}

func (s *ContestCalendarService) lookupUserEmail(ctx context.Context, userID int64) (string, error) {
	if s.reg == nil {
		return "", nil
	}
	conn, err := grpc.DialInsecure(
		ctx,
		grpc.WithEndpoint("discovery:///user"),
		grpc.WithDiscovery(s.reg.Reg.(registry.Discovery)),
		grpc.WithTimeout(10*time.Second),
	)
	if err != nil {
		return "", err
	}
	defer conn.Close()
	cli := profile.NewProfileClient(conn)
	res, err := cli.GetById(ctx, &profile.GetByIdReq{UserId: userID})
	if err != nil {
		return "", err
	}
	if res == nil {
		return "", nil
	}
	return res.GetEmail(), nil
}
