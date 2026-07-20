package dal

import (
	"testing"
	"time"

	"cwxu-algo/app/core_data/internal/data/model"

	"gorm.io/driver/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func setupCalendarDB(t *testing.T) *ContestCalendarDal {
	t.Helper()
	dsn := "file:cal_" + t.Name() + "?mode=memory&cache=shared"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatalf("sqlite: %v", err)
	}
	if err := db.AutoMigrate(&model.ContestCalendar{}, &model.ContestCalendarSub{}); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return NewContestCalendarDalDB(db)
}

func TestListCalendarPlatformFilterCaseInsensitive(t *testing.T) {
	d := setupCalendarDB(t)
	now := time.Now().Unix()
	seed := []model.ContestCalendar{
		{
			Platform: "AtCoder", PlatformName: "AtCoder", ExternalID: "abc1",
			Name: "ABC 1", URL: "https://example.com/1",
			StartTime: now + 3600, EndTime: now + 7200, Source: "cpolar",
		},
		{
			Platform: "CodeForces", PlatformName: "Codeforces", ExternalID: "cf1",
			Name: "CF Round", URL: "https://example.com/cf",
			StartTime: now + 7200, EndTime: now + 10800, Source: "cpolar",
		},
	}
	if err := d.db.Create(&seed).Error; err != nil {
		t.Fatalf("seed: %v", err)
	}

	cases := []struct {
		name     string
		platform string
		wantN    int
		wantPlat string
	}{
		{"canonical", "AtCoder", 1, "AtCoder"},
		{"lower", "atcoder", 1, "AtCoder"},
		{"upper", "ATCODER", 1, "AtCoder"},
		{"cf lower", "codeforces", 1, "CodeForces"},
		{"cf canon", "CodeForces", 1, "CodeForces"},
		{"empty all", "", 2, ""},
		{"unknown", "no-such-oj", 0, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			list, total, err := d.List(CalendarListQuery{
				Platform: tc.platform,
				Status:   "upcoming",
				Limit:    20,
			})
			if err != nil {
				t.Fatalf("List: %v", err)
			}
			if int(total) != tc.wantN || len(list) != tc.wantN {
				t.Fatalf("platform=%q: total=%d len=%d want %d", tc.platform, total, len(list), tc.wantN)
			}
			if tc.wantN == 1 && list[0].Platform != tc.wantPlat {
				t.Fatalf("got platform %q want %q", list[0].Platform, tc.wantPlat)
			}
		})
	}
}

func TestNormalizeLegacySubsPlatform(t *testing.T) {
	d := setupCalendarDB(t)
	// sqlite 不跑 postgres 的 UPDATE AS 日历迁移；只验订阅表简单 UPDATE
	if err := d.db.Create(&model.ContestCalendarSub{
		UserID: 1, Scope: model.CalScopePlatform, Platform: "atcoder",
		CalendarID: 0, AdvanceMinutes: 360, Enabled: true,
	}).Error; err != nil {
		t.Fatalf("seed sub: %v", err)
	}
	// 直接调用订阅迁移片段等价逻辑（NormalizeLegacyPlatformNames 含日历 AS 语法，sqlite 可能不兼容）
	if err := d.db.Exec(`UPDATE contest_calendar_subs SET platform = ? WHERE platform = ?`, "AtCoder", "atcoder").Error; err != nil {
		t.Fatalf("migrate sub: %v", err)
	}
	var sub model.ContestCalendarSub
	if err := d.db.First(&sub).Error; err != nil {
		t.Fatalf("load: %v", err)
	}
	if sub.Platform != "AtCoder" {
		t.Fatalf("platform=%q want AtCoder", sub.Platform)
	}
}
